// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const lifetimes = @import("sql_relation_lifetimes.zig");
const authority = local.common_relation_owner_authority;
const prepared = @import("sql_prepared.zig");
const owner: lifetimes.Owner = .{ .issuer_node_id = 7, .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = 5, .principal_digest = @splat(4) };
const descriptor: lifetimes.Descriptor = .{ .kind = .temporary, .owner_node_id = std.math.maxInt(u64), .owner_store_id = std.math.maxInt(u64), .generation_instance = @splat(8), .data_generation = std.math.maxInt(u64), .owner = owner };

fn cloneBinding(a: std.mem.Allocator) !void {
    const table: local.common_topology_records.TableRecord = .{ .table_id = 17, .name = "table:retained", .sql_persistence = descriptor, .schema_json = "{\"version\":3}", .full_text_read_index = "text:retained" };
    var binding = domain.ResolvedTable.fromTable(table);
    binding.query_definition = domain.QueryDefinition.fromTable(table);
    const copy = try binding.clone(a);
    defer copy.deinit(a);
    try copy.requireRequestAccess(null, owner);
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, copy.requireRequestAccess(null, null));
    const bytes = try std.json.Stringify.valueAlloc(a, copy, .{});
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(domain.ResolvedTable, a, bytes, .{});
    defer parsed.deinit();
    try std.testing.expect(std.meta.eql(descriptor, parsed.value.sql_persistence.?));
    try std.testing.expect(std.meta.eql(descriptor, parsed.value.query_definition.?.sql_persistence.?));
    try parsed.value.requireRequestAccess(null, owner);
    parsed.value.query_definition.?.sql_persistence.?.data_generation -= 1;
    try std.testing.expectError(error.CatalogGenerationChanged, parsed.value.requireRequestAccess(null, owner));
}

test "SQL relation lifetime captured bindings preserve full width owner identities across wire clone and allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, cloneBinding, .{});
    const binding: domain.ResolvedTable = .{ .table_id = 1, .name = "ordinary" };
    const bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, binding, .{});
    defer std.testing.allocator.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "sql_persistence") == null);
    try binding.requireRequestAccess(null, null);
}

test "SQL relation lifetime prepared manifests fence native generation host and recycled connection" {
    const a = std.testing.allocator;
    const table: local.sql_catalog.Table = .{ .id = 17, .physical_name = "table:retained", .schema_version = 3, .columns = &.{}, .scope = .{ .database = "default", .namespace = "pg_temp", .name = "items", .revision = 4 }, .sql_persistence = descriptor };
    const binding = try prepared.Binding.from(table);
    try prepared.Binding.verify(&.{binding}, table);
    inline for (.{ "data_generation", "owner_node_id", "owner_store_id" }) |field| {
        var changed = table;
        @field(changed.sql_persistence.?, field) -= 1;
        try std.testing.expectError(error.CatalogGenerationChanged, prepared.Binding.verify(&.{binding}, changed));
    }
    var changed = table;
    changed.sql_persistence.?.generation_instance[0] ^= 0xff;
    try std.testing.expectError(error.CatalogGenerationChanged, prepared.Binding.verify(&.{binding}, changed));
    changed = table;
    changed.sql_persistence.?.owner.?.connection_generation += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, prepared.Binding.verify(&.{binding}, changed));
    changed = table;
    changed.sql_persistence = null;
    try std.testing.expectError(error.CatalogGenerationChanged, prepared.Binding.verify(&.{binding}, changed));
    const ordinary = try prepared.Binding.from(changed);
    const bytes = try std.json.Stringify.valueAlloc(a, ordinary, .{});
    defer a.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "sql_persistence_fingerprint") == null);
    const captured = try std.json.Stringify.valueAlloc(a, binding, .{});
    defer a.free(captured);
    var decoded = try std.json.parseFromSlice(prepared.Binding, a, captured, .{});
    defer decoded.deinit();
    try prepared.Binding.verify(&.{decoded.value}, table);
}

test "SQL relation lifetime retained minter copies connection authority instead of borrowing request state" {
    const Server = struct {
        cfg: struct { trusted_principal_secret: ?[]const u8 = "configured authority", trusted_principal_issuer: ?[]const u8 = "issuer" } = .{},
        pub fn internalAuthRealtimeNs(_: *@This()) i128 {
            return 100 * std.time.ns_per_s;
        }
    };
    var server: Server = .{};
    var context: local.api_operation.RequestContext = .{ .sql_relation_owner = owner };
    const minter = (try lifetimes.ownerMinter(&server, context)).?;
    context.sql_relation_owner.?.connection_generation += 1;
    const scope: authority.Scope = .{ .purpose = .read, .table_id = 17, .group_id = 19, .descriptor_fingerprint = try descriptor.fingerprint(), .body_digest = authority.bodyDigest("exact request") };
    const bytes = try minter.mint(std.testing.allocator, scope);
    defer std.testing.allocator.free(bytes);
    const accepted = try authority.verify(std.testing.allocator, server.cfg.trusted_principal_secret.?, server.cfg.trusted_principal_issuer.?, scope, descriptor, 100, bytes);
    try std.testing.expect(owner.eql(accepted));
    try std.testing.expect(!context.sql_relation_owner.?.eql(accepted));
}

test "SQL relation lifetime final participant capture refuses foreign owners and keeps authority off the wire" {
    const Server = struct {
        cfg: struct { trusted_principal_secret: ?[]const u8 = "configured authority", trusted_principal_issuer: ?[]const u8 = "issuer" } = .{},
        pub fn internalAuthRealtimeNs(_: *@This()) i128 {
            return 100 * std.time.ns_per_s;
        }
    };
    var server: Server = .{};
    const Participant = local.api_distributed_txn_contract.TableCommitRequest;
    const ordinary = [_]Participant{.{ .table_name = "ordinary" }};
    // Ordinary execution must remain available without connection authority
    // and must not allocate an outer participant array.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const unchanged = try lifetimes.attachOwnerMinters(&server, failing.allocator(), .{}, &ordinary);
    try std.testing.expect(unchanged.ptr == ordinary[0..].ptr);
    const participants = [_]Participant{ ordinary[0], .{ .table_name = "table:retained", .sql_persistence = descriptor } };
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, lifetimes.attachOwnerMinters(&server, std.testing.allocator, .{}, &participants));
    var foreign = owner;
    foreign.connection_generation += 1;
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, lifetimes.attachOwnerMinters(&server, std.testing.allocator, .{ .sql_relation_owner = foreign }, &participants));
    const captured = try lifetimes.attachOwnerMinters(&server, std.testing.allocator, .{ .sql_relation_owner = owner }, &participants);
    defer std.testing.allocator.free(captured);
    try std.testing.expect(captured.ptr != participants[0..].ptr);
    try std.testing.expect(captured[0].relation_owner_minter == null);
    try std.testing.expect(owner.eql(captured[1].relation_owner_minter.?.owner));
    try std.testing.expect(participants[1].relation_owner_minter == null);
    const bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, captured, .{});
    defer std.testing.allocator.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "relation_owner_minter") == null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "configured authority") == null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "sql_persistence") != null);
}

test "SQL relation lifetime private namespaces retain exact owners and strict duplicate creation" {
    const a = std.testing.allocator;
    var buffer: [72]u8 = undefined;
    const name = try owner.namespace(&buffer);
    const request: domain.Mutation = .{ .kind = .namespace, .action = .create, .name = name, .sql_temporary_owner = owner };
    var initial = try domain.plan(a, .{ .next_id = 7 }, request, &.{});
    defer initial.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), initial.upserts.len);
    try std.testing.expect(initial.upserts[0].sql_temporary_owner.?.eql(owner));
    const retained: domain.State = .{ .next_id = initial.next_id, .resources = initial.upserts };
    try std.testing.expectError(error.CatalogAlreadyExists, domain.plan(a, retained, request, &.{}));
    var unowned = request;
    unowned.sql_temporary_owner = null;
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, domain.plan(a, retained, unowned, &.{}));
    unowned.action = .drop;
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, domain.plan(a, retained, unowned, &.{}));
    var collision = initial.upserts[0];
    collision.sql_temporary_owner = null;
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, domain.plan(a, .{ .next_id = 8, .resources = &.{collision} }, request, &.{}));
    var forged = request;
    forged.sql_temporary_owner.?.connection_generation += 1;
    try std.testing.expectError(error.InvalidCatalogMutation, domain.plan(a, retained, forged, &.{}));
    const bytes = try std.json.Stringify.valueAlloc(a, initial.upserts[0], .{});
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(domain.Resource, a, bytes, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.sql_temporary_owner.?.eql(owner));
    var index = try domain.StateIndex.init(a, .{ .next_id = 8, .resources = &.{parsed.value} });
    defer index.deinit(a);
}

test "SQL relation lifetime TEMP access is sticky across savepoint rollback and durable metadata recovery" {
    const a = std.testing.allocator;
    const transactions = @import("transactions.zig");
    var backend = local.storage_mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/sql-temp-access" });
    defer store.deinit();
    var durable = transactions.DurableSessionStore.initRuntime(a, &store);
    var registry = transactions.SessionRegistry.init(&durable);
    defer registry.deinit(a);
    const info = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice");
    const savepoint = (try registry.createSavepoint(a, info.txn_id)).?;
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, registry.markSqlTemporaryAccess(a, info.txn_id));
    durable.fail_writes_for_test = false;
    {
        var state = (try registry.getSqlState(a, info.txn_id)).?;
        defer state.deinit(a);
        try std.testing.expect(!state.metadata.temporary_relation_access);
        const bytes = try std.json.Stringify.valueAlloc(a, state.metadata, .{});
        defer a.free(bytes);
        try std.testing.expect(std.mem.indexOf(u8, bytes, "temporary_relation_access") == null);
    }
    try registry.markSqlTemporaryAccess(a, info.txn_id);
    // The allocation-free repeat cannot introduce a new durability failure.
    durable.fail_writes_for_test = true;
    try registry.markSqlTemporaryAccess(a, info.txn_id);
    durable.fail_writes_for_test = false;
    _ = try registry.rollbackToSavepoint(a, info.txn_id, savepoint.savepoint_id);
    var recovered = (try durable.load(info.txn_id)).?;
    defer recovered.deinit(a);
    try std.testing.expect(recovered.sql.?.temporary_relation_access);
    const other = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice");
    var unrelated = (try registry.getSqlState(a, other.txn_id)).?;
    defer unrelated.deinit(a);
    try std.testing.expect(!unrelated.metadata.temporary_relation_access);
}

test "SQL relation lifetime namespace shadowing preserves explicit order alias and terminal authorization" {
    const Catalog = local.sql_catalog;
    const Ast = local.sql_ast;
    const path = @import("../pgwire/search_path.zig");
    const lookup = @import("sql_lookup_path.zig");
    const Fixture = struct {
        calls: usize = 0,
        temporary_missing: bool = false,
        temporary_error: ?anyerror = null,
        fn resolve(raw: *anyopaque, _: std.mem.Allocator, name: Ast.Name, _: Catalog.Action) !Catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const namespace = name.namespace orelse "public";
            if (std.mem.startsWith(u8, namespace, "pg_temp_")) {
                if (self.temporary_error) |err| return err;
                if (self.temporary_missing) return error.TableNotFound;
                return .{ .id = 42, .physical_name = "temporary", .schema_version = 1, .columns = &.{} };
            }
            if (!std.mem.eql(u8, namespace, "public")) return error.TableNotFound;
            return .{ .id = 11, .physical_name = "ordinary", .schema_version = 1, .columns = &.{} };
        }
        fn read(raw: *anyopaque, a: std.mem.Allocator, name: Ast.Name) !Catalog.ReadRelation {
            return .{ .table = try resolve(raw, a, name, .read) };
        }
        fn backend(self: *@This()) Catalog.Backend {
            return .{ .temporary_relation_owner = owner, .ptr = self, .vtable = &.{ .resolve = resolve, .resolve_read = read, .scan = undefined, .mutate = undefined, .checkpoint = undefined } };
        }
    };
    var ordinary_path: path.Path = .{};
    try ordinary_path.append(try path.Namespace.init("public"));
    var explicit_path = ordinary_path;
    try explicit_path.append(try path.Namespace.init("pg_temp"));
    var fixture: Fixture = .{};
    try std.testing.expectEqual(@as(u64, 42), (try lookup.resolveOnPath(fixture.backend(), std.testing.allocator, .{ .table = "rows" }, .read, &ordinary_path)).id);
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    fixture = .{};
    const relation = try lookup.resolveReadOnPath(fixture.backend(), std.testing.allocator, .{ .table = "rows" }, &ordinary_path);
    try std.testing.expectEqual(@as(u64, 42), relation.table.id);
    fixture = .{};
    try std.testing.expectEqual(@as(u64, 11), (try lookup.resolveOnPath(fixture.backend(), std.testing.allocator, .{ .table = "rows" }, .read, &explicit_path)).id);
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    fixture = .{};
    try std.testing.expectEqual(@as(u64, 42), (try lookup.resolveOnPath(fixture.backend(), std.testing.allocator, .{ .namespace = "pg_temp", .table = "rows" }, .read, null)).id);
    fixture = .{};
    try std.testing.expectEqual(@as(u64, 11), (try lookup.resolveOnPath(fixture.backend(), std.testing.allocator, .{ .namespace = "public", .table = "rows" }, .read, null)).id);
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    fixture = .{ .temporary_missing = true };
    try std.testing.expectEqual(@as(u64, 11), (try lookup.resolveOnPath(fixture.backend(), std.testing.allocator, .{ .table = "rows" }, .read, &ordinary_path)).id);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    for ([_]anyerror{ error.Forbidden, error.SqlTemporaryRelationNotOwned, error.CatalogGenerationChanged, error.QueryCanceled }) |failure| {
        fixture = .{ .temporary_error = failure };
        try std.testing.expectError(failure, lookup.resolveReadOnPath(fixture.backend(), std.testing.allocator, .{ .table = "rows" }, &ordinary_path));
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    }
}

test "SQL relation lifetime DDL preserves native intent and refuses backends without lifecycle authority" {
    const a = std.testing.allocator;
    const Catalog = local.sql_catalog;
    const Fixture = struct {
        calls: usize = 0,
        fn checkpoint(_: *anyopaque) !void {}
        fn ddl(raw: *anyopaque, _: Catalog.DdlAllocators, input: Catalog.Ddl) !Catalog.DdlOutcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expect(input == .create_table);
            try std.testing.expectEqual(local.sql_ast.RelationPopulation.Persistence.temporary, input.create_table.persistence);
            try std.testing.expectEqual(local.sql_ast.RelationPopulation.OnCommit.delete_rows, input.create_table.on_commit.?);
            return .{ .mutation_outcome = .committed };
        }
    };
    var compiled = try local.sql_compiler.compile(a, "CREATE TEMPORARY TABLE local_rows(id BIGINT PRIMARY KEY) ON COMMIT DELETE ROWS", .{});
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var backend: Catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = Fixture.checkpoint, .ddl = Fixture.ddl } };
    try std.testing.expectError(error.UnsupportedSqlExecution, local.sql_ddl_runtime.execute(.{ .result = a, .scratch = a }, backend, compiled.statement));
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    backend.supports_native_relation_lifetimes = true;
    _ = try local.sql_ddl_runtime.execute(.{ .result = a, .scratch = a }, backend, compiled.statement);
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    const native_schema = try local.sql_ddl_runtime.createSchemaAlloc(a, compiled.statement.create_table);
    defer a.free(native_schema);
    var ordinary = compiled.statement.create_table;
    ordinary.persistence = .permanent;
    ordinary.on_commit = null;
    const ordinary_schema = try local.sql_ddl_runtime.createSchemaAlloc(a, ordinary);
    defer a.free(ordinary_schema);
    try std.testing.expectEqualStrings(ordinary_schema, native_schema);
}

const CreationFixture = struct {
    cfg: struct { trusted_principal_secret: ?[]const u8 = "configured-user-key", trusted_principal_issuer: ?[]const u8 = "issuer" } = .{},
    source: Source,
    const boots = @import("../metadata/sql_relation_boots.zig");
    const directory = @import("../metadata/sql_relation_lifetimes.zig");
    const Source = struct {
        host: boots.Record,
        prior: ?lifetimes.Descriptor = null,
        calls: usize = 0,
        registration_current: bool = true,
        pub fn systemCatalog(self: *@This(), a: std.mem.Allocator, context: local.api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try std.testing.expect(context.setting_admin);
            self.calls += 1;
            switch (call) {
                .sql_relation_lifetime_page => |query| {
                    try std.testing.expect(query.selection == .connection);
                    try std.testing.expect(query.selection.connection.eql(owner));
                    try std.testing.expectEqual(@as(u16, 1), query.limit);
                    const entries = [_]directory.Entry{.{ .table_id = 17, .descriptor = self.prior orelse descriptor }};
                    return std.json.Stringify.valueAlloc(a, directory.Page{ .entries = if (self.prior != null) &entries else &.{}, .next = null }, .{});
                },
                .sql_relation_boot_read => |query| {
                    if (self.prior) |existing| {
                        try std.testing.expectEqual(existing.owner_node_id, query.node.?);
                        try std.testing.expect(query.preferred_node == null);
                    } else if (context.sql_relation_owner != null) {
                        try std.testing.expect(query.node == null);
                        try std.testing.expectEqual(owner.issuer_node_id, query.preferred_node.?);
                    }
                    return std.json.Stringify.valueAlloc(a, boots.Observation{ .record = self.host, .registration_current = self.registration_current }, .{});
                },
                else => return error.TestUnexpectedResult,
            }
        }
    };
    fn init() !@This() {
        const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(5));
        return .{ .source = .{ .host = .{ .node_id = 23, .identity = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 23, .store_id = 31, .root_incarnation = 41, .public_key = pair.public_key.toBytes() }, .reporter_incarnation = 1, .generation = 2, .instance = @splat(8), .claim_digest = @splat(9), .unclean_restart = false, .phase = .ready } } };
    }
};

fn captureCreation(a: std.mem.Allocator) !void {
    var fixture = try CreationFixture.init();
    const capture = try @import("sql_relation_creation.zig").prepare(&fixture, a, .{ .sql_relation_owner = owner }, .{ .table = "rows" }, .temporary, .preserve_rows, false);
    defer a.free(capture.target.namespace);
    try std.testing.expectEqual(@as(usize, 2), fixture.source.calls);
    try std.testing.expectEqual(@as(u64, 23), capture.descriptor.?.owner_node_id);
    try std.testing.expectEqual(@as(u64, 31), capture.descriptor.?.owner_store_id);
    try std.testing.expectEqual(@as(u64, 1), capture.descriptor.?.data_generation);
    try std.testing.expect(owner.eql(capture.descriptor.?.owner.?));
    fixture.source.prior = capture.descriptor;
    fixture.source.host.instance[0] ^= 0xff;
    const changed = @import("sql_relation_creation.zig").prepare(&fixture, a, .{ .sql_relation_owner = owner }, .{ .table = "next" }, .temporary, .preserve_rows, false) catch |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.SqlRelationOwnerChanged, err);
        return;
    };
    defer a.free(changed.target.namespace);
    return error.TestUnexpectedResult;
}

test "SQL relation lifetime creation pins the native host independently of its API issuer and cleans allocation faults" {
    try captureCreation(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, captureCreation, .{});
    var fixture = try CreationFixture.init();
    _ = try @import("sql_relation_creation.zig").prepare(&fixture, std.testing.allocator, .{}, .{ .table = "ordinary" }, .permanent, .preserve_rows, false);
    try std.testing.expectEqual(@as(usize, 0), fixture.source.calls);
    const unlogged = try @import("sql_relation_creation.zig").prepare(&fixture, std.testing.allocator, .{}, .{ .table = "unlogged" }, .unlogged, .preserve_rows, false);
    try std.testing.expect(unlogged.descriptor.?.owner == null);
    try std.testing.expectEqual(@as(u64, 23), unlogged.descriptor.?.owner_node_id);
    fixture.source.registration_current = false;
    try std.testing.expectError(error.SqlRelationOwnerChanged, @import("sql_relation_creation.zig").prepare(&fixture, std.testing.allocator, .{}, .{ .table = "unlogged" }, .unlogged, .preserve_rows, false));
}

test "SQL relation lifetime phase two activation cannot inherit authority for new effects" {
    const types = local.storage_db_mod.types;
    const external: local.storage_transactions.ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const activation: types.BatchRequest = .{ .transaction = .{ .prepare = .{ .txn_id = @splat(4), .topology_epoch = 5, .external_decision_owner = external, .external_activation = true } } };
    try std.testing.expect(activation.externalActivationOwner().?.eql(external));
    var changed = activation;
    changed.writes = &.{.{ .key = "private", .value = "new user data" }};
    try std.testing.expectEqual(null, changed.externalActivationOwner());
    changed = activation;
    changed.activate_range_tracking = true;
    try std.testing.expectEqual(null, changed.externalActivationOwner());
    changed = activation;
    changed.trigger_preparation_group_id = 3;
    try std.testing.expectEqual(null, changed.externalActivationOwner());
    changed = activation;
    changed.row_policy_install_bundle = "unauthorized control";
    try std.testing.expectEqual(null, changed.externalActivationOwner());
    changed = activation;
    changed.transaction.?.prepare.external_activation = false;
    try std.testing.expectEqual(null, changed.externalActivationOwner());
    changed = activation;
    changed.transaction.?.prepare.external_decision_owner = null;
    try std.testing.expectEqual(null, changed.externalActivationOwner());
}

test "SQL relation lifetime external ACK retains native authority and never falls back to raft" {
    const distributed = @import("distributed_txn.zig");
    const a = std.testing.allocator;
    const external: local.storage_transactions.ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const route: local.metadata_catalog_route_contract.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 1, .table_id = 2, .topology_epoch = 1, .route = .{ .group_id = 9, .range_id = 9, .identity_namespace = .{ .table_id = 2, .shard_id = 9, .range_id = 9 }, .execution = .{ .native_local = descriptor } } };
    const participant = try distributed.participantIdForGroupScoped(a, "native", 9, null, null);
    defer a.free(participant);
    const request: distributed.TxnAcknowledgeManyRequest = .{ .txn_id = @splat(4), .participants = &.{participant}, .route_fence = route, .external_decision_owner = external };
    const wire = try distributed.encodeTxnAcknowledgeManyRequest(a, request);
    defer a.free(wire);
    var parsed = try distributed.parseTxnAcknowledgeManyRequest(a, wire);
    defer distributed.freeTxnAcknowledgeManyRequest(a, &parsed);
    try std.testing.expect(parsed.route_fence.?.route.execution.native_local.eql(descriptor));
    try std.testing.expect(parsed.external_decision_owner.?.eql(external));
    const status_wire = try distributed.encodeTxnStatusRequestWithScope(a, .{ .txn_id = request.txn_id, .route_fence = route, .external_decision_owner = external });
    defer a.free(status_wire);
    const status_request = try distributed.parseTxnStatusRequestWithScope(a, status_wire);
    try std.testing.expect(status_request.route_fence.?.route.execution.native_local.eql(descriptor));
    try std.testing.expect(status_request.external_decision_owner.?.eql(external));
    try std.testing.expectError(error.InvalidTxnRequest, distributed.parseTxnStatusRequest(a, status_wire));
    const Capture = struct {
        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: local.storage_db_mod.types.BatchRequest) !?void {
            return error.UnexpectedOrdinaryWrite;
        }
        fn ordinary(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: local.storage_db_mod.types.BatchRequest) !?void {
            return error.UnexpectedRaftWrite;
        }
        fn native(raw: *anyopaque, _: std.mem.Allocator, group: u64, _: []const u8, req: local.storage_db_mod.types.BatchRequest, fence: local.metadata_catalog_route_contract.CatalogRouteFence) !?void {
            const calls: *usize = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u64, 9), group);
            try std.testing.expect(fence.route.execution.native_local.eql(descriptor));
            try std.testing.expect(req.externalDecisionControlOwner() != null);
            calls.* += 1;
            return {};
        }
    };
    var calls: usize = 0;
    const writes: @import("table_writes.zig").TableWriteSource = .{ .ptr = &calls, .vtable = &.{ .batch = Capture.batch, .batch_group_local = Capture.ordinary, .batch_group_local_routed = Capture.native } };
    _ = try distributed.acknowledgeManyGroupLocalWithRequest(writes, a, 9, "native", parsed, .none);
    try std.testing.expectEqual(@as(usize, 1), calls);
    parsed.external_decision_owner = null;
    try std.testing.expectError(error.InvalidExternalDecisionOwner, distributed.acknowledgeManyGroupLocalWithRequest(writes, a, 9, "native", parsed, .none));
}

test "SQL temporary creation requires preparation authority before namespace metadata or native lookup" {
    var fixture = try CreationFixture.init();
    fixture.cfg.trusted_principal_secret = null;
    fixture.cfg.trusted_principal_issuer = null;
    try std.testing.expectError(error.SqlPreparationAuthorityNotConfigured, @import("sql_relation_creation.zig").prepare(&fixture, std.testing.allocator, .{ .sql_relation_owner = owner }, .{ .table = "must-not-exist" }, .temporary, .preserve_rows, false));
    try std.testing.expectEqual(@as(usize, 0), fixture.source.calls);
    // Permanent/unlogged relations require no connection grant and do not
    // acquire a fake role or authority just to preserve ordinary DDL.
    _ = try @import("sql_relation_creation.zig").prepare(&fixture, std.testing.allocator, .{}, .{ .table = "ordinary" }, .permanent, .preserve_rows, false);
    try std.testing.expectEqual(@as(usize, 0), fixture.source.calls);
}

test "SQL owner minter selection leaves ordinary sessions unconfigured and fences captured TEMP only" {
    const Server = struct {
        cfg: struct { trusted_principal_secret: ?[]const u8 = null, trusted_principal_issuer: ?[]const u8 = null } = .{},
        pub fn internalAuthRealtimeNs(_: *@This()) i128 {
            return 100 * std.time.ns_per_s;
        }
    };
    var server: Server = .{};
    const context: local.api_operation.RequestContext = .{ .sql_relation_owner = owner };
    try std.testing.expect((try lifetimes.ownerMinterForRelation(&server, context, null)) == null);
    var ordinary = descriptor;
    ordinary.kind = .unlogged;
    ordinary.owner = null;
    try std.testing.expect((try lifetimes.ownerMinterForRelation(&server, context, ordinary)) == null);
    try std.testing.expectError(error.SqlPreparationAuthorityNotConfigured, lifetimes.ownerMinterForRelation(&server, context, descriptor));
    server.cfg.trusted_principal_secret = "configured-user-key";
    server.cfg.trusted_principal_issuer = "issuer";
    try std.testing.expect((try lifetimes.ownerMinterForRelation(&server, context, descriptor)) != null);
    var foreign = context;
    foreign.sql_relation_owner.?.connection_generation += 1;
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, lifetimes.ownerMinterForRelation(&server, foreign, descriptor));
    const cohort = @import("../system_catalog/server_call.zig").SqlSnapshotCohort{ .revision = 1, .tables = &.{.{ .id = 17, .table = "ordinary", .schema_version = 1, .external = false }} };
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, cohort, .{});
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "sql_persistence") == null);
    const temporary_cohort = @import("../system_catalog/server_call.zig").SqlSnapshotCohort{ .revision = 2, .tables = &.{.{ .id = 17, .table = "owned", .schema_version = 1, .external = false, .sql_persistence = descriptor }} };
    const owned_wire = try std.json.Stringify.valueAlloc(std.testing.allocator, temporary_cohort, .{});
    defer std.testing.allocator.free(owned_wire);
    var decoded = try std.json.parseFromSlice(@import("../system_catalog/server_call.zig").SqlSnapshotCohort, std.testing.allocator, owned_wire, .{ .allocate = .alloc_always });
    defer decoded.deinit();
    try std.testing.expect(std.meta.eql(descriptor, decoded.value.tables[0].sql_persistence.?));
    try std.testing.expect((try lifetimes.ownerMinterForRelation(&server, context, decoded.value.tables[0].sql_persistence)) != null);
}
