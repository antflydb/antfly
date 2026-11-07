// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const signing = @import("sql_trigger_authority.zig");

test "dedicated SQL preparation keystore restart readonly partial pair and transport isolation" {
    try local.common_sql_preparation_authority_config.checkStandaloneLifecycle(std.testing.allocator, std.testing.io);
}

test "dedicated SQL preparation configured API minter and native receiver share only the selected authority" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const Server = struct {
        cfg: http.ApiHttpServerConfig,
        pub fn internalAuthRealtimeNs(_: *@This()) i128 {
            return 100 * std.time.ns_per_s;
        }
    };
    var server: Server = .{ .cfg = .{
        .auth_enabled = false,
        .sql_preparation_secret = "dedicated-SQL-preparation-key",
        .sql_preparation_issuer = "private-SQL-issuer",
        .trusted_principal_secret = "different-public-principal-key",
        .trusted_principal_issuer = "public-principal-issuer",
        .internal_service_secret = "different-node-transport-key",
        .internal_service_issuer = "node-issuer",
    } };
    const fixture = try local.sql_catalog.trigger_native_admission_tests.configuredAuthorityFixture(a);
    const minter = try signing.minter(&server, a, &.{fixture.captured}, null);
    const native = local.sql_catalog.trigger_native_admission;
    const request = try native.seal(a, minter, 11, 100, .{
        .trigger_receipts = &.{fixture.captured},
        .schema_version = 3,
        .writes = &.{.{ .key = "row", .value = "{\"n\":42}" }},
        .predicates = &.{.{ .key = "row", .expected_version = 7, .expected_content_digest = @splat(3) }},
    });
    const selected = try signing.configured(server.cfg);
    try native.verify(a, &fixture.view, request, selected.secret, selected.issuer, 100, false);
    try std.testing.expectError(error.Forbidden, native.verify(a, &fixture.view, request, server.cfg.trusted_principal_secret, server.cfg.trusted_principal_issuer, 100, false));
    try std.testing.expectError(error.Forbidden, native.verify(a, &fixture.view, request, server.cfg.internal_service_secret, server.cfg.internal_service_issuer, 100, false));
    try std.testing.expect(!server.cfg.auth_enabled);
}

test "default auth disabled SQL owner grants use the persisted private key and reject transport substitutes before admission" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(dir);
    const path = try std.fs.path.join(a, &.{ dir, "sql-user-authority.json" });
    defer a.free(path);
    var store = try local.common_secrets.FileStore.initWithIo(a, std.testing.io, path);
    defer store.deinit();
    store.environment_enabled = false;
    var key = (try local.common_sql_preparation_authority_config.load(a, &store, true)).?;
    defer key.deinit(a);
    const Server = struct {
        cfg: http.ApiHttpServerConfig,
        pub fn internalAuthRealtimeNs(_: *@This()) i128 {
            return 100 * std.time.ns_per_s;
        }
    };
    var server: Server = .{ .cfg = .{ .auth_enabled = false, .sql_preparation_secret = key.secret, .sql_preparation_issuer = key.issuer, .internal_service_secret = "node-only", .internal_service_issuer = "node" } };
    const lifetimes = @import("sql_relation_lifetimes.zig");
    const authority = local.common_relation_owner_authority;
    const owner: lifetimes.Owner = .{ .issuer_node_id = 7, .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = 5, .principal_digest = @splat(4) };
    const descriptor: lifetimes.Descriptor = .{ .kind = .temporary, .owner_node_id = 7, .owner_store_id = 9, .generation_instance = @splat(8), .data_generation = 1, .owner = owner };
    const minter = (try lifetimes.ownerMinter(&server, .{ .sql_relation_owner = owner })).?;
    const scope: authority.Scope = .{ .purpose = .read, .table_id = 17, .group_id = 19, .descriptor_fingerprint = try descriptor.fingerprint(), .body_digest = authority.bodyDigest("exact read body") };
    const token = try minter.mint(a, scope);
    defer a.free(token);
    try std.testing.expect(owner.eql(try authority.verify(a, key.secret, key.issuer, scope, descriptor, 100, token)));
    try std.testing.expectError(error.Forbidden, authority.verify(a, server.cfg.internal_service_secret.?, server.cfg.internal_service_issuer.?, scope, descriptor, 100, token));
    var changed = scope;
    changed.purpose = .write;
    try std.testing.expectError(error.Forbidden, authority.verify(a, key.secret, key.issuer, changed, descriptor, 100, token));
    changed = scope;
    changed.body_digest[0] ^= 1;
    try std.testing.expectError(error.Forbidden, authority.verify(a, key.secret, key.issuer, changed, descriptor, 100, token));
    try std.testing.expect(server.cfg.trusted_principal_secret == null);
    try std.testing.expect(!server.cfg.auth_enabled);
}

test "restored and created trigger functions share authenticated destination ABI and namespace admission" {
    const a = std.testing.allocator;
    const guard = @import("sql_trigger_function_admission.zig");
    const users = @import("../usermgr/mod.zig");
    var record: local.sql_catalog.routines.records.Record = .{ .id = 9, .generation = 1, .definition = .{ .arguments = &.{}, .returns_trigger = true, .language = .plpgsql, .body = "BEGIN RETURN NEW; END", .owner = .auth_disabled } };
    const target: local.system_catalog_domain.Target = .{ .namespace = "private", .table = "touch" };
    try guard.authorize(a, false, null, record, target);
    try std.testing.expectError(error.Forbidden, guard.authorize(a, true, null, record, target));
    var actor: http.AuthenticatedIdentity = .{ .username = @constCast("visitor") };
    try std.testing.expectError(error.Forbidden, guard.authorize(a, true, actor, record, target));
    record.definition.owner = .{ .user = .{ .name = "function-owner", .instance_id = @splat(7) } };
    try std.testing.expectError(error.Forbidden, guard.authorize(a, true, actor, record, target));
    var permission = try users.Permission.initOwned(a, .namespace, "*", .read);
    defer permission.deinit(a);
    var permissions = [_]users.Permission{permission};
    actor.permissions = &permissions;
    try guard.authorize(a, true, actor, record, target);
    record.definition.returns_trigger = false;
    try std.testing.expectError(error.InvalidSqlTriggerDefinition, guard.authorize(a, true, actor, record, target));
}
