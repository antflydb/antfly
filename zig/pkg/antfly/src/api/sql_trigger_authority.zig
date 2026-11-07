// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Trigger preparation authority is delegated only from a completed trusted
//! statement, or a freshly verified user-authorized forwarding grant. The
//! callback borrows the process-long server, never a statement/request arena.
const std = @import("std");
const local = @import("antfly_local_sources");
const authority = local.common_trigger_preparation_authority;
const handoff = local.sql_catalog.trigger_handoff;
const A = std.mem.Allocator;

const keys = @import("sql_preparation_authority_keys.zig");
pub const Key = keys.Key;
pub const configured = keys.configured;
pub const requireConfiguration = keys.requireConfiguration;

pub fn minter(server: anytype, a: A, receipts: []const handoff.Receipt, command_digest: ?[32]u8) !authority.Minter {
    if (receipts.len == 0) return error.SqlTriggerContextRequired;
    try requireConfiguration(server.cfg);
    const table = receipts[0].program.table;
    for (receipts) |receipt| if (!receipt.program.table.eql(table)) return error.InvalidSqlTriggerManifest;
    const Server = @TypeOf(server);
    const Bridge = struct {
        fn sign(raw: *anyopaque, alloc: A, scope: authority.Scope) anyerror![]u8 {
            const api: Server = @ptrCast(@alignCast(raw));
            const key = try configured(api.cfg);
            return authority.sign(alloc, key.secret, key.issuer, scope, @intCast(@divFloor(api.internalAuthRealtimeNs(), std.time.ns_per_s)));
        }
    };
    return .{ .ptr = server, .sign_fn = Bridge.sign, .table_id = table.id, .table_fingerprint = try table.fingerprint(), .receipts_digest = try handoff.receiptsDigest(a, receipts), .command_fence_digest = command_digest };
}
/// Receipt data reaches this function only through the existing protected
/// statement/session ownership path. Public JSON cannot install it. Recreate
/// process-local authority here after savepoint/restart decoding rather than
/// storing a borrowed signer or refiring a trigger at COMMIT.
pub fn attachMinters(server: anytype, a: A, context: local.api_operation.RequestContext, tables: []const local.api_distributed_txn_contract.TableCommitRequest) ![]const local.api_distributed_txn_contract.TableCommitRequest {
    var needed = false;
    for (tables) |table| if (table.trigger_receipts.len != 0) {
        needed = true;
        break;
    };
    if (!needed) return tables;
    try context.ensureActive();
    const command_digest: ?[32]u8 = if (context.sql_command_fence) |fence| digest: {
        try fence.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        fence.hashInto(&hash);
        var result: [32]u8 = undefined;
        hash.final(&result);
        break :digest result;
    } else null;
    const captured = try a.dupe(local.api_distributed_txn_contract.TableCommitRequest, tables);
    for (captured) |*table| if (table.trigger_receipts.len != 0) {
        table.trigger_minter = try minter(server, a, table.trigger_receipts, command_digest);
    };
    return captured;
}

/// Call after ordinary node transport authentication and actual catalog route
/// resolution. The forwarded body and receiver route supply expected scope;
/// a self-reported checksum or a node credential alone grants no authority.
pub fn admitHttp(server: anytype, a: A, route: local.metadata_catalog_route_contract.CatalogRouteFence, req: local.storage_db_types.TransactionIntentRequest, body: []const u8, token: ?[]const u8, purpose: authority.Purpose) !local.metadata_catalog_route_contract.CatalogRouteFence {
    if (req.trigger_receipts.len == 0) {
        if (token != null) return error.Forbidden;
        return route;
    }
    var result = route;
    const table = req.trigger_receipts[0].program.table;
    if (table.id != route.table_id or req.trigger_preparation_group_id != route.route.group_id) return error.PreparedGenerationChanged;
    const expected: authority.Scope = .{ .purpose = purpose, .table_id = route.table_id, .group_id = route.route.group_id, .table_fingerprint = try table.fingerprint(), .receipts_digest = try handoff.receiptsDigest(a, req.trigger_receipts), .body_digest = authority.bodyDigest(body), .command_fence_digest = req.trigger_preparation_command_digest };
    const key = try configured(server.cfg);
    _ = try authority.verify(a, key.secret, key.issuer, expected, @intCast(@divFloor(server.internalAuthRealtimeNs(), std.time.ns_per_s)), token orelse return error.SqlTriggerContextRequired);
    result.admission_trigger_minter = try minter(server, a, req.trigger_receipts, req.trigger_preparation_command_digest);
    return result;
}

test "SQL trigger authority requires dedicated configured user credentials before effects" {
    const Config = struct { sql_preparation_secret: ?[]const u8 = null, sql_preparation_issuer: ?[]const u8 = null, trusted_principal_secret: ?[]const u8 = null, trusted_principal_issuer: ?[]const u8 = null, internal_service_secret: ?[]const u8 = null };
    var cfg: Config = .{ .internal_service_secret = "node-only" };
    try std.testing.expectError(error.SqlPreparationAuthorityNotConfigured, requireConfiguration(cfg));
    cfg.trusted_principal_secret = "dedicated-user-authority";
    try std.testing.expectError(error.SqlPreparationAuthorityNotConfigured, requireConfiguration(cfg));
    cfg.trusted_principal_issuer = "cluster";
    try requireConfiguration(cfg);
    cfg.trusted_principal_secret = "";
    try std.testing.expectError(error.SqlPreparationAuthorityNotConfigured, requireConfiguration(cfg));
    cfg.sql_preparation_secret = "separate-preparation-key";
    try std.testing.expectError(error.SqlPreparationAuthorityNotConfigured, requireConfiguration(cfg));
    cfg.sql_preparation_issuer = "local-catalog";
    try std.testing.expectEqualStrings("separate-preparation-key", (try configured(cfg)).secret);
    try std.testing.expectEqualStrings("local-catalog", (try configured(cfg)).issuer);
}
