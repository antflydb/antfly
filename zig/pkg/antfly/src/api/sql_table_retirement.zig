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

//! Public whole-table CASCADE uses the existing catalog-operation receipt and
//! read-only recovery API. Tokens bind immutable effects, never mutable names.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const domain = local.system_catalog_domain;
const http = @import("http_server.zig");
const root = @import("../metadata/table_retirement_ledger.zig");
const receipts = @import("../metadata/table_retirement_receipt.zig");
const operation = local.api_operation;
const Token = struct { version: u8 = 6, kind: catalog.CatalogOperationReceipt.Kind = .table, target: domain.Target, request: receipts.Request };
const base64 = std.base64.url_safe_no_pad;

fn encode(a: std.mem.Allocator, scratch: std.mem.Allocator, token: Token) ![]u8 {
    const bytes = try std.json.Stringify.valueAlloc(scratch, token, .{});
    const size = base64.Encoder.calcSize(bytes.len);
    if (size > @import("sql_catalog_recovery.zig").max_token_bytes) return error.InvalidSqlRecoveryToken;
    const result = try a.alloc(u8, size);
    _ = base64.Encoder.encode(result, bytes);
    return result;
}
fn requireToken(token: Token) !void {
    if (token.version != 6 or token.kind != .table or token.request.effect != .admission) return error.InvalidSqlRecoveryToken;
    token.target.validate() catch return error.InvalidSqlRecoveryToken;
    token.request.validate() catch return error.InvalidSqlRecoveryToken;
    const expected = root.Scope.targetDigest(token.target.database, token.target.namespace, token.target.table);
    if (!std.mem.eql(u8, &expected, &token.request.effect.admission.scope.target_digest)) return error.InvalidSqlRecoveryToken;
}
fn newReceipt(a: std.mem.Allocator, token: Token, encoded: []const u8) !catalog.CatalogOperationReceipt {
    const database = try a.dupe(u8, token.target.database);
    errdefer a.free(database);
    const namespace = try a.dupe(u8, token.target.namespace);
    errdefer a.free(namespace);
    const name = try a.dupe(u8, token.target.table);
    return .{ .kind = .table, .operation = .drop, .database = database, .namespace = namespace, .name = name, .recovery_token = encoded, .object_id = token.request.lookup.table };
}
fn observe(server: *http.ApiHttpServer, context: operation.RequestContext, a: std.mem.Allocator, expected: receipts.Request, result: *catalog.CatalogOperationReceipt) !void {
    const bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_receipt = expected });
    const observed = try std.json.parseFromSliceLeaky(receipts.Observation, a, bytes, .{});
    try observed.requireFor(expected);
    if (!observed.accepted) return;
    const head = observed.head.?;
    result.state = .pending;
    if (head.phase == .canceled) {
        result.state = .rejected;
        result.diagnostic = .{ .code = "57014", .message = "DROP was canceled before physical retirement; the table remains present", .retryable = false };
        return;
    }
    if (head.phase != .published) return;
    var revision = head.direct_catalog_revision;
    if (head.survivor) |survivor| {
        const lookup: @import("../metadata/schema_validation_read.zig").Lookup = .{ .group = head.lookup.group, .metadata_incarnation = head.incarnation, .table = survivor.table, .id = survivor.id, .plan_digest = survivor.digest };
        const request: @import("../metadata/schema_validation_receipt.zig").Request = .{ .lookup = lookup, .expected = .admission };
        const state = try server.source.systemCatalog(a, context, .{ .schema_validation_receipt_read = request });
        const cohort = try std.json.parseFromSliceLeaky(@import("../metadata/schema_validation_receipt.zig").Observation, a, state, .{});
        if (!try cohort.isAcknowledged(request) or cohort.head.?.phase != .succeeded) return;
        revision = cohort.head.?.catalog_revision;
    }
    if (revision == 0) return error.InvalidCatalogRecord;
    result.state = .committed;
    result.object_id = null;
    result.catalog_revision = revision;
    result.diagnostic = null;
}

pub fn recoverIfTable(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: operation.RequestContext, a: std.mem.Allocator, encoded: []const u8) !?catalog.CatalogOperationReceipt {
    if (encoded.len == 0 or encoded.len > @import("sql_catalog_recovery.zig").max_token_bytes) return error.InvalidSqlRecoveryToken;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const size = base64.Decoder.calcSizeForSlice(encoded) catch return error.InvalidSqlRecoveryToken;
    const bytes = try scratch.alloc(u8, size);
    base64.Decoder.decode(bytes, encoded) catch return error.InvalidSqlRecoveryToken;
    const header = std.json.parseFromSliceLeaky(struct { version: u8, kind: catalog.CatalogOperationReceipt.Kind }, scratch, bytes, .{ .ignore_unknown_fields = true }) catch return error.InvalidSqlRecoveryToken;
    if (header.version != 6 and header.kind != .table) return null;
    const token = std.json.parseFromSliceLeaky(Token, scratch, bytes, .{}) catch return error.InvalidSqlRecoveryToken;
    try requireToken(token);
    try @import("sql_catalog_auth.zig").authorizeTarget(server, identity, scratch, token.target);
    const owned = try a.dupe(u8, encoded);
    errdefer a.free(owned);
    var result = try newReceipt(a, token, owned);
    errdefer result.deinit(a);
    var private = try context.platformDeadline();
    private.setting_admin = true;
    private.schema_validation_authority = true;
    observe(server, private, scratch, token.request, &result) catch |err| {
        if (!@import("../metadata/schema_validation_coordinator.zig").isDeferred(err)) return err;
    };
    return result;
}

/// A null result keeps dependency-free document DROP on its ordinary native
/// metadata path. Every coordinated path pins the captured physical owner.
pub fn execute(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, input: operation.RequestContext, allocators: catalog.DdlAllocators, guard: domain.RelationMutationGuard) !?catalog.DdlOutcome {
    try input.ensureActive();
    var region = std.heap.ArenaAllocator.init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const target: domain.Target = .{ .database = guard.target.database, .namespace = guard.target.namespace, .table = guard.logical_table };
    try @import("sql_catalog_auth.zig").authorizeTarget(server, identity, a, target);
    var context = try input.platformDeadline();
    context.setting_admin = true;
    context.schema_validation_authority = true;
    context.fk_generation_publication_authority = true;
    const bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_prepare = guard });
    var manifest = try @import("../metadata/table_retirement_manifest.zig").Owned.parse(a, bytes);
    defer manifest.deinit();
    const Source = struct { storage_mode: enum { document, relational } = .document };
    const source: Source = if (manifest.value.root.schema_json.len != 0) try std.json.parseFromSliceLeaky(Source, a, manifest.value.root.schema_json, .{ .ignore_unknown_fields = true }) else .{};
    if (source.storage_mode == .document) {
        if (manifest.value.survivors.len == 0 and manifest.value.dependent_views.len == 0 and manifest.value.counters.len == 0) return null;
        return error.UnsupportedSqlShape;
    }
    if (manifest.value.view_backings.len != 0 or manifest.value.root_ranges.len == 0) return error.UnsupportedSqlShape;
    if (server.cfg.deployment_mode == .standalone and (server.hotStandbyMutationPolicy().failover_safe_mutations_only or server.cfg.fk_generation_source == null or server.cfg.fk_generation_parent == null)) return error.UnsupportedSqlExecution;
    const io = server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
    const labels = try server.source.systemCatalog(a, context, .{ .read = .{ .kind = .namespace, .database = target.database, .name = target.namespace } });
    const scope_rows = try std.json.parseFromSliceLeaky(domain.State, a, labels, .{});
    var namespace: ?domain.Resource = null;
    for (scope_rows.resources) |resource| if (resource.kind == .namespace and std.mem.eql(u8, resource.name, target.namespace)) {
        namespace = resource;
        break;
    };
    const ns = namespace orelse return error.NamespaceNotFound;
    var id: [16]u8 = undefined;
    while (true) {
        try io.randomSecure(&id);
        if (!std.mem.allEqual(u8, id[0..8], 0) and !std.mem.allEqual(u8, id[8..16], 0)) break;
    }
    const scope: root.Scope = .{ .database = ns.parent_id, .namespace = ns.id, .creator = (try @import("sql_authorization_recovery.zig").creator(server, identity)) orelse .bootstrap, .target_digest = root.Scope.targetDigest(target.database, target.namespace, target.table) };
    const lookup: root.Lookup = .{ .group = manifest.value.metadata_group_id, .table = manifest.value.root.table_id, .id = id };
    const token: Token = .{ .target = target, .request = .{ .lookup = lookup, .effect = .{ .admission = .{ .incarnation = manifest.value.metadata_incarnation, .effects = try manifest.value.reference(), .scope = scope } } } };
    try requireToken(token);
    // Reject unsupported owner/artifact shapes before durable reservation.
    _ = try @import("fk_generation_plan_builder.zig").buildTableRetirement(server, a, context, id, manifest.value);
    const encoded = try encode(allocators.result, a, token);
    var result = newReceipt(allocators.result, token, encoded) catch |err| {
        allocators.result.free(encoded);
        return err;
    };
    errdefer result.deinit(allocators.result);
    const command = try (root.Command{ .begin = .{ .id = id, .scope = scope, .manifest = manifest.value } }).encodeAlloc(a);
    try @import("sql_catalog_auth.zig").authorizeTarget(server, identity, a, target);
    try context.ensureActive();
    const reply = server.source.systemCatalog(a, context, .{ .table_retirement_submit = command }) catch |err| switch (err) {
        error.MetadataMutationOutcomeUnknown, error.OutOfMemory => return .{ .mutation_outcome = null, .catalog_receipt = result },
        else => return err,
    };
    const accepted = std.json.parseFromSliceLeaky(receipts.Observation, a, reply, .{}) catch return .{ .mutation_outcome = null, .catalog_receipt = result };
    accepted.requireFor(token.request) catch return .{ .mutation_outcome = null, .catalog_receipt = result };
    if (!accepted.accepted) return .{ .mutation_outcome = null, .catalog_receipt = result };
    result.state = .pending;
    const now = @import("antfly_platform").time;
    const deadline = if (allocators.completion == .bounded) now.monotonicNs() +| 2 * std.time.ns_per_s else context.deadline_ns orelse std.math.maxInt(u64);
    while (now.monotonicNs() < deadline) {
        input.ensureActive() catch break;
        var poll = std.heap.ArenaAllocator.init(allocators.scratch);
        defer poll.deinit();
        observe(server, context, poll.allocator(), token.request, &result) catch break;
        if (result.state == .committed) return .{ .mutation_outcome = .committed, .catalog_receipt = result };
        if (result.state == .rejected) return error.Canceled;
        io.sleep(.fromMilliseconds(20), .awake) catch break;
    }
    return .{ .mutation_outcome = .committed_pending, .catalog_receipt = result };
}

test "SQL catalog recovery table tokens reject malformed and substituted identities before native observation" {
    const target: domain.Target = .{ .table = "usage_records" };
    const valid: Token = .{ .target = target, .request = .{
        .lookup = .{ .group = 21, .table = 7, .id = @splat(1) },
        .effect = .{ .admission = .{
            .incarnation = @splat('f'),
            .effects = .{ .digest = @splat(1), .root_table = 7, .survivors = 0, .counters = 0, .views = 0 },
            .scope = .{ .database = 1, .namespace = 2, .creator = .bootstrap, .target_digest = root.Scope.targetDigest(target.database, target.namespace, target.table) },
        } },
    } };
    try requireToken(valid);
    var invalid = valid;
    invalid.target.table = "";
    try std.testing.expectError(error.InvalidSqlRecoveryToken, requireToken(invalid));
    invalid = valid;
    invalid.request.lookup.table = 0;
    try std.testing.expectError(error.InvalidSqlRecoveryToken, requireToken(invalid));
    invalid = valid;
    invalid.request.effect.admission.scope.namespace = 0;
    try std.testing.expectError(error.InvalidSqlRecoveryToken, requireToken(invalid));
    invalid = valid;
    invalid.target.table = "another_table";
    try std.testing.expectError(error.InvalidSqlRecoveryToken, requireToken(invalid));
    invalid = valid;
    invalid.version = 5;
    try std.testing.expectError(error.InvalidSqlRecoveryToken, requireToken(invalid));
    invalid = valid;
    invalid.request.effect = .{ .publication = 1 };
    try std.testing.expectError(error.InvalidSqlRecoveryToken, requireToken(invalid));
}
