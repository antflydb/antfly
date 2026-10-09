// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Native table binding, authorization and Iceberg catalog operations. File
//! commits are explicit; ordinary row batch writes cannot bypass the lake policy.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.serverless_external_source_mod.lake_catalog;
const configured = @import("../serverless/configured_object_store_support.zig");
const server_api = @import("http_server.zig");
const operation = local.api_operation;
const A = std.mem.Allocator;
pub const Action = enum { load, create, commit, resolve, changes, maintenance };
pub const Request = struct {
    action: Action,
    body: []const u8 = "",
    commit_id: []const u8 = "",
    request_hash: []const u8 = "",
};
pub const Response = struct {
    status: u16,
    body: []u8,
    pub fn deinit(self: *Response, a: A) void {
        a.free(self.body);
    }
};
pub fn execute(a: A, server: *server_api.ApiHttpServer, physical: []const u8, expected_id: ?u64, identity: ?server_api.AuthenticatedIdentity, context: operation.RequestContext, request: Request) !Response {
    const mutation = request.action == .create or request.action == .commit or request.action == .changes or request.action == .maintenance;
    if (identity) |value| {
        if (!server_api.permissionsAllow(value.permissions, .table, physical, if (mutation) .admin else .read)) return error.Forbidden;
        // Catalog writes are table-wide file commits, not row-policy mutations.
        if (mutation and server_api.effectiveRowFilterJson(value, physical) != null) return error.Forbidden;
    }
    var snapshot = (try server.source.linearizableSnapshot(context)) orelse return error.ReadUnavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = @import("tables.zig").findTableByName(&snapshot, physical) orelse return error.TableNotFound;
    if (expected_id) |id| if (id != table.table_id) return error.TableGenerationChanged;
    var source = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, table.schema_json)) orelse return error.InvalidLakeCatalog;
    defer source.deinit(a);
    if (source.binding.catalog == null) return error.InvalidLakeCatalog;
    const options: configured.BindingObjectStoreOpenOptions = .{ .node_config = server.cfg.node_config, .secret_store = server.cfg.secret_store, .catalog_table_id = table.table_id, .catalog_generation = table.object_storage_generation };
    const lake_context: catalog.types.Context = .{ .io = server.sharedApiNetworkIo(), .deadline_ns = context.deadline_ns, .cancellation = if (context.cancellation.ptr != null and context.cancellation.is_cancelled_fn != null) .{ .ptr = context.cancellation.ptr.?, .is_cancelled_fn = context.cancellation.is_cancelled_fn.? } else null };
    if (request.action == .maintenance) {
        const maintenance = @import("lake_maintenance.zig");
        const result = try maintenance.execute(a, server, table.*, source.binding, options, lake_context, context, request.body);
        if (result.mutated) server.notifyLakeCommit(physical) catch |err| std.log.warn("lake maintenance publication wakeup deferred table={s} err={s}", .{ physical, @errorName(err) });
        return .{ .status = 200, .body = result.body };
    }
    if (request.action == .changes) {
        const lsn = try @import("../serverless/lake_ingestion.zig").accept(a, source.binding, options, lake_context, request.body);
        server.notifyLakeCommit(physical) catch |err| std.log.warn("lake ingestion wakeup deferred table={s} err={s}", .{ physical, @errorName(err) });
        return .{ .status = 202, .body = try std.json.Stringify.valueAlloc(a, .{ .state = "accepted", .wal_lsn = lsn, .searchable = false }, .{}) };
    }
    if (!mutation) {
        const call: configured.CatalogOperation = if (request.action == .load) .load else .{ .resolve = .{ .id = request.commit_id, .hash = request.request_hash } };
        if (request.action == .resolve and (request.commit_id.len == 0 or request.commit_id.len > 256 or request.request_hash.len != 64)) return error.InvalidLakeCommit;
        var result = try configured.executeLakeCatalogAlloc(a, source.binding, options, lake_context, call);
        defer result.deinit(a);
        if (request.action == .resolve and result == .outcome and result.outcome == .committed) {
            server.notifyLakeCommit(physical) catch |err| std.log.warn("lake publication wakeup deferred table={s} err={s}", .{ physical, @errorName(err) });
        }
        return encode(a, result, if (request.action == .load) "loaded" else null, request.commit_id, request.request_hash);
    }
    if (request.body.len > catalog.types.max_commit_bytes) return error.LakeMetadataTooLarge;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, request.body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const root = &parsed.value;
    if (root.* != .object) return error.InvalidLakeCommit;
    const id = try catalog.metadata.str(try catalog.metadata.get(root.*, "commit_id"));
    const expected = if (request.action == .create) "<create>" else try catalog.metadata.str(try catalog.metadata.get(root.*, "expected_metadata_location"));
    _ = root.object.swapRemove("commit_id");
    _ = root.object.swapRemove("expected_metadata_location");
    if (request.action == .create) {
        try root.object.put(parsed.arena.allocator(), "location", .{ .string = source.binding.source_uri });
    }
    const body = try std.json.Stringify.valueAlloc(a, root.*, .{});
    defer a.free(body);
    const c: catalog.types.Commit = .{ .id = id, .expected_metadata_location = expected, .body = body, .timestamp_ms = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms) };
    try c.validate();
    const call: configured.CatalogOperation = if (request.action == .create) .{ .create = c } else .{ .commit = c };
    var result = configured.executeLakeCatalogAlloc(a, source.binding, options, lake_context, call) catch |err| switch (err) {
        error.LakeCommitOutcomeUnknown, error.LakeCommitInProgress => return .{ .status = 202, .body = try std.json.Stringify.valueAlloc(a, .{ .state = "unknown", .commit_id = c.id, .request_hash = &catalog.types.commitHash(c), .searchable = false }, .{}) },
        else => return err,
    };
    defer result.deinit(a);
    if (request.action == .create and std.mem.eql(u8, source.binding.schema_fingerprint, "auto")) {
        const prepared = (try @import("lake_schema_detection.zig").prepare(a, table.schema_json, options, lake_context)) orelse return error.InvalidLakeMetadata;
        defer a.free(prepared);
        // Finalizing an auto fingerprint changes the durable runtime schema.
        // Publish a new schema version so a native owner can reconcile/reopen
        // it instead of rejecting a different layout at the same version.
        const table_api = @import("tables.zig");
        const version = std.math.add(u32, try table_api.schemaVersion(table.schema_json), 1) catch return error.SchemaVersionExhausted;
        const normalized = try table_api.normalizeSchemaVersion(a, prepared, version);
        defer a.free(normalized);
        var replacement = table.*;
        replacement.schema_json = normalized;
        server.source.replaceTableDefinition(table.*, replacement) catch return .{ .status = 202, .body = try std.json.Stringify.valueAlloc(a, .{ .state = "lake_committed", .commit_id = c.id, .request_hash = &catalog.types.commitHash(c), .searchable = false, .binding_ready = false }, .{}) };
    }
    server.notifyLakeCommit(physical) catch |err| std.log.warn("lake publication wakeup deferred table={s} err={s}", .{ physical, @errorName(err) });
    return encode(a, result, "lake_committed", c.id, &catalog.types.commitHash(c));
}
fn encode(a: A, result: configured.CatalogResult, state: ?[]const u8, id: []const u8, hash: []const u8) !Response {
    if (result == .outcome) return .{ .status = 200, .body = try std.json.Stringify.valueAlloc(a, .{ .state = @tagName(result.outcome), .commit_id = id, .request_hash = hash }, .{}) };
    var metadata = try std.json.parseFromSlice(std.json.Value, a, result.table.metadata_json, .{});
    defer metadata.deinit();
    return .{ .status = 200, .body = try std.json.Stringify.valueAlloc(a, .{ .state = state.?, .commit_id = if (id.len == 0) null else id, .request_hash = if (hash.len == 0) null else hash, .metadata_location = result.table.metadata_location, .metadata = metadata.value, .searchable = if (std.mem.eql(u8, state.?, "lake_committed")) @as(?bool, false) else null }, .{ .emit_null_optional_fields = false }) };
}
pub fn errorStatus(err: anyerror) u16 {
    return switch (err) {
        error.Forbidden, error.LakeCatalogForbidden, error.ExternalLakeReadOnly, error.UnsupportedExternalLakeCredentialRef, error.LakeVacuumOwnershipRequired => 403,
        error.LakeTableNotFound, error.TableNotFound => 404,
        error.TableGenerationChanged, error.LakeCommitConflict, error.LakeCommitIdReused, error.LakeRelocationRequired, error.WalIdempotencyConflict, error.LakeCheckpointConflict, error.LakeSourceConflict, error.LakeMaintenanceAlreadyStarted => 409,
        error.InvalidLakeChangeBatch, error.InvalidLakeRow, error.UnsupportedLakeWriteType, error.LakeWriteTooLarge, error.InvalidLakeMaintenanceLimits => 400,
        error.InvalidLakeCatalog, error.InvalidLakeMetadata, error.InvalidLakeCommit, error.UnsupportedLakeRequirement, error.UnsupportedLakeUpdate, error.UnsupportedLakeFormatVersion, error.LakeMetadataTooLarge, error.UnexpectedToken, error.UnknownField, error.MissingField => 400,
        error.DeadlineExceeded, error.Timeout => 504,
        else => 503,
    };
}
