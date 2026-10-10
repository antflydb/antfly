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

//! Incarnation-fenced configuration for a managed CDC controller. Provider
//! ownership remains in its durable workflow; Antfly owns CDC execution.
const std = @import("std");
const local = @import("antfly_local_sources");
const api = @import("http_server.zig");
const authorization = local.api_stored_destination_authorization;
const A = std.mem.Allocator;
const Input = struct {
    table_id: u64,
    expected_sources_hash: []const u8,
    replication_sources: std.json.Value,
};
pub fn configure(a: A, server: *api.ApiHttpServer, physical: []const u8, bound_id: ?u64, identity: ?api.AuthenticatedIdentity, context: local.api_operation.RequestContext, body: []const u8) ![]u8 {
    if (identity) |value| {
        if (!api.permissionsAllow(value.permissions, .table, physical, .admin) or api.effectiveRowFilterJson(value, physical) != null) return error.Forbidden;
    }
    if (body.len > 4 * 1024 * 1024) return error.InvalidCreateTableRequest;
    var parsed = try std.json.parseFromSlice(Input, a, body, .{});
    defer parsed.deinit();
    if (parsed.value.replication_sources != .array) return error.InvalidCreateTableRequest;
    var snapshot = (try server.source.linearizableSnapshot(context)) orelse return error.ReadUnavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = @import("tables.zig").findTableByName(&snapshot, physical) orelse return error.TableNotFound;
    if (table.table_id != parsed.value.table_id or (bound_id != null and bound_id.? != table.table_id)) return error.TableGenerationChanged;
    const hash = sourcesHash(table.replication_sources_json);
    if (!std.mem.eql(u8, &hash, parsed.value.expected_sources_hash)) return error.TableGenerationChanged;
    // Managed PostgreSQL currently uses the native CDC executor. Object
    // notifications use lake reconciliation, not a synthetic replication slot.
    if (table.storage.engine == .object or server.cfg.deployment_mode == .standalone) return error.UnsupportedOperation;
    for (parsed.value.replication_sources.array.items) |source| {
        if (source != .object) return error.InvalidCreateTableRequest;
    }
    const raw = try std.json.Stringify.valueAlloc(a, parsed.value.replication_sources, .{});
    defer a.free(raw);
    const sealed = try authorization.sealReplicationSourcesJsonForPrincipalAlloc(a, raw, physical, api.storedDestinationPrincipal(identity), .{ .manager = server.cfg.user_manager, .auth_enabled = server.cfg.auth_enabled });
    defer a.free(sealed);
    var replacement = table.*;
    replacement.replication_sources_json = sealed;
    try server.source.replaceTableDefinition(table.*, replacement);
    const next_hash = sourcesHash(sealed);
    return std.json.Stringify.valueAlloc(a, .{ .table_id = table.table_id, .sources_hash = &next_hash, .state = "configured" }, .{});
}
pub fn sourcesHash(bytes: []const u8) [64]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    return std.fmt.bytesToHex(hash, .lower);
}

pub fn status(a: A, server: *api.ApiHttpServer, physical: []const u8, bound_id: ?u64, identity: ?api.AuthenticatedIdentity, context: local.api_operation.RequestContext) ![]u8 {
    if (identity) |value| if (!api.permissionsAllow(value.permissions, .table, physical, .admin) or api.effectiveRowFilterJson(value, physical) != null) return error.Forbidden;
    var snapshot = (try server.source.linearizableSnapshot(context)) orelse return error.ReadUnavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    const table = @import("tables.zig").findTableByName(&snapshot, physical) orelse return error.TableNotFound;
    if (bound_id) |id| if (id != table.table_id) return error.TableGenerationChanged;
    var sources = try std.json.parseFromSlice(std.json.Value, a, table.replication_sources_json, .{});
    defer sources.deinit();
    const hash = sourcesHash(table.replication_sources_json);
    var progress: std.ArrayList(@import("../metadata/table_manager.zig").ReplicationSourceStatusRecord) = .empty;
    defer progress.deinit(a);
    for (snapshot.replication_source_statuses) |record| if (record.table_id == table.table_id) try progress.append(a, record);
    return std.json.Stringify.valueAlloc(a, .{ .table_id = table.table_id, .sources_hash = &hash, .replication_sources = sources.value, .progress = progress.items }, .{});
}

test "lake SQL managed source replacement checks native definition hash and table incarnation" {
    const a = std.testing.allocator;
    const metadata = @import("../metadata/table_manager.zig");
    const metadata_api = @import("../metadata/api.zig");
    const Source = struct {
        table: [1]metadata.TableRecord,
        replacements: usize = 0,
        fn snapshot(raw: *anyopaque, context: local.api_operation.RequestContext) !?metadata_api.AdminSnapshot {
            try context.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .status = .{ .metadata_group_id = 1, .metrics = .{} }, .tables = &self.table, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn replace(raw: *anyopaque, expected: metadata.TableRecord, replacement: metadata.TableRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!metadata.tableDefinitionsEqual(self.table[0], expected)) return error.TableGenerationChanged;
            const owned = try std.testing.allocator.dupe(u8, replacement.replication_sources_json);
            std.testing.allocator.free(self.table[0].replication_sources_json);
            self.table[0].replication_sources_json = owned;
            self.replacements += 1;
        }
    };
    var source: Source = .{ .table = .{.{ .table_id = 7, .name = "hn", .schema_json = "{}", .indexes_json = "{}", .replication_sources_json = try a.dupe(u8, "[]") }} };
    defer a.free(source.table[0].replication_sources_json);
    var backend = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{});
    defer backend.deinit();
    var server = api.ApiHttpServer.init(a, .{ .backend_runtime = backend.ptr(), .deployment_mode = .standalone }, .{ .ptr = &source, .vtable = &.{ .status = undefined, .linearizable_snapshot = Source.snapshot, .free_admin_snapshot = Source.free, .replace_table_definition = Source.replace } }, .{ .ptr = &source, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined } }, null);
    defer server.deinit();
    const hash = sourcesHash("[]");
    const body = try std.fmt.allocPrint(a, "{{\"table_id\":7,\"expected_sources_hash\":\"{s}\",\"replication_sources\":[]}}", .{hash});
    defer a.free(body);
    try std.testing.expectError(error.TableGenerationChanged, configure(a, &server, "hn", 8, null, .{}, body));
    try std.testing.expectError(error.UnsupportedOperation, configure(a, &server, "hn", 7, null, .{}, body));
    server.cfg.deployment_mode = .distributed;
    const result = try configure(a, &server, "hn", 7, null, .{}, body);
    defer a.free(result);
    try std.testing.expectEqual(@as(usize, 1), source.replacements);
    const wrong = "{\"table_id\":7,\"expected_sources_hash\":\"stale\",\"replication_sources\":[]}";
    try std.testing.expectError(error.TableGenerationChanged, configure(a, &server, "hn", 7, null, .{}, wrong));
    try std.testing.expectEqual(@as(usize, 1), source.replacements);
}
