// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Capture the metadata-owned attribute directory under the existing backup
//! definition fence. Physical files alone cannot authenticate historical
//! attribute identity after DROP/ADD/rename churn.
const std = @import("std");
const local = @import("antfly_local_sources");
const operation = local.api_operation;
const snapshot_contract = @import("../system_catalog/trigger_snapshot.zig");
const A = std.mem.Allocator;
pub const Capture = struct { encoded: []const u8, digest: [32]u8, incarnation: [16]u8 };
pub fn capture(a: A, source: anytype, context: operation.RequestContext, table: local.common_topology_records.TableRecord) !?Capture {
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, if (table.schema_json.len == 0) "{}" else table.schema_json, .{});
    if (root != .object) return error.InvalidSqlTriggerDefinition;
    const raw = root.object.get("sql_triggers") orelse return null;
    var state = try local.schema_mod.table_triggers.parseValue(a, raw);
    defer state.deinit();
    const Source = switch (@typeInfo(@TypeOf(source))) {
        .pointer => |value| value.child,
        else => @TypeOf(source),
    };
    if (comptime !@hasDecl(Source, "systemCatalog")) return error.MetadataCapabilityUnavailable;
    var request = context;
    request.setting_admin = true;
    const bytes = try source.systemCatalog(a, request, .{ .sql_trigger_snapshot = .{ .table_id = table.table_id } });
    const snapshot = try std.json.parseFromSliceLeaky(snapshot_contract.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    if (snapshot.incoming_tables.len > snapshot_contract.max_incoming_tables or snapshot.incoming_records.len != snapshot.incoming_tables.len) return error.InvalidSqlBackendResponse;
    if (snapshot.record.table_id != table.table_id or !std.mem.eql(u8, snapshot.record.name, table.name) or !std.mem.eql(u8, snapshot.record.schema_json, table.schema_json)) return error.CatalogGenerationChanged;
    const found = switch (snapshot.source.value) {
        .table => |value| value,
        else => return error.InvalidSqlBackendResponse,
    };
    if (!found.binding.sql_trigger_functions_active or !std.mem.eql(u8, &snapshot.epoch.incarnation, &state.value.metadata_incarnation)) return error.CatalogGenerationChanged;
    const encoded = found.column_identities_json orelse return error.InvalidSqlBackendResponse;
    var columns = try local.system_catalog_column_identities.State.decode(a, encoded);
    defer columns.deinit();
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(table.schema_json, &digest, .{});
    const header = try std.json.parseFromSliceLeaky(struct { version: u32 = 0 }, a, table.schema_json, .{ .ignore_unknown_fields = true });
    if (columns.value.table_id != table.table_id or columns.value.epoch.version != header.version or !std.mem.eql(u8, &columns.value.epoch.digest, &digest)) return error.CatalogGenerationChanged;
    return .{ .encoded = try columns.value.encode(a), .digest = try local.sql_catalog.trigger_restore.sourceColumnsDigest(a, columns.value), .incarnation = snapshot.epoch.incarnation };
}
pub fn decorate(a: A, source: anytype, context: operation.RequestContext, table: local.common_topology_records.TableRecord, manifest: *local.api_backup_contract.TableBackupManifest) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const captured = (try capture(arena.allocator(), source, context, table)) orelse return;
    const encoded = try a.dupe(u8, captured.encoded);
    if (manifest.sql_column_identities_json.len != 0) a.free(manifest.sql_column_identities_json);
    manifest.sql_column_identities_json = encoded;
}
