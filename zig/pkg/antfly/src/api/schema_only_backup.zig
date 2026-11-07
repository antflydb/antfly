// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Physical/base UNLOGGED backup captures only the fenced catalog definition.
//! No storage owner, primary rows, artifact checksum or cohort pin is invented.
const std = @import("std");
const local = @import("antfly_local_sources");
const backups = @import("backups.zig");
const A = std.mem.Allocator;
pub fn eligible(table: local.common_topology_records.TableRecord, format: backups.BackupFormat) bool {
    if (format != .native) return false;
    return if (table.sql_persistence) |native| native.kind == .unlogged else false;
}
pub fn capture(a: A, server: anytype, table: local.common_topology_records.TableRecord, expected: backups.TableBackupFence, backup_id: []const u8, format: backups.BackupFormat, input: local.api_operation.RequestContext) !backups.TableBackupManifest {
    if (!eligible(table, format)) return error.InvalidSqlRelationPersistence;
    var context = input;
    context.setting_admin = true;
    const raw = try server.source.systemCatalog(a, context, .{ .sql_relation_boot_read = .{ .recovery_capture = .{ .table_id = table.table_id } } });
    defer a.free(raw);
    var captured = try std.json.parseFromSlice(@import("../metadata/sql_relation_recovery_capture.zig").Capture, a, raw, .{});
    defer captured.deinit();
    const cut = captured.value;
    if (!eligible(cut.relation.record, format) or cut.relation.record.table_id != table.table_id or !std.mem.eql(u8, cut.relation.record.name, table.name) or cut.ranges.len != 1) return error.CatalogGenerationChanged;
    const definition = local.api_backup_contract.tableDefinitionDigest(table.table_id, cut.relation.record.name, cut.relation.record.description, cut.relation.record.schema_json, cut.relation.record.read_schema_json, cut.relation.record.indexes_json, cut.relation.record.replication_sources_json, cut.relation.record.sql_native_indexes_json);
    const topology = @import("../metadata/api.zig").catalogTableTopology(table.table_id, cut.ranges);
    if (expected.table_id != table.table_id or !std.mem.eql(u8, &expected.definition_digest, &definition) or expected.topology_range_count != topology.range_count or !std.mem.eql(u8, &expected.topology_digest, &topology.digest)) return error.CatalogGenerationChanged;
    if (expected.hasMetadataIdentity() and !std.mem.eql(u8, &expected.metadata_incarnation, &std.fmt.bytesToHex(cut.relation.epoch.incarnation, .lower))) return error.CatalogGenerationChanged;
    const range = cut.ranges[0];
    if (range.start_key.len != 0 or range.end_key != null) return error.InvalidSqlRelationPersistence;
    var manifest = try backups.createManifest(a, backup_id, format, &cut.relation.record, &.{.{ .group_id = range.group_id, .range_id = range.range_id, .doc_identity_shard_id = range.doc_identity_shard_id, .doc_identity_range_id = range.doc_identity_range_id, .start_key = "", .snapshot_path = "" }});
    errdefer manifest.deinit(a);
    try @import("sql_trigger_backup.zig").decorate(a, server.source, context, cut.relation.record, &manifest);
    return manifest;
}
