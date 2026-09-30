// Copyright 2026 Antfly, Inc.
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

//! Shared staging plan construction for the existing durable restore job.
//! Source artifacts are already authenticated by backups.zig. Callers retain
//! this allocation arena until the metadata reservation has copied the plan.
const std = @import("std");
const metadata = @import("../metadata/table_manager.zig");
const staging = @import("../metadata/restore_staging.zig");
const backup = @import("backup_contract.zig");
const group_ids = @import("../common/group_ids.zig");

pub const SourceTable = struct {
    source_table_id: u64,
    manifest: *const backup.TableBackupManifest,
    catalog_binding: ?@import("../system_catalog/domain.zig").Resource = null,
    existing_name: ?[]const u8 = null,
    destination_name: ?[]const u8 = null,
    /// Exact primary identities from the authenticated cohort, in manifest
    /// shard order. They are not inferred from potentially historical group IDs.
    namespaces: []const @import("../storage/db/doc_identity.zig").Namespace = &.{},
    seals: []const @import("../storage/db/native_backup_seal.zig").Handle = &.{},
};
pub const Selection = struct { plan: ?staging.Plan, skipped: []const []const u8 };

/// Explicit selections are closed over outgoing dependencies in both schema
/// generations. Never silently enlarge authorization scope, and never satisfy
/// a missing parent with an unrelated live table or another backup cut.
pub fn validateSelection(arena: std.mem.Allocator, sources: []const SourceTable) !void {
    var names: std.StringHashMapUnmanaged(void) = .empty;
    defer names.deinit(arena);
    for (sources) |source| {
        const slot = try names.getOrPut(arena, source.manifest.table_name);
        if (slot.found_existing) return error.DuplicateTableName;
    }
    for (sources) |source| for ([_][]const u8{ source.manifest.schema_json, source.manifest.read_schema_json }) |json| {
        if (json.len == 0) continue;
        var schema = try @import("../schema/mod.zig").parseValidatedTableSchema(arena, json);
        defer schema.deinit(arena);
        if (schema.foreign_keys) |fks| for (fks.value) |fk| {
            if (!names.contains(fk.parent_table)) return error.RestoreDependencyMissing;
        };
    };
}

/// Verify the aggregate's common-cut proof against one already authenticated
/// table manifest. A native file checksum alone does not prove a shared cut.
pub fn cohortSource(arena: std.mem.Allocator, proof: @import("../metadata/backup_cohort.zig").Job, manifest: *const backup.TableBackupManifest) !SourceTable {
    try proof.validate();
    if (!std.mem.eql(u8, @tagName(proof.artifact_format), @tagName(manifest.format))) return error.BackupIntegrityFailure;
    if (proof.state.phase != .publishing or proof.state.cursor != 0 or proof.seals.len != proof.state.owners.len or manifest.table_id == 0) return error.BackupIntegrityFailure;
    var owners: std.AutoHashMapUnmanaged(u64, usize) = .empty;
    var seals: std.AutoHashMapUnmanaged(u64, @import("../storage/db/native_backup_seal.zig").Handle) = .empty;
    for (proof.state.owners, 0..) |owner, index| {
        const slot = try owners.getOrPut(arena, owner.fence.owner_group_id);
        if (slot.found_existing) return error.BackupIntegrityFailure;
        slot.value_ptr.* = index;
    }
    for (proof.seals) |receipt| {
        const index = owners.get(receipt.handle.fence.owner_group_id) orelse return error.BackupIntegrityFailure;
        const owner = proof.state.owners[index];
        if (!owner.fence.eql(receipt.handle.fence) or receipt.source_node_id != owner.capture_node_id or receipt.source_node_id == 0) return error.BackupIntegrityFailure;
        const slot = try seals.getOrPut(arena, owner.fence.owner_group_id);
        if (slot.found_existing) return error.BackupIntegrityFailure;
        slot.value_ptr.* = receipt.handle;
    }
    const selected = for (proof.tables) |table| {
        if (std.mem.eql(u8, table.name, manifest.table_name)) break table;
    } else return error.BackupIntegrityFailure;
    if (selected.table_id != manifest.table_id) return error.BackupIntegrityFailure;
    const declaration = selected.manifest_definition orelse return error.BackupIntegrityFailure;
    if (!std.mem.eql(u8, &declaration, &@import("../metadata/backup_cohort.zig").manifestDefinition(manifest.table_name, manifest.description, manifest.schema_json, manifest.read_schema_json, manifest.indexes_json, manifest.replication_sources_json))) return error.BackupIntegrityFailure;
    const namespaces = try arena.alloc(@import("../storage/db/doc_identity.zig").Namespace, manifest.shards.len);
    const handles = try arena.alloc(@import("../storage/db/native_backup_seal.zig").Handle, manifest.shards.len);
    var matched: usize = 0;
    for (proof.state.owners) |owner| if (std.mem.eql(u8, owner.table_name, manifest.table_name)) {
        matched += 1;
    };
    if (matched != manifest.shards.len) return error.BackupIntegrityFailure;
    var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
    for (manifest.shards, 0..) |shard, index| {
        const owner = proof.state.owners[owners.get(shard.group_id) orelse return error.BackupIntegrityFailure];
        if (!std.mem.eql(u8, owner.table_name, manifest.table_name) or !std.mem.eql(u8, owner.range_start, shard.start_key) or !std.mem.eql(u8, owner.range_end, shard.end_key orelse "") or owner.fence.namespace.table_id != manifest.table_id or
            owner.fence.namespace.shard_id != (if (shard.doc_identity_shard_id == 0) shard.group_id else shard.doc_identity_shard_id) or owner.fence.namespace.range_id != (if (shard.doc_identity_range_id == 0) (if (shard.range_id == 0) shard.group_id else shard.range_id) else shard.doc_identity_range_id)) return error.BackupIntegrityFailure;
        const slot = try seen.getOrPut(arena, shard.group_id);
        if (slot.found_existing) return error.BackupIntegrityFailure;
        namespaces[index] = owner.fence.namespace;
        handles[index] = seals.get(shard.group_id) orelse return error.BackupIntegrityFailure;
    }
    return .{ .source_table_id = manifest.table_id, .manifest = manifest, .namespaces = namespaces, .seals = handles };
}

fn identity(id: staging.Id, source_id: u64, ordinal: u64, domain: []const u8) u64 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly restore staging identity v1");
    hash.update(domain);
    hash.update(&id);
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], source_id, .little);
    std.mem.writeInt(u64, bytes[8..16], ordinal, .little);
    hash.update(&bytes);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return group_ids.dataGroupIdFromHash(std.mem.readInt(u64, digest[0..8], .little));
}

/// All modes use isolated fresh generations. Overwrite includes the exact old
/// generation as a CAS expectation; it does not destructively drop a live table.
/// Skipping a dependency cannot silently bind a restored FK to unrelated data.
pub fn buildPlan(arena: std.mem.Allocator, id: staging.Id, cohort_digest: staging.Digest, sources: []const SourceTable, current_tables: []const metadata.TableRecord, current_ranges: []const metadata.RangeRecord, mode: []const u8) !Selection {
    if (!std.mem.eql(u8, mode, "fail_if_exists") and !std.mem.eql(u8, mode, "skip_if_exists") and !std.mem.eql(u8, mode, "overwrite")) return error.InvalidRestoreMode;
    if (sources.len == 0 or sources.len > 128) return error.InvalidRestoreStaging;
    try validateSelection(arena, sources);
    var targets = std.ArrayListUnmanaged(staging.Target).empty;
    var skipped = std.ArrayListUnmanaged([]const u8).empty;
    for (sources) |source| {
        const manifest = source.manifest;
        if (source.source_table_id == 0 or manifest.shards.len == 0 or manifest.shards.len > 4096) return error.InvalidRestoreStaging;
        try @import("../schema/restore_migration.zig").validate(arena, manifest.schema_json, manifest.read_schema_json);
        const existing: ?metadata.TableRecord = for (current_tables) |table| {
            if (std.mem.eql(u8, table.name, source.existing_name orelse manifest.table_name)) break table;
        } else null;
        if (existing != null) {
            if (std.mem.eql(u8, mode, "fail_if_exists")) return error.TableAlreadyExists;
            if (std.mem.eql(u8, mode, "skip_if_exists")) {
                try skipped.append(arena, manifest.table_name);
                continue;
            }
        }
        const table_id = identity(id, source.source_table_id, 0, "table");
        const ranges = try arena.alloc(metadata.RangeRecord, manifest.shards.len);
        if (source.namespaces.len != 0 and source.namespaces.len != manifest.shards.len) return error.InvalidRestoreStaging;
        if (source.seals.len != 0 and source.seals.len != manifest.shards.len) return error.InvalidRestoreStaging;
        const artifacts: []staging.SourceArtifact = if (source.namespaces.len != 0) try arena.alloc(staging.SourceArtifact, manifest.shards.len) else &.{};
        for (manifest.shards, ranges, 0..) |shard, *range, ordinal| {
            const group_id = identity(id, source.source_table_id, @intCast(ordinal), "group");
            range.* = .{ .table_id = table_id, .group_id = group_id, .range_id = group_id, .doc_identity_shard_id = group_id, .doc_identity_range_id = group_id, .start_key = shard.start_key, .end_key = shard.end_key };
            if (artifacts.len != 0) {
                var artifact_digest: [32]u8 = undefined;
                if (shard.artifact_sha256.len != 64) return error.BackupIntegrityFailure;
                _ = std.fmt.hexToBytes(&artifact_digest, shard.artifact_sha256) catch return error.BackupIntegrityFailure;
                artifacts[ordinal] = .{ .target_group_id = group_id, .source_namespace = source.namespaces[ordinal], .cohort_seal = if (source.seals.len == 0) null else source.seals[ordinal], .format = switch (manifest.format) {
                    .native => .native,
                    .portable => .portable,
                }, .snapshot_path = shard.snapshot_path, .artifact_size_bytes = shard.artifact_size_bytes, .artifact_sha256 = artifact_digest, .native_manifest_size_bytes = shard.native_manifest_size_bytes, .native_manifest_sha256 = shard.native_manifest_sha256 };
            }
        }
        metadata.sortKeyspaceRanges(metadata.RangeRecord, ranges);
        if (manifest.format == .portable) {
            var source_schema = try @import("../schema/mod.zig").parseValidatedTableSchema(arena, manifest.schema_json);
            defer source_schema.deinit(arena);
            if ((source.seals.len != 0 or source_schema.storage_mode == .relational) and
                (manifest.shards.len == 0 or manifest.shards[0].accepted_generation_summary_digest == null))
                return error.RestoreSourceProofMissing;
        }
        const admissions: []staging.SourceRangeGenerationAdmissions = if (manifest.format == .portable and manifest.shards.len != 0 and manifest.shards[0].accepted_generation_summary_digest != null) blk: {
            if (source.namespaces.len != manifest.shards.len) return error.RestoreSourceProofMissing;
            const proofs = try arena.alloc(staging.SourceRangeGenerationAdmissions, manifest.shards.len);
            for (manifest.shards, proofs, 0..) |shard, *proof, ordinal| {
                proof.* = .{
                    .target_group_id = identity(id, source.source_table_id, @intCast(ordinal), "group"),
                    .source_namespace = source.namespaces[ordinal],
                    .entries = shard.accepted_generation_summary,
                    .digest = shard.accepted_generation_summary_digest orelse return error.RestoreSourceProofMissing,
                };
            }
            break :blk proofs;
        } else &.{};
        var target: staging.Target = .{
            .source_table_id = source.source_table_id,
            .source_table_name = if (admissions.len != 0) manifest.table_name else "",
            .table = .{ .table_id = table_id, .name = source.destination_name orelse manifest.table_name, .description = manifest.description, .schema_json = manifest.schema_json, .read_schema_json = manifest.read_schema_json, .indexes_json = manifest.indexes_json, .replication_sources_json = if (manifest.replication_sources_json.len == 0) "[]" else manifest.replication_sources_json, .placement_role = if (existing) |table| table.placement_role else "data", .desired_replica_count = if (existing) |table| table.desired_replica_count else 3, .min_ranges = @intCast(ranges.len) },
            .ranges = ranges,
            .source_artifacts = artifacts,
            .source_generation_admissions = admissions,
        };
        if (source.catalog_binding) |binding| {
            target.catalog_binding = binding;
            target.catalog_binding.?.id = table_id;
            target.catalog_binding.?.storage_name = target.table.name;
        }
        if (existing) |old| {
            var old_ranges = std.ArrayListUnmanaged(metadata.RangeRecord).empty;
            for (current_ranges) |range| if (range.table_id == old.table_id) try old_ranges.append(arena, range);
            metadata.sortKeyspaceRanges(metadata.RangeRecord, old_ranges.items);
            target.replace = .{ .table = old, .ranges = old_ranges.items };
        }
        try targets.append(arena, target);
    }
    if (targets.items.len == 0) return .{ .plan = null, .skipped = skipped.items };
    try staging.prepareTargetProjectionsAlloc(arena, targets.items);
    const plan: staging.Plan = .{ .id = id, .cohort_digest = cohort_digest, .targets = targets.items, .skipped_tables = skipped.items };
    try plan.validate(arena);
    return .{ .plan = plan, .skipped = skipped.items };
}

test "restore staging driver shares stable identities and existing destination modes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const manifest: backup.TableBackupManifest = .{ .format = .native, .backup_id = "daily", .table_name = "docs", .description = "", .schema_json = "{}", .read_schema_json = "", .indexes_json = "{}", .replication_sources_json = "[]", .shards = &.{.{ .group_id = 301, .start_key = "", .snapshot_path = "daily/301" }} };
    const sources = [_]SourceTable{.{ .source_table_id = 9, .manifest = &manifest }};
    const id = try staging.idForAttempt(7, 1);
    const first = try buildPlan(a, id, @splat(3), &sources, &.{}, &.{}, "fail_if_exists");
    const retry = try buildPlan(a, id, @splat(3), &sources, &.{}, &.{}, "fail_if_exists");
    try std.testing.expectEqual(first.plan.?.targets[0].table.table_id, retry.plan.?.targets[0].table.table_id);
    try std.testing.expect(first.plan.?.targets[0].table.table_id != 9);
    const copied = try buildPlan(a, id, @splat(3), &.{.{
        .source_table_id = 9,
        .manifest = &manifest,
        .existing_name = "copy",
        .destination_name = "copy",
        .catalog_binding = .{ .kind = .table, .id = 0, .parent_id = 2, .name = "copy", .storage_name = "copy" },
    }}, &.{.{ .table_id = 9, .name = "docs", .schema_json = "{}" }}, &.{}, "fail_if_exists");
    try std.testing.expectEqualStrings("copy", copied.plan.?.targets[0].table.name);
    try std.testing.expectEqualStrings("docs", manifest.table_name);
    try std.testing.expect(copied.plan.?.targets[0].replace == null);
    const catalog = @import("../system_catalog/domain.zig");
    const qualified = try catalog.restoreStorageNameAlloc(a, "table:" ++ "a" ** 32, .{
        .database = "d" ** 128,
        .namespace = "n" ** 128,
        .table = "t" ** 255,
    });
    const long_copy = try buildPlan(a, id, @splat(3), &.{.{
        .source_table_id = 9,
        .manifest = &manifest,
        .existing_name = qualified,
        .destination_name = qualified,
        .catalog_binding = .{ .kind = .table, .id = 0, .parent_id = 2, .name = "t" ** 255, .storage_name = qualified },
    }}, &.{}, &.{}, "fail_if_exists");
    const long_plan = long_copy.plan.?;
    const long_target = long_plan.targets[0];
    const bootstrap: @import("../storage/db/restore_staging_contract.zig").OwnerBootstrap = .{
        .scope = .{
            .plan_id = id,
            .plan_digest = try long_plan.digest(a),
            .source_artifact_digest = @splat(3),
            .source_namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 },
            .target_namespace = .{ .table_id = long_target.table.table_id, .shard_id = long_target.ranges[0].group_id, .range_id = long_target.ranges[0].group_id },
            .target_schema_digest = @splat(4),
        },
        .table_name = long_target.table.name,
        .schema_json = "{}",
        .read_schema_json = "",
        .indexes_json = "{}",
        .byte_range = .{ .start = "", .end = "" },
    };
    try bootstrap.validate();
    var name_key: [2048]u8 = undefined;
    _ = try staging.nameKey(&name_key, 1, qualified);
    const old: metadata.TableRecord = .{ .table_id = 99, .name = "docs", .schema_json = "{}" };
    const old_ranges = [_]metadata.RangeRecord{.{ .table_id = 99, .group_id = 401, .start_key = "" }};
    try std.testing.expectError(error.TableAlreadyExists, buildPlan(a, id, @splat(3), &sources, &.{old}, &old_ranges, "fail_if_exists"));
    const skipped = try buildPlan(a, id, @splat(3), &sources, &.{old}, &old_ranges, "skip_if_exists");
    try std.testing.expect(skipped.plan == null and skipped.skipped.len == 1);
    const replaced = try buildPlan(a, id, @splat(3), &sources, &.{old}, &old_ranges, "overwrite");
    try std.testing.expectEqual(@as(u64, 99), replaced.plan.?.targets[0].replace.?.table.table_id);
    try std.testing.expectEqual(@as(u64, 401), replaced.plan.?.targets[0].replace.?.ranges[0].group_id);
}

test "restore staging driver retains migration pair without source coverage" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var manifest: backup.TableBackupManifest = .{ .format = .native, .backup_id = "daily", .table_name = "docs", .description = "", .schema_json = "{\"version\":2}", .read_schema_json = "{\"version\":0}", .indexes_json = "{}", .replication_sources_json = "[]", .shards = &.{.{ .group_id = 301, .start_key = "", .snapshot_path = "daily/301" }} };
    const id = try staging.idForAttempt(7, 1);
    const selected = try buildPlan(a, id, @splat(3), &.{.{ .source_table_id = 9, .manifest = &manifest }}, &.{}, &.{}, "fail_if_exists");
    try std.testing.expectEqualStrings(manifest.read_schema_json, selected.plan.?.targets[0].table.read_schema_json);
    const before = try selected.plan.?.digest(a);
    manifest.read_schema_json = "{\"version\":1}";
    const changed = try buildPlan(a, id, @splat(3), &.{.{ .source_table_id = 9, .manifest = &manifest }}, &.{}, &.{}, "fail_if_exists");
    try std.testing.expect(!std.mem.eql(u8, &before, &try changed.plan.?.digest(a)));
    manifest.read_schema_json = manifest.schema_json;
    try std.testing.expectError(error.InvalidRestoreMigrationState, buildPlan(a, id, @splat(3), &.{.{ .source_table_id = 9, .manifest = &manifest }}, &.{}, &.{}, "fail_if_exists"));
}

test "restore staging cohort proof binds every source identity and durable seal" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cohort = @import("../metadata/backup_cohort.zig");
    const fence: @import("../storage/db/relational_integrity_topology_contract.zig").Fence = .{ .transition_id = 7, .attempt = 1, .owner_group_id = 301, .peer_group_id = 301, .role = .backup_snapshot, .namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 }, .catalog_digest = @splat(1) };
    const owner: cohort.Owner = .{ .table_name = "docs", .range_start = "", .range_end = "", .fence = fence, .artifact_id = "artifact", .capture_node_id = 44 };
    const receipt: cohort.SealReceipt = .{ .source_node_id = 44, .handle = .{ .fence = fence, .digest = @splat(2) } };
    var proof: cohort.Job = .{ .id = 7, .revision = 9, .attempt_id = "attempt", .backup_id = "daily", .location = "s3://archive/daily", .connection = "archive", .tables = &.{.{ .table_id = 9, .name = "docs", .definition = @splat(1), .manifest_definition = cohort.manifestDefinition("docs", "", "{}", "", "{}", "[]") }}, .state = .{ .phase = .publishing, .metadata_digest = @splat(3), .owners = &.{owner} }, .seals = &.{receipt} };
    var manifest: backup.TableBackupManifest = .{ .format = .native, .backup_id = "daily-docs", .table_name = "docs", .table_id = 9, .description = "", .schema_json = "{}", .read_schema_json = "", .indexes_json = "{}", .replication_sources_json = "[]", .shards = &.{.{ .group_id = 301, .start_key = "", .snapshot_path = "artifact/301", .artifact_size_bytes = 19, .artifact_sha256 = "ab" ** 32 }} };
    const source = try cohortSource(a, proof, &manifest);
    try std.testing.expect(source.namespaces[0].eql(fence.namespace));
    const selected = try buildPlan(a, try staging.idForAttempt(11, 1), @splat(9), &.{source}, &.{}, &.{}, "fail_if_exists");
    try std.testing.expectEqual(receipt.handle.digest, selected.plan.?.targets[0].source_artifacts[0].cohort_seal.?.digest);
    manifest.format = .portable;
    try std.testing.expectError(error.BackupIntegrityFailure, cohortSource(a, proof, &manifest));
    proof.artifact_format = .portable;
    const portable_source = try cohortSource(a, proof, &manifest);
    const portable_selected = try buildPlan(a, try staging.idForAttempt(11, 1), @splat(9), &.{portable_source}, &.{}, &.{}, "fail_if_exists");
    try std.testing.expectEqual(.portable, portable_selected.plan.?.targets[0].source_artifacts[0].format);
    proof.seals = &.{};
    try std.testing.expectError(error.BackupIntegrityFailure, cohortSource(a, proof, &manifest));
    proof.seals = &.{receipt};
    manifest.table_id = 10;
    try std.testing.expectError(error.BackupIntegrityFailure, cohortSource(a, proof, &manifest));
    manifest.table_id = 9;
    manifest.schema_json = "{\"version\":2}";
    try std.testing.expectError(error.BackupIntegrityFailure, cohortSource(a, proof, &manifest));
    manifest.schema_json = "{}";
    var wrong = receipt;
    wrong.handle.fence.admission_epoch += 1;
    proof.seals = &.{wrong};
    try std.testing.expectError(error.BackupIntegrityFailure, cohortSource(a, proof, &manifest));
}

test "restore staging partial selection is dependency closed across both schema generations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var parent: backup.TableBackupManifest = .{ .format = .portable, .backup_id = "daily-parent", .table_name = "parent", .description = "", .schema_json = "{}", .read_schema_json = "", .indexes_json = "{}", .replication_sources_json = "[]", .shards = &.{} };
    var child = parent;
    child.table_name = "child";
    child.schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"parent","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const parents = SourceTable{ .source_table_id = 1, .manifest = &parent };
    const children = SourceTable{ .source_table_id = 2, .manifest = &child };
    // The full cluster may contain unrelated tables: only outgoing closure
    // matters when publishing fresh identities in the selected subset.
    try validateSelection(a, &.{parents});
    try validateSelection(a, &.{ parents, children });
    try std.testing.expectError(error.RestoreDependencyMissing, validateSelection(a, &.{children}));
    child.read_schema_json = child.schema_json;
    child.schema_json = "{}";
    try std.testing.expectError(error.RestoreDependencyMissing, validateSelection(a, &.{children}));
    try validateSelection(a, &.{ parents, children });
    try std.testing.expectError(error.DuplicateTableName, validateSelection(a, &.{ parents, parents }));
}
