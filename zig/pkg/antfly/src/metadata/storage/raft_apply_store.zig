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

const std = @import("std");
const raft_engine = @import("raft_engine");
const fs_paths = @import("../../common/fs_paths.zig");
const threaded_io_limits = @import("../../common/threaded_io_limits.zig");
const group_ids = @import("../../common/group_ids.zig");
const docstore = @import("../../storage/docstore.zig");
const lsm_backend = @import("../../storage/lsm_backend.zig");
const metadata = @import("../domain.zig");
const metadata_reconciler = @import("../reconciler.zig");
const metadata_incarnation = @import("../incarnation.zig");
const transition_state = @import("../transition_state.zig");
const extension_domain = @import("../../extensions/mod.zig");
const metadata_table_manager = @import("../table_manager.zig");
const raft_catalog = @import("../../raft/catalog.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const raft_storage_mod = @import("../../raft/storage/mod.zig");
const wal_replica_state_mod = @import("../../raft/storage/wal_replica_state.zig");
const raft_state_machine = @import("../../raft/state_machine/mod.zig");

pub const AppliedMetadataBatch = struct {
    commit_index: u64,
    entries_bytes: []const u8,
};

pub const ExtensionMemberKey = struct {
    extension_name: []const u8,
    object_kind: extension_domain.ExtensionObjectKind,
    object_name: []const u8,
};

pub const ExtensionDependencyKey = struct {
    extension_name: []const u8,
    required_extension_name: []const u8,
    package_name: []const u8,
};

pub const ExtensionLifecycleDelta = struct {
    upsert_tables: []const metadata.TableRecord = &.{},
    upsert_installed_extensions: []const extension_domain.InstalledExtension = &.{},
    remove_installed_extensions: []const []const u8 = &.{},
    upsert_extension_members: []const extension_domain.ExtensionMember = &.{},
    remove_extension_members: []const ExtensionMemberKey = &.{},
    upsert_extension_dependencies: []const extension_domain.ExtensionDependency = &.{},
    remove_extension_dependencies: []const ExtensionDependencyKey = &.{},
};

pub const TableTransitionFence = struct {
    generation: u64 = 0,
    active_count: u32 = 0,

    pub fn active(self: @This()) bool {
        return self.active_count != 0;
    }
};

pub const TransitionCommand = union(enum) {
    initialize_metadata_incarnation: metadata_incarnation.MetadataClusterIncarnation,
    upsert_node: metadata.NodeRecord,
    register_node: metadata.NodeRecord,
    remove_node: struct {
        node_id: u64,
    },
    request_node_shutdown: struct {
        node_id: u64,
    },
    cancel_node_shutdown: struct {
        node_id: u64,
    },
    finalize_node_shutdown: struct {
        node_id: u64,
    },
    upsert_store: metadata.StoreRecord,
    register_store: metadata.StoreRecord,
    remove_store: struct {
        store_id: u64,
    },
    upsert_replica_intent: struct {
        expected_metadata_version: ?u64,
        expected_version_fence: u64,
        expected_target_drain_requested: bool,
        replacement: raft_reconciler.PlacementIntent,
    },
    remove_replica_intent: struct {
        group_id: u64,
        local_node_id: u64,
        expected_metadata_version: u64,
    },
    upsert_table: metadata.TableRecord,
    compare_and_replace_table: struct {
        expected: metadata.TableRecord,
        replacement: metadata.TableRecord,
    },
    remove_table: struct {
        table_id: u64,
        expected_transition_generation: u64,
    },
    upsert_schema_progress: metadata.SchemaProgressRecord,
    remove_schema_progress: struct {
        table_id: u64,
        node_id: u64,
    },
    upsert_restore_progress: metadata.RestoreProgressRecord,
    remove_restore_progress: struct {
        table_id: u64,
        node_id: u64,
        group_id: u64,
    },
    upsert_replication_source_status: metadata.ReplicationSourceStatusRecord,
    claim_replication_source_cutover: struct {
        expected_replication_sources_json: []const u8,
        expected_authority_id: u64,
        record: metadata.ReplicationSourceStatusRecord,
    },
    complete_replication_source_retirement: metadata.ReplicationSourceStatusRecord,
    upsert_range: metadata.RangeRecord,
    complete_restore_range: metadata.RestoreIntentIdentity,
    remove_range: struct {
        group_id: u64,
    },
    admit_split_transition: struct {
        expected_source_epoch: u64,
        record: metadata.SplitTransitionRecord,
    },
    upsert_split_transition: metadata.SplitTransitionRecord,
    remove_split_transition: struct {
        transition_id: u64,
    },
    upsert_merge_transition: metadata.MergeTransitionRecord,
    remove_merge_transition: struct {
        transition_id: u64,
    },
    upsert_reconcile_lease: metadata.ReconcileLeaseRecord,
    remove_reconcile_lease: struct {},
    upsert_shuffle_join_lease: metadata.ShuffleJoinLeaseRecord,
    remove_shuffle_join_lease: struct {
        job_id: u64,
    },
    upsert_restore_job: struct {
        key: []const u8,
        value: []const u8,
    },
    remove_restore_job: struct {
        key: []const u8,
    },
    remove_restore_jobs: struct {
        keys: []const []const u8,
    },
    upsert_reallocation_request: metadata.ReallocationRequestRecord,
    remove_reallocation_request: struct {
        expected_request_id: u128,
    },
    upsert_extension_package: extension_domain.PackageManifest,
    remove_extension_package: struct {
        name: []const u8,
        version: []const u8,
    },
    upsert_installed_extension: extension_domain.InstalledExtension,
    remove_installed_extension: struct {
        name: []const u8,
    },
    upsert_extension_member: extension_domain.ExtensionMember,
    remove_extension_member: struct {
        extension_name: []const u8,
        object_kind: extension_domain.ExtensionObjectKind,
        object_name: []const u8,
    },
    upsert_extension_dependency: extension_domain.ExtensionDependency,
    remove_extension_dependency: struct {
        extension_name: []const u8,
        required_extension_name: []const u8,
        package_name: []const u8,
    },
    apply_extension_lifecycle: ExtensionLifecycleDelta,

    pub fn deinit(self: *TransitionCommand, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .upsert_node, .register_node => |*record| {
                metadata_table_manager.freeNode(alloc, record.*);
            },
            .upsert_store, .register_store => |*record| {
                metadata_table_manager.freeStore(alloc, record.*);
            },
            .upsert_replica_intent => |*replacement| {
                var record = replacement.replacement.record;
                record.deinit(alloc);
                if (replacement.replacement.peer_node_ids.len > 0) alloc.free(replacement.replacement.peer_node_ids);
            },
            .upsert_table => |*record| {
                metadata_table_manager.freeTable(alloc, record.*);
            },
            .compare_and_replace_table => |*replacement| {
                metadata_table_manager.freeTable(alloc, replacement.expected);
                metadata_table_manager.freeTable(alloc, replacement.replacement);
            },
            .upsert_restore_progress => |*record| {
                metadata_table_manager.freeRestoreProgress(alloc, record.*);
            },
            .upsert_replication_source_status => |*record| {
                metadata_table_manager.freeReplicationSourceStatus(alloc, record.*);
            },
            .complete_replication_source_retirement => |*record| {
                metadata_table_manager.freeReplicationSourceStatus(alloc, record.*);
            },
            .claim_replication_source_cutover => |*claim| {
                alloc.free(@constCast(claim.expected_replication_sources_json));
                metadata_table_manager.freeReplicationSourceStatus(alloc, claim.record);
            },
            .upsert_range => |*record| {
                metadata_table_manager.freeRange(alloc, record.*);
            },
            .complete_restore_range => |*identity| {
                metadata_table_manager.freeRestoreIntentIdentity(alloc, identity.*);
            },
            .upsert_split_transition => |*record| {
                metadata_table_manager.freeSplitTransitionRecord(
                    alloc,
                    record.*,
                );
            },
            .admit_split_transition => |*admission| {
                metadata_table_manager.freeSplitTransitionRecord(
                    alloc,
                    admission.record,
                );
            },
            .upsert_merge_transition => |*record| {
                metadata_table_manager.freeMergeTransitionRecord(
                    alloc,
                    record.*,
                );
            },
            .upsert_extension_package => |*record| record.deinitOwned(alloc),
            .remove_extension_package => |record| {
                alloc.free(record.name);
                alloc.free(record.version);
            },
            .upsert_installed_extension => |*record| record.deinitOwned(alloc),
            .remove_installed_extension => |record| {
                alloc.free(record.name);
            },
            .upsert_extension_member => |*record| record.deinitOwned(alloc),
            .remove_extension_member => |record| {
                alloc.free(record.extension_name);
                alloc.free(record.object_name);
            },
            .upsert_extension_dependency => |*record| record.deinitOwned(alloc),
            .remove_extension_dependency => |record| {
                alloc.free(record.extension_name);
                alloc.free(record.required_extension_name);
                alloc.free(record.package_name);
            },
            .upsert_restore_job => |record| {
                alloc.free(record.key);
                alloc.free(record.value);
            },
            .remove_restore_job => |record| alloc.free(record.key),
            .remove_restore_jobs => |record| {
                for (record.keys) |key| alloc.free(key);
                alloc.free(record.keys);
            },
            .apply_extension_lifecycle => |*delta| freeExtensionLifecycleDelta(alloc, delta.*),
            else => {},
        }
        self.* = undefined;
    }
};

pub fn validateTransitionCommandDataGroupIds(command: TransitionCommand) !void {
    switch (command) {
        .compare_and_replace_table => |replacement| {
            if (replacement.expected.table_id == 0 or
                replacement.replacement.table_id != replacement.expected.table_id or
                !std.mem.eql(u8, replacement.replacement.name, replacement.expected.name))
            {
                return error.InvalidTableDefinitionReplacement;
            }
        },
        .remove_table => |record| {
            if (record.table_id == 0) return error.InvalidTableId;
        },
        .claim_replication_source_cutover => |claim| {
            if (claim.expected_replication_sources_json.len == 0 or
                claim.record.table_id == 0 or
                claim.record.source_kind.len == 0 or
                claim.record.external_table.len == 0 or
                claim.record.slot_name.len == 0 or
                claim.record.publication_name.len == 0 or
                !std.mem.eql(
                    u8,
                    claim.record.cutover_mode,
                    "exported_snapshot_pending",
                ) or
                !std.mem.eql(u8, claim.record.phase, "cutover_preparing") or
                claim.record.snapshot_offset != 0 or
                claim.record.prepared_checkpoint.len != 0 or
                claim.record.stream_checkpoint.len != 0 or
                claim.record.cutover_intent_id == 0 or
                claim.record.cutover_authority_id == 0 or
                std.mem.allEqual(
                    u8,
                    &claim.record.cutover_config_fingerprint,
                    0,
                ) or
                std.mem.allEqual(u8, &claim.record.cutover_provider_identity, 0))
            {
                return error.InvalidReplicationCutoverIntent;
            }
            if (claim.expected_authority_id == 0) {
                if (claim.record.retired_cutover_authority_id != 0 or
                    claim.record.retired_slot_name.len != 0 or
                    claim.record.retired_publication_name.len != 0)
                    return error.InvalidReplicationCutoverIntent;
            } else if (claim.record.cutover_authority_id ==
                claim.expected_authority_id)
            {
                // A cleanup-pending attempt retries under the same authority
                // and continues to carry its predecessor's retirement record.
                if (claim.record.retired_cutover_authority_id == 0 or
                    claim.record.retired_slot_name.len == 0 or
                    claim.record.retired_publication_name.len == 0)
                    return error.InvalidReplicationCutoverIntent;
            } else if (claim.record.retired_cutover_authority_id != claim.expected_authority_id or
                claim.record.retired_slot_name.len == 0 or
                claim.record.retired_publication_name.len == 0)
            {
                return error.InvalidReplicationCutoverIntent;
            }
        },
        .complete_replication_source_retirement => |record| {
            if (record.cutover_authority_id == 0 or
                record.retired_cutover_authority_id == 0 or
                record.retired_slot_name.len == 0 or
                record.retired_publication_name.len == 0)
                return error.InvalidReplicationCutoverIntent;
        },
        .upsert_replica_intent => |replacement| try group_ids.requireDataGroupId(replacement.replacement.record.group_id),
        .remove_replica_intent => |record| try group_ids.requireDataGroupId(record.group_id),
        .upsert_restore_progress => |record| try group_ids.requireDataGroupId(record.group_id),
        .remove_restore_progress => |record| try group_ids.requireDataGroupId(record.group_id),
        .upsert_range => |record| try group_ids.requireDataGroupId(record.group_id),
        .complete_restore_range => |identity| {
            try group_ids.requireDataGroupId(identity.group_id);
            if (identity.table_id == 0) return error.InvalidRestoreIntentIdentity;
            const bootstrap = raft_catalog.BackupRestoreBootstrapRecord{
                .backup_id = identity.backup_id,
                .artifact_backup_id = identity.artifact_backup_id,
                .location = identity.location,
                .snapshot_path = identity.snapshot_path,
                .connection = identity.connection,
                .artifact_size_bytes = identity.artifact_size_bytes,
                .artifact_sha256 = identity.artifact_sha256,
            };
            bootstrap.validate() catch {
                return error.InvalidRestoreIntentIdentity;
            };
        },
        .remove_range => |record| try group_ids.requireDataGroupId(record.group_id),
        .admit_split_transition => |admission| {
            try group_ids.requireDataGroupId(admission.record.source_group_id);
            try group_ids.requireDataGroupId(admission.record.destination_group_id);
            if (admission.record.transition_id == 0 or
                admission.record.attempt_epoch == 0 or
                admission.expected_source_epoch == std.math.maxInt(u64) or
                admission.record.attempt_epoch != admission.expected_source_epoch + 1 or
                admission.record.phase != .prepare or
                admission.record.split_key == null)
            {
                return error.InvalidSplitAdmission;
            }
        },
        .upsert_split_transition => |record| {
            try group_ids.requireDataGroupId(record.source_group_id);
            try group_ids.requireDataGroupId(record.destination_group_id);
        },
        .upsert_merge_transition => |record| {
            try group_ids.requireDataGroupId(record.donor_group_id);
            try group_ids.requireDataGroupId(record.receiver_group_id);
        },
        .upsert_shuffle_join_lease => |record| try group_ids.requireDataGroupId(record.owner_group_id),
        .upsert_restore_job => |record| {
            try validateRestoreJobLogicalKey(record.key);
            if (record.value.len == 0 or record.value.len > max_restore_job_value_bytes) return error.InvalidRestoreJobRecord;
        },
        .remove_restore_job => |record| try validateRestoreJobLogicalKey(record.key),
        .remove_restore_jobs => |record| {
            if (record.keys.len == 0 or record.keys.len > 4096) return error.InvalidRestoreJobRecord;
            for (record.keys) |key| try validateRestoreJobLogicalKey(key);
        },
        else => {},
    }
}

test "metadata raft apply store decodes range values before split attempt epochs" {
    const encoded = try encodeRangeRecord(std.testing.allocator, .{
        .group_id = 901,
        .range_id = 902,
        .table_id = 903,
        .start_key = "a",
        .end_key = "z",
        .doc_identity_shard_id = 904,
        .doc_identity_range_id = 905,
        .split_attempt_epoch = 7,
    });
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(encoded.len >= @sizeOf(u64));

    const appended_fields_size =
        @sizeOf(u64) + // split_attempt_epoch
        @sizeOf(u32) + // empty restore_connection
        @sizeOf(u64) + // restore_artifact_size_bytes
        @sizeOf(u32) + // empty restore_artifact_sha256
        @sizeOf(u32) + // empty restore_artifact_backup_id
        @sizeOf(metadata_table_manager.RestoreCompletionFingerprint);
    const legacy = encoded[0 .. encoded.len - appended_fields_size];
    const decoded = try decodeRangeRecord(std.testing.allocator, legacy);
    defer metadata_table_manager.freeRange(std.testing.allocator, decoded);
    try std.testing.expectEqual(@as(u64, 901), decoded.group_id);
    try std.testing.expectEqual(@as(u64, 902), decoded.range_id);
    try std.testing.expectEqual(@as(u64, 903), decoded.table_id);
    try std.testing.expectEqual(@as(u64, 0), decoded.split_attempt_epoch);
    try std.testing.expectEqualStrings("", decoded.restore_connection);
    try std.testing.expectEqual(@as(u64, 0), decoded.restore_artifact_size_bytes);
    try std.testing.expectEqualStrings("", decoded.restore_artifact_sha256);
    try std.testing.expectEqualSlices(
        u8,
        &metadata_table_manager.empty_restore_completion_fingerprint,
        &decoded.completed_restore_fingerprint,
    );
}

fn validateRestoreJobLogicalKey(key: []const u8) !void {
    if (key.len <= restore_job_logical_prefix.len or key.len > max_restore_job_logical_key_bytes or
        !std.mem.startsWith(u8, key, restore_job_logical_prefix)) return error.InvalidRestoreJobRecord;
}

const test_transition_table_contract: metadata.TransitionTableContract = .{
    .table_id = 7,
    .table_name = "docs",
    .schema_json = "{\"title\":{\"type\":\"keyword\"}}",
    .indexes_json = "{\"full_text\":{\"type\":\"full_text\"}}",
    .source_identity = .{ .shard_id = 70, .range_id = 700 },
    .target_identity = .{ .shard_id = 70, .range_id = 700 },
};

const test_merge_transition_table_contract: metadata.TransitionTableContract = .{
    .table_id = 7,
    .table_name = "docs",
    .schema_json = "{\"title\":{\"type\":\"keyword\"}}",
    .indexes_json = "{\"full_text\":{\"type\":\"full_text\"}}",
    .source_identity = .{ .shard_id = 70, .range_id = 700 },
    .target_identity = .{ .shard_id = 71, .range_id = 701 },
};

fn testTransitionTableRecord() metadata.TableRecord {
    return .{
        .table_id = test_transition_table_contract.table_id,
        .name = test_transition_table_contract.table_name,
        .schema_json = test_transition_table_contract.schema_json,
        .indexes_json = test_transition_table_contract.indexes_json,
    };
}

fn testTransitionRangeRecord(
    group_id: u64,
    identity: transition_state.TransitionIdentity,
    start_key: []const u8,
    end_key: ?[]const u8,
) metadata.RangeRecord {
    return .{
        .group_id = group_id,
        .range_id = identity.range_id,
        .table_id = test_transition_table_contract.table_id,
        .start_key = start_key,
        .end_key = end_key,
        .doc_identity_shard_id = identity.shard_id,
        .doc_identity_range_id = identity.range_id,
    };
}

test "transition command validation rejects metadata group ids in data group fields" {
    const metadata_group_id = group_ids.main_metadata_group_id;
    const commands = [_]TransitionCommand{
        .{ .upsert_replica_intent = .{
            .expected_metadata_version = null,
            .expected_version_fence = 0,
            .expected_target_drain_requested = false,
            .replacement = .{ .record = .{ .group_id = metadata_group_id, .replica_id = 1, .local_node_id = 1 } },
        } },
        .{ .remove_replica_intent = .{ .group_id = metadata_group_id, .local_node_id = 1, .expected_metadata_version = 0 } },
        .{ .upsert_restore_progress = .{ .table_id = 1, .node_id = 1, .group_id = metadata_group_id, .backup_id = "backup" } },
        .{ .remove_restore_progress = .{ .table_id = 1, .node_id = 1, .group_id = metadata_group_id } },
        .{ .upsert_range = .{ .group_id = metadata_group_id, .table_id = 1, .start_key = "" } },
        .{ .remove_range = .{ .group_id = metadata_group_id } },
        .{ .upsert_split_transition = .{ .transition_id = 1, .attempt_epoch = 1, .source_group_id = metadata_group_id, .destination_group_id = 2 } },
        .{ .upsert_split_transition = .{ .transition_id = 1, .attempt_epoch = 1, .source_group_id = 2, .destination_group_id = metadata_group_id } },
        .{ .upsert_merge_transition = .{ .transition_id = 1, .donor_group_id = metadata_group_id, .receiver_group_id = 2 } },
        .{ .upsert_merge_transition = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = metadata_group_id } },
        .{ .upsert_shuffle_join_lease = .{ .job_id = 1, .owner_group_id = metadata_group_id, .expires_at_ms = 1 } },
    };
    for (commands) |command| {
        try std.testing.expectError(error.ReservedGroupId, validateTransitionCommandDataGroupIds(command));
    }
}

test "metadata raft apply store restore job transition encoding is append-only compatible" {
    const encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_restore_job = .{
            .key = "\x00\x00__api_restore_jobs__:000000000000002a",
            .value = "{\"job_id\":42}",
        },
    });
    defer std.testing.allocator.free(encoded);
    var decoded = (try decodeTransitionCommand(std.testing.allocator, encoded)).?;
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expect(decoded == .upsert_restore_job);
    try std.testing.expectEqualStrings("\x00\x00__api_restore_jobs__:000000000000002a", decoded.upsert_restore_job.key);
    try std.testing.expectEqualStrings("{\"job_id\":42}", decoded.upsert_restore_job.value);

    const keys = [_][]const u8{
        "\x00\x00__api_restore_jobs__:000000000000002a",
        "\x00\x00__api_restore_jobs__:000000000000002b",
    };
    const batch_encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_restore_jobs = .{ .keys = &keys },
    });
    defer std.testing.allocator.free(batch_encoded);
    var batch_decoded = (try decodeTransitionCommand(std.testing.allocator, batch_encoded)).?;
    defer batch_decoded.deinit(std.testing.allocator);
    try std.testing.expect(batch_decoded == .remove_restore_jobs);
    try std.testing.expectEqual(@as(usize, 2), batch_decoded.remove_restore_jobs.keys.len);
    try std.testing.expectEqualStrings(keys[0], batch_decoded.remove_restore_jobs.keys[0]);
    try std.testing.expectEqualStrings(keys[1], batch_decoded.remove_restore_jobs.keys[1]);
}

test "metadata transition decoders reject unknown enum values" {
    const unknown_transition = transition_magic ++ [_]u8{0xff};
    try std.testing.expectError(
        error.InvalidMetadataTransitionEncoding,
        decodeTransitionCommand(std.testing.allocator, unknown_transition),
    );

    const bootstrap_mode_offset = @sizeOf(u64) * 4;
    var placement = try encodePlacementIntent(std.testing.allocator, .{
        .record = .{
            .group_id = 7001,
            .replica_id = 1,
            .local_node_id = 2,
        },
    });
    defer std.testing.allocator.free(placement);
    const original_bootstrap_mode = placement[bootstrap_mode_offset];
    placement[bootstrap_mode_offset] = 0xff;
    try std.testing.expectError(
        error.InvalidMetadataTransitionEncoding,
        decodePlacementIntent(std.testing.allocator, placement),
    );
    placement[bootstrap_mode_offset] = original_bootstrap_mode;

    const serving_state_offset = bootstrap_mode_offset + 1 + @sizeOf(u32) + @sizeOf(u64) + 1;
    placement[serving_state_offset] = 0xff;
    try std.testing.expectError(
        error.InvalidMetadataTransitionEncoding,
        decodePlacementIntent(std.testing.allocator, placement),
    );

    var merge = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_merge_transition = .{
            .transition_id = 1,
            .donor_group_id = 7001,
            .receiver_group_id = 7002,
            .table_contract = test_transition_table_contract,
        },
    });
    defer std.testing.allocator.free(merge);
    const merge_phase_offset = transition_magic.len + 1 + @sizeOf(u64) * 3;
    const original_merge_phase = merge[merge_phase_offset];
    merge[merge_phase_offset] = 0xff;
    try std.testing.expectError(
        error.InvalidMetadataTransitionEncoding,
        decodeTransitionCommand(std.testing.allocator, merge),
    );
    merge[merge_phase_offset] = original_merge_phase;

    const merge_reassignment_offset = merge_phase_offset + 2;
    merge[merge_reassignment_offset] = 2;
    try std.testing.expectError(
        error.InvalidMetadataTransitionEncoding,
        decodeTransitionCommand(std.testing.allocator, merge),
    );
}

test "metadata raft apply store replicates restore job records" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-restore-job-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    const group_id = group_ids.main_metadata_group_id;
    const logical_key = "\x00\x00__api_restore_jobs__:000000000000002a";
    const value = "{\"job_id\":42,\"phase\":\"queued\"}";

    const upsert = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_restore_job = .{ .key = logical_key, .value = value },
    });
    defer std.testing.allocator.free(upsert);
    const upsert_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = upsert },
    });
    defer std.testing.allocator.free(upsert_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = group_id,
        .commit_index = 1,
        .entries_bytes = upsert_entries,
    });
    const loaded = (try store.getRestoreJobValue(std.testing.allocator, group_id, logical_key)).?;
    defer std.testing.allocator.free(loaded);
    try std.testing.expectEqualStrings(value, loaded);
    const rows = try store.listRestoreJobRows(std.testing.allocator, group_id);
    defer store.freeRestoreJobRows(std.testing.allocator, rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings(logical_key, rows[0].key);

    const remove = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_restore_job = .{ .key = logical_key },
    });
    defer std.testing.allocator.free(remove);
    const remove_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = remove },
    });
    defer std.testing.allocator.free(remove_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = group_id,
        .commit_index = 2,
        .entries_bytes = remove_entries,
    });
    try std.testing.expect((try store.getRestoreJobValue(std.testing.allocator, group_id, logical_key)) == null);
}

test "metadata raft apply store initializes one durable snapshotted cluster incarnation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-incarnation-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_root);
    const target_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-incarnation-target", .{tmp.sub_path});
    defer std.testing.allocator.free(target_root);
    const group_id = group_ids.main_metadata_group_id;
    const first: metadata_incarnation.MetadataClusterIncarnation = "11111111111111111111111111111111".*;
    const competing: metadata_incarnation.MetadataClusterIncarnation = "22222222222222222222222222222222".*;

    const initialize = try encodeTransitionCommand(std.testing.allocator, .{ .initialize_metadata_incarnation = first });
    defer std.testing.allocator.free(initialize);
    const compete = try encodeTransitionCommand(std.testing.allocator, .{ .initialize_metadata_incarnation = competing });
    defer std.testing.allocator.free(compete);
    const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = initialize },
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = compete },
    });
    defer std.testing.allocator.free(entries);

    var source = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = source_root });
    try source.snapshotBuilder().applyBatch(.{ .group_id = group_id, .commit_index = 2, .entries_bytes = entries });
    try std.testing.expectEqual(first, (try source.getMetadataIncarnation(group_id)).?);
    const snapshot = try source.snapshotBuilder().buildSnapshot(std.testing.allocator, group_id);
    defer std.testing.allocator.free(snapshot);
    source.deinit();

    var reopened = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = source_root });
    defer reopened.deinit();
    try std.testing.expectEqual(first, (try reopened.getMetadataIncarnation(group_id)).?);

    var target = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = target_root });
    defer target.deinit();
    try std.testing.expect(try target.snapshotBuilder().installSnapshot(std.testing.allocator, group_id, 2, snapshot));
    try std.testing.expectEqual(first, (try target.getMetadataIncarnation(group_id)).?);
}

test "metadata raft apply store snapshots and promotes the latest pending reallocation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-reallocation-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_root);
    const target_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-reallocation-target", .{tmp.sub_path});
    defer std.testing.allocator.free(target_root);
    const group_id = group_ids.main_metadata_group_id;
    const incarnation: metadata_incarnation.MetadataClusterIncarnation = "0123456789abcdef0123456789abcdef".*;
    const fingerprint = metadata.reallocation_request.membershipFingerprint(&.{1});
    const active: metadata.ReallocationRequestRecord = .{
        .request_id = 17,
        .requested_at_ms = 17_000,
        .barrier_protocol_version = metadata.reallocation_request.barrier_protocol_version,
        .metadata_incarnation = incarnation,
        .protected_metadata_member_count = 1,
        .protected_metadata_membership_fingerprint = fingerprint,
    };
    const superseded_pending: metadata.ReallocationRequestRecord = .{
        .request_id = 18,
        .requested_at_ms = 18_000,
        .barrier_protocol_version = metadata.reallocation_request.barrier_protocol_version,
        .metadata_incarnation = incarnation,
        .protected_metadata_member_count = 1,
        .protected_metadata_membership_fingerprint = fingerprint,
    };
    const latest_pending: metadata.ReallocationRequestRecord = .{
        .request_id = 19,
        .requested_at_ms = 19_000,
        .barrier_protocol_version = metadata.reallocation_request.barrier_protocol_version,
        .metadata_incarnation = incarnation,
        .protected_metadata_member_count = 1,
        .protected_metadata_membership_fingerprint = fingerprint,
    };

    const active_command = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_reallocation_request = active });
    defer std.testing.allocator.free(active_command);
    const superseded_pending_command = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_reallocation_request = superseded_pending });
    defer std.testing.allocator.free(superseded_pending_command);
    const latest_pending_command = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_reallocation_request = latest_pending });
    defer std.testing.allocator.free(latest_pending_command);
    const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = active_command },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = superseded_pending_command },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = latest_pending_command },
    });
    defer std.testing.allocator.free(entries);

    var source = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = source_root });
    defer source.deinit();
    try source.snapshotBuilder().applyBatch(.{ .group_id = group_id, .commit_index = 3, .entries_bytes = entries });
    try std.testing.expectEqual(active, (try source.getReallocationRequest(group_id)).?);
    const snapshot = try source.snapshotBuilder().buildSnapshot(std.testing.allocator, group_id);
    defer std.testing.allocator.free(snapshot);

    var target = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = target_root });
    defer target.deinit();
    try std.testing.expect(try target.snapshotBuilder().installSnapshot(std.testing.allocator, group_id, 3, snapshot));
    try std.testing.expectEqual(active, (try target.getReallocationRequest(group_id)).?);

    const clear_active = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_reallocation_request = .{ .expected_request_id = active.request_id },
    });
    defer std.testing.allocator.free(clear_active);
    const clear_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = clear_active },
    });
    defer std.testing.allocator.free(clear_entries);
    try target.snapshotBuilder().applyBatch(.{ .group_id = group_id, .commit_index = 4, .entries_bytes = clear_entries });
    try std.testing.expectEqual(latest_pending, (try target.getReallocationRequest(group_id)).?);

    const clear_promoted = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_reallocation_request = .{ .expected_request_id = latest_pending.request_id },
    });
    defer std.testing.allocator.free(clear_promoted);
    const final_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = clear_promoted },
    });
    defer std.testing.allocator.free(final_entries);
    try target.snapshotBuilder().applyBatch(.{ .group_id = group_id, .commit_index = 5, .entries_bytes = final_entries });
    try std.testing.expect((try target.getReallocationRequest(group_id)) == null);
}

test "metadata raft apply store snapshot replaces one complete projection and preserves other groups" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-snapshot-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_root);
    const target_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-snapshot-target", .{tmp.sub_path});
    defer std.testing.allocator.free(target_root);
    const group_id = group_ids.main_metadata_group_id;
    const other_group_id = group_id + 1;
    const retained_key = "\x00\x00__api_restore_jobs__:0000000000000001";
    const stale_key = "\x00\x00__api_restore_jobs__:0000000000000002";
    const other_group_key = "\x00\x00__api_restore_jobs__:0000000000000003";

    const retained_command = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_restore_job = .{ .key = retained_key, .value = "retained" },
    });
    defer std.testing.allocator.free(retained_command);
    const table_command = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = testTransitionTableRecord(),
    });
    defer std.testing.allocator.free(table_command);
    const source_range_command = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            101,
            test_transition_table_contract.source_identity,
            "a",
            "z",
        ),
    });
    defer std.testing.allocator.free(source_range_command);
    const split_command = try encodeTransitionCommand(std.testing.allocator, .{
        .admit_split_transition = .{
            .expected_source_epoch = 0,
            .record = .{
                .transition_id = 71,
                .attempt_epoch = 1,
                .source_group_id = 101,
                .destination_group_id = 102,
                .phase = .prepare,
                .split_key = "m",
                .source_range_end = "z",
                .table_contract = test_transition_table_contract,
            },
        },
    });
    defer std.testing.allocator.free(split_command);
    const source_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = retained_command },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = table_command },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = source_range_command },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = split_command },
    });
    defer std.testing.allocator.free(source_entries);

    var source = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = source_root });
    defer source.deinit();
    try source.snapshotBuilder().applyBatch(.{
        .group_id = group_id,
        .commit_index = 4,
        .entries_bytes = source_entries,
    });
    var prepared = (try source.snapshotBuilder().prepareSnapshot(group_id, 4)) orelse return error.MissingMetadataSnapshotSource;
    defer prepared.deinit();
    var materialized = try prepared.materialize(std.testing.allocator);
    defer materialized.deinit(std.testing.allocator);
    const snapshot = switch (materialized) {
        .bytes => |bytes| bytes,
        .artifact => return error.UnexpectedMetadataSnapshotArtifact,
    };
    const rebuilt_snapshot = try source.snapshotBuilder().buildSnapshot(std.testing.allocator, group_id);
    defer std.testing.allocator.free(rebuilt_snapshot);
    try std.testing.expectEqualSlices(u8, rebuilt_snapshot, snapshot);

    var target = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = target_root });
    defer target.deinit();
    const stale_command = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_restore_job = .{ .key = stale_key, .value = "stale" },
    });
    defer std.testing.allocator.free(stale_command);
    const stale_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = stale_command },
    });
    defer std.testing.allocator.free(stale_entries);
    try target.snapshotBuilder().applyBatch(.{ .group_id = group_id, .commit_index = 1, .entries_bytes = stale_entries });

    const other_group_command = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_restore_job = .{ .key = other_group_key, .value = "other-group" },
    });
    defer std.testing.allocator.free(other_group_command);
    const other_group_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = other_group_command },
    });
    defer std.testing.allocator.free(other_group_entries);
    try target.snapshotBuilder().applyBatch(.{ .group_id = other_group_id, .commit_index = 1, .entries_bytes = other_group_entries });

    try std.testing.expect(try target.snapshotBuilder().installSnapshot(std.testing.allocator, group_id, 4, snapshot));
    const retained = (try target.getRestoreJobValue(std.testing.allocator, group_id, retained_key)).?;
    defer std.testing.allocator.free(retained);
    try std.testing.expectEqualStrings("retained", retained);
    try std.testing.expect((try target.getRestoreJobValue(std.testing.allocator, group_id, stale_key)) == null);
    const other = (try target.getRestoreJobValue(std.testing.allocator, other_group_id, other_group_key)).?;
    defer std.testing.allocator.free(other);
    try std.testing.expectEqualStrings("other-group", other);
    const splits = try target.listSplitTransitions(std.testing.allocator, group_id);
    defer target.freeSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 1), splits.len);
    try std.testing.expectEqual(@as(u64, 71), splits[0].transition_id);
    const batch = (try target.latestBatch(group_id)).?;
    try std.testing.expectEqual(@as(u64, 4), batch.commit_index);
    const installed_snapshot = try target.snapshotBuilder().buildSnapshot(std.testing.allocator, group_id);
    defer std.testing.allocator.free(installed_snapshot);
    try std.testing.expectEqualSlices(u8, snapshot, installed_snapshot);
}

pub const RaftApplyStoreConfig = struct {
    root_dir: []const u8,
    map_size: usize = 16 * 1024 * 1024,
    no_sync: bool = false,
    read_only: bool = false,
    // Metadata apply traffic is many tiny durable WAL-backed writes. Flushing
    // every commit amplifies manifest churn and makes simulation/runtime costs
    // pathological without improving durability.
    flush_threshold: usize = 64,
};

pub const ProjectionSignalKind = enum {
    table,
    range,
    store,
    placement_intent,
    reconcile_lease,
    shuffle_join_lease,
    split_transition,
    merge_transition,
    schema_progress,
    restore_progress,
    restore_job,
    replication_source_status,
};

pub const ProjectionSignal = struct {
    kind: ProjectionSignalKind,
    metadata_group_id: u64,
    table_name: ?[]const u8 = null,
    table_id: u64 = 0,
    group_id: u64 = 0,
    store_id: u64 = 0,
    node_id: u64 = 0,
};

pub const ProjectionListener = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        on_projection_signal: *const fn (ptr: *anyopaque, signal: ProjectionSignal) void,
    };

    pub fn onProjectionSignal(self: ProjectionListener, signal: ProjectionSignal) void {
        self.vtable.on_projection_signal(self.ptr, signal);
    }
};

pub const CommittedKeySignal = struct {
    metadata_group_id: u64,
    key: []const u8,
};

pub const CommittedKeyListener = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        matches_key: *const fn (ptr: *anyopaque, signal: CommittedKeySignal) bool,
        on_committed_key: *const fn (ptr: *anyopaque, signal: CommittedKeySignal) void,
    };

    pub fn onCommittedKey(self: CommittedKeyListener, signal: CommittedKeySignal) void {
        if (!self.vtable.matches_key(self.ptr, signal)) return;
        self.vtable.on_committed_key(self.ptr, signal);
    }
};

pub const CommittedTransitionDelta = union(enum) {
    upsert_split: metadata.SplitTransitionRecord,
    remove_split: u64,
    upsert_merge: metadata.MergeTransitionRecord,
    remove_merge: u64,
};

const OwnedProjectionSignal = struct {
    signal: ProjectionSignal,
    table_name: ?[]u8 = null,

    fn deinit(self: *OwnedProjectionSignal, alloc: std.mem.Allocator) void {
        if (self.table_name) |name| alloc.free(name);
        self.* = undefined;
    }
};

const OwnedCommittedKeySignal = struct {
    metadata_group_id: u64,
    key: []u8,

    fn deinit(self: *OwnedCommittedKeySignal, alloc: std.mem.Allocator) void {
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub const CommittedApplyOutcome = struct {
    alloc: std.mem.Allocator,
    collect_transition_deltas: bool = true,
    projection_signals: std.ArrayListUnmanaged(OwnedProjectionSignal) = .empty,
    committed_keys: std.ArrayListUnmanaged(OwnedCommittedKeySignal) = .empty,
    transition_deltas: std.ArrayListUnmanaged(CommittedTransitionDelta) = .empty,
    failure: ?anyerror = null,

    pub fn deinit(self: *CommittedApplyOutcome) void {
        for (self.projection_signals.items) |*signal| signal.deinit(self.alloc);
        self.projection_signals.deinit(self.alloc);
        for (self.committed_keys.items) |*signal| signal.deinit(self.alloc);
        self.committed_keys.deinit(self.alloc);
        for (self.transition_deltas.items) |*delta| deinitCommittedTransitionDelta(self.alloc, delta);
        self.transition_deltas.deinit(self.alloc);
        self.* = undefined;
    }

    fn appendProjection(self: *CommittedApplyOutcome, signal: ProjectionSignal) !void {
        const table_name = if (signal.table_name) |name| try self.alloc.dupe(u8, name) else null;
        errdefer if (table_name) |name| self.alloc.free(name);
        var owned_signal = signal;
        owned_signal.table_name = table_name;
        try self.projection_signals.append(self.alloc, .{
            .signal = owned_signal,
            .table_name = table_name,
        });
    }

    fn appendCommittedKey(self: *CommittedApplyOutcome, signal: CommittedKeySignal) !void {
        const key = try self.alloc.dupe(u8, signal.key);
        errdefer self.alloc.free(key);
        try self.committed_keys.append(self.alloc, .{
            .metadata_group_id = signal.metadata_group_id,
            .key = key,
        });
    }

    fn appendTransition(self: *CommittedApplyOutcome, delta: CommittedTransitionDelta) !void {
        const owned = try cloneCommittedTransitionDelta(self.alloc, delta);
        errdefer {
            var doomed = owned;
            deinitCommittedTransitionDelta(self.alloc, &doomed);
        }
        try self.transition_deltas.append(self.alloc, owned);
    }

    fn recordFailure(self: *CommittedApplyOutcome, err: anyerror) void {
        if (self.failure == null) self.failure = err;
    }
};

fn cloneCommittedTransitionDelta(alloc: std.mem.Allocator, delta: CommittedTransitionDelta) !CommittedTransitionDelta {
    return switch (delta) {
        .upsert_split => |record| .{ .upsert_split = try cloneCommittedSplitTransition(alloc, record) },
        .remove_split => |transition_id| .{ .remove_split = transition_id },
        .upsert_merge => |record| .{ .upsert_merge = try cloneCommittedMergeTransition(alloc, record) },
        .remove_merge => |transition_id| .{ .remove_merge = transition_id },
    };
}

fn cloneCommittedSplitTransition(alloc: std.mem.Allocator, record: metadata.SplitTransitionRecord) !metadata.SplitTransitionRecord {
    const table_contract = try record.table_contract.clone(alloc);
    var owned = record;
    owned.split_key = null;
    owned.source_range_end = null;
    owned.rollback_reason = null;
    owned.table_contract = table_contract;
    errdefer metadata_table_manager.freeSplitTransitionRecord(alloc, owned);
    if (record.split_key) |value| owned.split_key = try alloc.dupe(u8, value);
    if (record.source_range_end) |value| owned.source_range_end = try alloc.dupe(u8, value);
    if (record.rollback_reason) |value| owned.rollback_reason = try alloc.dupe(u8, value);
    return owned;
}

fn cloneCommittedMergeTransition(alloc: std.mem.Allocator, record: metadata.MergeTransitionRecord) !metadata.MergeTransitionRecord {
    const table_contract = try record.table_contract.clone(alloc);
    var owned = record;
    owned.rollback_reason = null;
    owned.table_contract = table_contract;
    errdefer metadata_table_manager.freeMergeTransitionRecord(alloc, owned);
    if (record.rollback_reason) |value| owned.rollback_reason = try alloc.dupe(u8, value);
    return owned;
}

fn deinitCommittedTransitionDelta(alloc: std.mem.Allocator, delta: *CommittedTransitionDelta) void {
    switch (delta.*) {
        .upsert_split => |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record),
        .upsert_merge => |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record),
        .remove_split, .remove_merge => {},
    }
    delta.* = undefined;
}

pub const RaftApplyStore = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    root_dir: []u8,
    path: []u8,
    backend: lsm_backend.BackendHandle,
    store: docstore.DocStore,
    batches: std.AutoHashMapUnmanaged(u64, OwnedBatch) = .empty,
    projected_placement_intents: std.ArrayListUnmanaged(ProjectedPlacementIntent) = .empty,
    loaded_placement_groups: std.AutoHashMapUnmanaged(u64, void) = .empty,
    projection_listeners: std.ArrayListUnmanaged(ProjectionListener) = .empty,
    committed_key_listeners: std.ArrayListUnmanaged(CommittedKeyListener) = .empty,
    apply_mutex: std.Io.Mutex = .init,
    active_outcome: ?*CommittedApplyOutcome = null,

    const OwnedBatch = struct {
        commit_index: u64,
        entries_bytes: []u8,
    };

    const ProjectedPlacementIntent = struct {
        metadata_group_id: u64,
        intent: raft_reconciler.PlacementIntent,
    };

    pub fn init(alloc: std.mem.Allocator, cfg: RaftApplyStoreConfig) !RaftApplyStore {
        var io_impl = threaded_io_limits.initService(alloc);
        errdefer io_impl.deinit();

        const root_dir = try alloc.dupe(u8, cfg.root_dir);
        errdefer alloc.free(root_dir);
        if (!cfg.read_only) try fs_paths.createDirPathPortable(io_impl.io(), root_dir);

        const path = try std.fmt.allocPrint(alloc, "{s}/metadata-apply-store", .{root_dir});
        errdefer alloc.free(path);
        if (!cfg.read_only) try fs_paths.createDirPathPortable(io_impl.io(), path);

        var backend = try lsm_backend.BackendHandle.open(alloc, path, .{
            .backend = .{
                .durability = if (cfg.no_sync) .none else .full,
                .read_only = cfg.read_only,
                .create_if_missing = !cfg.read_only,
            },
            .flush_threshold = cfg.flush_threshold,
        });
        errdefer backend.close();

        var runtime_store = try backend.backend.runtimeStore(alloc, .{ .name = "metadata-apply" });
        errdefer runtime_store.deinit();

        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .root_dir = root_dir,
            .path = path,
            .backend = backend,
            .store = try docstore.DocStore.openRuntime(alloc, runtime_store),
        };
    }

    pub fn deinit(self: *RaftApplyStore) void {
        var it = self.batches.valueIterator();
        while (it.next()) |batch| self.alloc.free(batch.entries_bytes);
        self.batches.deinit(self.alloc);
        for (self.projected_placement_intents.items) |*entry| freePlacementIntent(self.alloc, entry.intent);
        self.projected_placement_intents.deinit(self.alloc);
        self.loaded_placement_groups.deinit(self.alloc);
        self.projection_listeners.deinit(self.alloc);
        self.committed_key_listeners.deinit(self.alloc);
        self.store.close();
        self.backend.close();
        self.alloc.free(self.path);
        self.alloc.free(self.root_dir);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn snapshotBuilder(self: *RaftApplyStore) raft_state_machine.SnapshotBuilder {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_snapshot = buildSnapshot,
                .prepare_snapshot = prepareSnapshot,
                .install_snapshot = installSnapshotFromRaft,
                .apply_batch = applyBatch,
            },
        };
    }

    pub fn snapshotWriteStats(self: *const RaftApplyStore) lsm_backend.Backend.WriteStats {
        return self.backend.snapshotWriteStats();
    }

    pub fn snapshotMaintenanceStats(self: *const RaftApplyStore) lsm_backend.Backend.MaintenanceStats {
        return self.backend.snapshotMaintenanceStats();
    }

    pub fn latestBatch(self: *RaftApplyStore, group_id: u64) !?AppliedMetadataBatch {
        const batch = (try self.ensureLoaded(group_id)) orelse return null;
        return .{
            .commit_index = batch.commit_index,
            .entries_bytes = batch.entries_bytes,
        };
    }

    pub fn addProjectionListener(self: *RaftApplyStore, listener: ProjectionListener) !void {
        const io = self.io_impl.io();
        self.apply_mutex.lockUncancelable(io);
        defer self.apply_mutex.unlock(io);
        try self.projection_listeners.append(self.alloc, listener);
    }

    pub fn addCommittedKeyListener(self: *RaftApplyStore, listener: CommittedKeyListener) !void {
        const io = self.io_impl.io();
        self.apply_mutex.lockUncancelable(io);
        defer self.apply_mutex.unlock(io);
        try self.committed_key_listeners.append(self.alloc, listener);
    }

    pub fn addLifecycleListeners(
        self: *RaftApplyStore,
        projection_listener: ProjectionListener,
        committed_key_listener: CommittedKeyListener,
    ) !void {
        const io = self.io_impl.io();
        self.apply_mutex.lockUncancelable(io);
        defer self.apply_mutex.unlock(io);

        try self.projection_listeners.ensureUnusedCapacity(self.alloc, 1);
        try self.committed_key_listeners.ensureUnusedCapacity(self.alloc, 1);
        self.projection_listeners.appendAssumeCapacity(projection_listener);
        self.committed_key_listeners.appendAssumeCapacity(committed_key_listener);
    }

    pub fn getMetadataIncarnation(
        self: *RaftApplyStore,
        group_id: u64,
    ) !?metadata_incarnation.MetadataClusterIncarnation {
        var key_buf: [160]u8 = undefined;
        const key = try metadataIncarnationKeyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        if (encoded.len != @sizeOf(metadata_incarnation.MetadataClusterIncarnation)) {
            return error.InvalidMetadataIncarnation;
        }
        var incarnation: metadata_incarnation.MetadataClusterIncarnation = undefined;
        @memcpy(&incarnation, encoded);
        if (!metadata_incarnation.isValid(incarnation)) return error.InvalidMetadataIncarnation;
        return incarnation;
    }

    pub fn listSplitTransitions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SplitTransitionRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try splitTransitionPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.SplitTransitionRecord, kvs.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |record|
                metadata_table_manager.freeSplitTransitionRecord(alloc, record);
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| {
            out[i] = try decodeSplitTransitionRecord(alloc, kv.value);
            initialized += 1;
        }
        return out;
    }

    pub fn listPlacementIntents(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]raft_reconciler.PlacementIntent {
        const io = self.io_impl.io();
        self.apply_mutex.lockUncancelable(io);
        defer self.apply_mutex.unlock(io);

        try self.ensurePlacementIntentsLoaded(alloc, group_id);

        var count: usize = 0;
        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id == group_id) count += 1;
        }

        const out = try alloc.alloc(raft_reconciler.PlacementIntent, count);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |intent| freePlacementIntent(alloc, intent);
            alloc.free(out);
        }

        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id != group_id) continue;
            out[initialized] = try clonePlacementIntent(alloc, entry.intent);
            initialized += 1;
        }
        return out;
    }

    pub fn listPlacementVersionFences(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
    ) ![]metadata_reconciler.PlacementVersionFence {
        var prefix_buf: [160]u8 = undefined;
        const prefix = try placementVersionPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata_reconciler.PlacementVersionFence, kvs.len);
        errdefer alloc.free(out);
        for (kvs, 0..) |kv, i| {
            if (kv.key.len <= prefix.len or kv.value.len != @sizeOf(u64))
                return error.InvalidPlacementVersionFence;
            var parts = std.mem.splitScalar(u8, kv.key[prefix.len..], ':');
            const range_group_id = try std.fmt.parseInt(
                u64,
                parts.next() orelse return error.InvalidPlacementVersionFence,
                10,
            );
            const local_node_id = try std.fmt.parseInt(
                u64,
                parts.next() orelse return error.InvalidPlacementVersionFence,
                10,
            );
            if (parts.next() != null) return error.InvalidPlacementVersionFence;
            out[i] = .{
                .group_id = range_group_id,
                .local_node_id = local_node_id,
                .version = std.mem.readInt(u64, kv.value[0..@sizeOf(u64)], .little),
            };
        }
        return out;
    }

    pub fn listLocalPlacementIntents(self: *RaftApplyStore, alloc: std.mem.Allocator, metadata_group_id: u64, local_node_id: u64) ![]raft_reconciler.PlacementIntent {
        const io = self.io_impl.io();
        self.apply_mutex.lockUncancelable(io);
        defer self.apply_mutex.unlock(io);

        try self.ensurePlacementIntentsLoaded(alloc, metadata_group_id);

        var count: usize = 0;
        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.local_node_id != local_node_id) continue;
            count += 1;
        }

        const out = try alloc.alloc(raft_reconciler.PlacementIntent, count);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |intent| freePlacementIntent(alloc, intent);
            alloc.free(out);
        }

        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.local_node_id != local_node_id) continue;
            out[initialized] = try clonePlacementIntent(alloc, entry.intent);
            initialized += 1;
        }
        return out;
    }

    pub fn freePlacementIntents(_: *RaftApplyStore, alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
        for (intents) |intent| {
            freePlacementIntent(alloc, intent);
        }
        alloc.free(intents);
    }

    fn ensurePlacementIntentsLoaded(self: *RaftApplyStore, alloc: std.mem.Allocator, metadata_group_id: u64) !void {
        if (self.loaded_placement_groups.contains(metadata_group_id)) return;

        var prefix_buf: [128]u8 = undefined;
        const prefix = try placementPrefixForGroup(&prefix_buf, metadata_group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        for (kvs) |kv| {
            const intent = try decodePlacementIntent(alloc, kv.value);
            defer freePlacementIntent(alloc, intent);
            try self.upsertProjectedPlacementIntent(metadata_group_id, intent);
        }
        try self.loaded_placement_groups.put(self.alloc, metadata_group_id, {});
    }

    fn upsertProjectedPlacementIntent(self: *RaftApplyStore, metadata_group_id: u64, intent: raft_reconciler.PlacementIntent) !void {
        for (self.projected_placement_intents.items) |*entry| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.group_id != intent.record.group_id) continue;
            if (entry.intent.record.local_node_id != intent.record.local_node_id) continue;
            freePlacementIntent(self.alloc, entry.intent);
            entry.* = .{
                .metadata_group_id = metadata_group_id,
                .intent = try clonePlacementIntent(self.alloc, intent),
            };
            return;
        }
        try self.projected_placement_intents.append(self.alloc, .{
            .metadata_group_id = metadata_group_id,
            .intent = try clonePlacementIntent(self.alloc, intent),
        });
    }

    fn removeProjectedPlacementIntent(self: *RaftApplyStore, metadata_group_id: u64, range_group_id: u64, local_node_id: u64) void {
        for (self.projected_placement_intents.items, 0..) |entry, i| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.group_id != range_group_id) continue;
            if (entry.intent.record.local_node_id != local_node_id) continue;
            freePlacementIntent(self.alloc, entry.intent);
            _ = self.projected_placement_intents.swapRemove(i);
            return;
        }
    }

    pub fn listNodes(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.NodeRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try nodePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.NodeRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeNode(alloc, record);
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| out[i] = try decodeNodeRecord(alloc, kv.value);
        return out;
    }

    pub fn freeNodes(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.NodeRecord) void {
        for (records) |record| metadata_table_manager.freeNode(alloc, record);
        alloc.free(records);
    }

    pub fn listStores(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.StoreRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try storePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.StoreRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeStore(alloc, record);
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| out[i] = try decodeStoreRecord(alloc, kv.value);
        return out;
    }

    pub fn freeStores(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.StoreRecord) void {
        for (records) |record| metadata_table_manager.freeStore(alloc, record);
        alloc.free(records);
    }

    pub fn listMergeTransitions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.MergeTransitionRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try mergeTransitionPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.MergeTransitionRecord, kvs.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |record|
                metadata_table_manager.freeMergeTransitionRecord(alloc, record);
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| {
            out[i] = try decodeMergeTransitionRecord(alloc, kv.value);
            initialized += 1;
        }
        return out;
    }

    pub fn listTables(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.TableRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try tablePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.TableRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| {
                metadata_table_manager.freeTable(alloc, record);
            }
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeTableRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeTables(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.TableRecord) void {
        for (records) |record| {
            metadata_table_manager.freeTable(alloc, record);
        }
        alloc.free(records);
    }

    pub fn getTable(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_id: u64,
    ) !?metadata.TableRecord {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, table_id);
        const encoded = self.store.get(alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(encoded);
        return try decodeTableRecord(alloc, encoded);
    }

    pub fn getTableTransitionFence(
        self: *RaftApplyStore,
        group_id: u64,
        table_id: u64,
    ) !TableTransitionFence {
        var key_buf: [192]u8 = undefined;
        const key = try tableTransitionFenceKeyForGroup(&key_buf, group_id, table_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return .{},
            else => return err,
        };
        defer self.alloc.free(encoded);
        return try decodeTableTransitionFence(encoded);
    }

    pub fn listSchemaProgress(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SchemaProgressRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try schemaProgressPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.SchemaProgressRecord, kvs.len);
        errdefer alloc.free(out);
        for (kvs, 0..) |kv, i| out[i] = try decodeSchemaProgressRecord(kv.value);
        return out;
    }

    pub fn freeSchemaProgress(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.SchemaProgressRecord) void {
        alloc.free(records);
    }

    pub fn listRestoreProgress(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.RestoreProgressRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try restoreProgressPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.RestoreProgressRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeRestoreProgressRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeRestoreProgress(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.RestoreProgressRecord) void {
        for (records) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
        alloc.free(records);
    }

    pub fn listReplicationSourceStatuses(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.ReplicationSourceStatusRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try replicationSourceStatusPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.ReplicationSourceStatusRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| metadata_table_manager.freeReplicationSourceStatus(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeReplicationSourceStatusRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeReplicationSourceStatuses(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.ReplicationSourceStatusRecord) void {
        for (records) |record| metadata_table_manager.freeReplicationSourceStatus(alloc, record);
        alloc.free(records);
    }

    pub fn getReplicationSourceStatus(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_id: u64,
        source_ordinal: u32,
    ) !?metadata.ReplicationSourceStatusRecord {
        var key_buf: [192]u8 = undefined;
        const key = try replicationSourceStatusKeyForGroup(
            &key_buf,
            group_id,
            table_id,
            source_ordinal,
        );
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        return try decodeReplicationSourceStatusRecord(alloc, encoded);
    }

    pub fn listExtensionPackages(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.PackageManifest {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try extensionPackagePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer freeKvs(alloc, kvs);

        const out = try alloc.alloc(extension_domain.PackageManifest, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*record| record.deinitOwned(alloc);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeExtensionPackageRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeExtensionPackages(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []extension_domain.PackageManifest) void {
        for (records) |*record| record.deinitOwned(alloc);
        alloc.free(records);
    }

    pub fn listInstalledExtensions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.InstalledExtension {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try installedExtensionPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer freeKvs(alloc, kvs);

        const out = try alloc.alloc(extension_domain.InstalledExtension, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*record| record.deinitOwned(alloc);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeInstalledExtensionRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeInstalledExtensions(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []extension_domain.InstalledExtension) void {
        for (records) |*record| record.deinitOwned(alloc);
        alloc.free(records);
    }

    pub fn listExtensionMembers(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.ExtensionMember {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try extensionMemberPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer freeKvs(alloc, kvs);

        const out = try alloc.alloc(extension_domain.ExtensionMember, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*record| record.deinitOwned(alloc);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeExtensionMemberRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeExtensionMembers(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []extension_domain.ExtensionMember) void {
        for (records) |*record| record.deinitOwned(alloc);
        alloc.free(records);
    }

    pub fn listExtensionDependencies(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.ExtensionDependency {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try extensionDependencyPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer freeKvs(alloc, kvs);

        const out = try alloc.alloc(extension_domain.ExtensionDependency, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*record| record.deinitOwned(alloc);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeExtensionDependencyRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeExtensionDependencies(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []extension_domain.ExtensionDependency) void {
        for (records) |*record| record.deinitOwned(alloc);
        alloc.free(records);
    }

    pub fn listShuffleJoinLeases(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.ShuffleJoinLeaseRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try shuffleJoinLeasePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.ShuffleJoinLeaseRecord, kvs.len);
        errdefer alloc.free(out);
        for (kvs, 0..) |kv, i| {
            var pos: usize = 0;
            out[i] = try readShuffleJoinLeaseRecord(kv.value, &pos);
        }
        return out;
    }

    pub fn freeShuffleJoinLeases(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.ShuffleJoinLeaseRecord) void {
        alloc.free(records);
    }

    pub fn listRanges(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.RangeRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try rangePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.RangeRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeRange(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| out[i] = try decodeRangeRecord(alloc, kv.value);
        return out;
    }

    pub fn freeRanges(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.RangeRecord) void {
        for (records) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(records);
    }

    fn freeKvs(alloc: std.mem.Allocator, kvs: anytype) void {
        for (kvs) |kv| {
            alloc.free(kv.key);
            alloc.free(kv.value);
        }
        alloc.free(kvs);
    }

    pub fn freeSplitTransitions(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.SplitTransitionRecord) void {
        for (records) |record|
            metadata_table_manager.freeSplitTransitionRecord(alloc, record);
        alloc.free(records);
    }

    pub fn freeMergeTransitions(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.MergeTransitionRecord) void {
        for (records) |record|
            metadata_table_manager.freeMergeTransitionRecord(alloc, record);
        alloc.free(records);
    }

    pub fn getReconcileLease(self: *RaftApplyStore, group_id: u64) !?metadata.ReconcileLeaseRecord {
        var key_buf: [160]u8 = undefined;
        const key = try reconcileLeaseKeyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        var pos: usize = 0;
        return try readReconcileLeaseRecord(encoded, &pos);
    }

    pub fn getReallocationRequest(self: *RaftApplyStore, group_id: u64) !?metadata.ReallocationRequestRecord {
        var key_buf: [160]u8 = undefined;
        const key = try reallocationRequestKeyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        var pos: usize = 0;
        return try readReallocationRequestRecord(encoded, &pos);
    }

    pub fn getShuffleJoinLease(self: *RaftApplyStore, group_id: u64, job_id: u64) !?metadata.ShuffleJoinLeaseRecord {
        var key_buf: [192]u8 = undefined;
        const key = try shuffleJoinLeaseKeyForGroup(&key_buf, group_id, job_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        var pos: usize = 0;
        return try readShuffleJoinLeaseRecord(encoded, &pos);
    }

    pub const RestoreJobRow = struct {
        key: []u8,
        value: []u8,
    };

    pub fn listRestoreJobRows(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]RestoreJobRow {
        var prefix_buf: [192]u8 = undefined;
        const prefix = try restoreJobPrefixForGroup(&prefix_buf, group_id);
        var rows = std.ArrayListUnmanaged(RestoreJobRow).empty;
        errdefer {
            for (rows.items) |row| {
                alloc.free(row.key);
                alloc.free(row.value);
            }
            rows.deinit(alloc);
        }
        var after_key: ?[]u8 = null;
        defer if (after_key) |key| alloc.free(key);
        while (true) {
            const page = try self.store.scanPrefixPage(alloc, prefix, after_key, 512);
            defer docstore.DocStore.freeResults(alloc, page);
            if (page.len == 0) break;
            for (page) |row| try rows.append(alloc, .{
                .key = try alloc.dupe(u8, row.key[prefix.len..]),
                .value = try alloc.dupe(u8, row.value),
            });
            if (page.len < 512) break;
            const next = try alloc.dupe(u8, page[page.len - 1].key);
            if (after_key) |key| alloc.free(key);
            after_key = next;
        }
        return try rows.toOwnedSlice(alloc);
    }

    pub fn getRestoreJobValue(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, logical_key: []const u8) !?[]u8 {
        var key_buf: [256]u8 = undefined;
        const key = try restoreJobKeyForGroup(&key_buf, group_id, logical_key);
        return self.store.get(alloc, key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
    }

    pub fn freeRestoreJobRows(_: *RaftApplyStore, alloc: std.mem.Allocator, rows: []RestoreJobRow) void {
        for (rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }

    const metadata_snapshot_magic = "AFMS";
    const metadata_snapshot_version: u8 = 1;

    const MetadataSnapshotKeyFn = *const fn ([]u8, u64) anyerror![]const u8;
    const MetadataSnapshotProjection = enum {
        metadata_incarnation,
        split_transition,
        merge_transition,
        placement,
        placement_version,
        node,
        store,
        table,
        table_transition_fence,
        schema_progress,
        restore_progress,
        replication_source_status,
        extension_package,
        installed_extension,
        extension_member,
        extension_dependency,
        shuffle_join_lease,
        restore_job,
        range,
        reconcile_lease,
        reallocation_request,
        reallocation_request_pending,
    };
    const MetadataSnapshotKey = union(enum) {
        prefix: MetadataSnapshotKeyFn,
        point: MetadataSnapshotKeyFn,
    };
    const MetadataSnapshotProjectionDescriptor = struct {
        projection: MetadataSnapshotProjection,
        key: MetadataSnapshotKey,
    };
    const metadata_snapshot_projections = [_]MetadataSnapshotProjectionDescriptor{
        .{ .projection = .metadata_incarnation, .key = .{ .point = metadataIncarnationKeyForGroup } },
        .{ .projection = .split_transition, .key = .{ .prefix = splitTransitionPrefixForGroup } },
        .{ .projection = .merge_transition, .key = .{ .prefix = mergeTransitionPrefixForGroup } },
        .{ .projection = .placement, .key = .{ .prefix = placementPrefixForGroup } },
        .{ .projection = .placement_version, .key = .{ .prefix = placementVersionPrefixForGroup } },
        .{ .projection = .node, .key = .{ .prefix = nodePrefixForGroup } },
        .{ .projection = .store, .key = .{ .prefix = storePrefixForGroup } },
        .{ .projection = .table, .key = .{ .prefix = tablePrefixForGroup } },
        .{ .projection = .table_transition_fence, .key = .{ .prefix = tableTransitionFencePrefixForGroup } },
        .{ .projection = .schema_progress, .key = .{ .prefix = schemaProgressPrefixForGroup } },
        .{ .projection = .restore_progress, .key = .{ .prefix = restoreProgressPrefixForGroup } },
        .{ .projection = .replication_source_status, .key = .{ .prefix = replicationSourceStatusPrefixForGroup } },
        .{ .projection = .extension_package, .key = .{ .prefix = extensionPackagePrefixForGroup } },
        .{ .projection = .installed_extension, .key = .{ .prefix = installedExtensionPrefixForGroup } },
        .{ .projection = .extension_member, .key = .{ .prefix = extensionMemberPrefixForGroup } },
        .{ .projection = .extension_dependency, .key = .{ .prefix = extensionDependencyPrefixForGroup } },
        .{ .projection = .shuffle_join_lease, .key = .{ .prefix = shuffleJoinLeasePrefixForGroup } },
        .{ .projection = .restore_job, .key = .{ .prefix = restoreJobPrefixForGroup } },
        .{ .projection = .range, .key = .{ .prefix = rangePrefixForGroup } },
        .{ .projection = .reconcile_lease, .key = .{ .point = reconcileLeaseKeyForGroup } },
        .{ .projection = .reallocation_request, .key = .{ .point = reallocationRequestKeyForGroup } },
        .{ .projection = .reallocation_request_pending, .key = .{ .point = pendingReallocationRequestKeyForGroup } },
    };

    fn metadataSnapshotProjectionBit(projection: MetadataSnapshotProjection) u32 {
        return @as(u32, 1) << @intFromEnum(projection);
    }

    /// Exhaustively ties every durable transition command to the projection
    /// classes that must survive a metadata Raft snapshot. Adding a command
    /// without classifying its durable output is therefore a compile error.
    fn transitionCommandProjectionMask(tag: std.meta.Tag(TransitionCommand)) u32 {
        return switch (tag) {
            .initialize_metadata_incarnation => metadataSnapshotProjectionBit(.metadata_incarnation),
            .upsert_node, .register_node, .remove_node => metadataSnapshotProjectionBit(.node),
            .request_node_shutdown, .cancel_node_shutdown, .finalize_node_shutdown => metadataSnapshotProjectionBit(.node) | metadataSnapshotProjectionBit(.store),
            .upsert_store, .register_store, .remove_store => metadataSnapshotProjectionBit(.store),
            .upsert_replica_intent, .remove_replica_intent => metadataSnapshotProjectionBit(.placement) |
                metadataSnapshotProjectionBit(.placement_version),
            .upsert_table, .compare_and_replace_table, .remove_table => metadataSnapshotProjectionBit(.table),
            .upsert_schema_progress, .remove_schema_progress => metadataSnapshotProjectionBit(.schema_progress),
            .upsert_restore_progress, .remove_restore_progress => metadataSnapshotProjectionBit(.restore_progress),
            .upsert_replication_source_status, .claim_replication_source_cutover, .complete_replication_source_retirement => metadataSnapshotProjectionBit(.replication_source_status),
            .upsert_range, .complete_restore_range, .remove_range => metadataSnapshotProjectionBit(.range),
            .admit_split_transition => metadataSnapshotProjectionBit(.split_transition) |
                metadataSnapshotProjectionBit(.range) |
                metadataSnapshotProjectionBit(.table_transition_fence),
            .upsert_split_transition, .remove_split_transition => metadataSnapshotProjectionBit(.split_transition) |
                metadataSnapshotProjectionBit(.table_transition_fence),
            .upsert_merge_transition, .remove_merge_transition => metadataSnapshotProjectionBit(.merge_transition) |
                metadataSnapshotProjectionBit(.table_transition_fence),
            .upsert_reconcile_lease, .remove_reconcile_lease => metadataSnapshotProjectionBit(.reconcile_lease),
            .upsert_shuffle_join_lease, .remove_shuffle_join_lease => metadataSnapshotProjectionBit(.shuffle_join_lease),
            .upsert_restore_job, .remove_restore_job, .remove_restore_jobs => metadataSnapshotProjectionBit(.restore_job),
            .upsert_reallocation_request, .remove_reallocation_request => metadataSnapshotProjectionBit(.reallocation_request) |
                metadataSnapshotProjectionBit(.reallocation_request_pending),
            .upsert_extension_package, .remove_extension_package => metadataSnapshotProjectionBit(.extension_package),
            .upsert_installed_extension, .remove_installed_extension => metadataSnapshotProjectionBit(.installed_extension),
            .upsert_extension_member, .remove_extension_member => metadataSnapshotProjectionBit(.extension_member),
            .upsert_extension_dependency, .remove_extension_dependency => metadataSnapshotProjectionBit(.extension_dependency),
            .apply_extension_lifecycle => metadataSnapshotProjectionBit(.table) |
                metadataSnapshotProjectionBit(.table_transition_fence) |
                metadataSnapshotProjectionBit(.installed_extension) |
                metadataSnapshotProjectionBit(.extension_member) |
                metadataSnapshotProjectionBit(.extension_dependency),
        };
    }

    const PreparedSnapshot = struct {
        owner: *RaftApplyStore,
        txn: docstore.DocStore.Txn,
        group_id: u64,
        cancelled: std.atomic.Value(bool) = .init(false),

        fn source(self: *@This()) raft_engine.runtime.storage_iface.SnapshotSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .materialize = materialize,
                    .cancel = cancel,
                    .deinit = PreparedSnapshot.deinit,
                },
            };
        }

        fn materialize(ptr: *anyopaque, alloc: std.mem.Allocator) !raft_engine.runtime.storage_iface.SnapshotMaterialization {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.cancelled.load(.acquire)) return error.SnapshotBuildCancelled;
            return .{ .bytes = try self.owner.buildMetadataSnapshotTxn(alloc, &self.txn, self.group_id, &self.cancelled) };
        }

        fn cancel(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cancelled.store(true, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.txn.abort();
            std.heap.page_allocator.destroy(self);
        }
    };

    fn buildSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        var txn = try self.store.beginReadTxn();
        defer txn.abort();
        return try self.buildMetadataSnapshotTxn(alloc, &txn, group_id, null);
    }

    fn prepareSnapshot(ptr: *anyopaque, group_id: u64, applied_index: u64) !?raft_engine.runtime.storage_iface.SnapshotSource {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        var txn = try self.store.beginReadTxn();
        errdefer txn.abort();
        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const value = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        if (value.len < @sizeOf(u64)) return error.InvalidMetadataApplyBatch;
        if (std.mem.readInt(u64, value[0..8], .little) != applied_index) return error.AppliedSnapshotIndexMismatch;

        const prepared = try std.heap.page_allocator.create(PreparedSnapshot);
        prepared.* = .{
            .owner = self,
            .txn = txn,
            .group_id = group_id,
        };
        return prepared.source();
    }

    fn installSnapshotFromRaft(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        commit_index: u64,
        encoded: []const u8,
    ) !void {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        const rows = try decodeMetadataSnapshotAlloc(alloc, encoded);
        defer freeMetadataSnapshotRows(alloc, rows);
        try validateMetadataSnapshotRows(alloc, group_id, rows);

        const empty_entries = try raft_state_machine.encodeCommittedEntries(alloc, &.{});
        defer alloc.free(empty_entries);
        const owned_entries = try self.alloc.dupe(u8, empty_entries);
        errdefer self.alloc.free(owned_entries);
        const io = self.io_impl.io();
        self.apply_mutex.lockUncancelable(io);
        defer self.apply_mutex.unlock(io);
        try self.batches.ensureUnusedCapacity(self.alloc, 1);

        const existing = blk: {
            var read_txn = try self.store.beginReadTxn();
            defer read_txn.abort();
            break :blk try self.collectMetadataSnapshotRowsTxn(alloc, &read_txn, group_id, null);
        };
        defer freeMetadataSnapshotRows(alloc, existing);

        const watermark = try alloc.alloc(u8, @sizeOf(u64) + empty_entries.len);
        defer alloc.free(watermark);
        std.mem.writeInt(u64, watermark[0..8], commit_index, .little);
        @memcpy(watermark[8..], empty_entries);
        var watermark_key_buf: [128]u8 = undefined;
        const watermark_key = try keyForGroup(&watermark_key_buf, group_id);

        const writes = try alloc.alloc(docstore.KVPair, rows.len + 1);
        defer alloc.free(writes);
        for (rows, 0..) |row, i| writes[i] = .{ .key = row.key, .value = row.value };
        writes[rows.len] = .{ .key = watermark_key, .value = watermark };
        const deletes = try alloc.alloc([]const u8, existing.len);
        defer alloc.free(deletes);
        for (existing, 0..) |row, i| deletes[i] = row.key;
        try self.store.putBatch(writes, deletes);

        if (self.batches.getPtr(group_id)) |batch| {
            self.alloc.free(batch.entries_bytes);
            batch.* = .{ .commit_index = commit_index, .entries_bytes = owned_entries };
        } else {
            self.batches.putAssumeCapacity(group_id, .{ .commit_index = commit_index, .entries_bytes = owned_entries });
        }
        self.invalidateProjectedPlacementGroup(group_id);
        self.notifyMetadataSnapshotInstalled(group_id);
        for (existing) |row| self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = row.key });
        for (rows) |row| self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = row.key });
    }

    fn buildMetadataSnapshotTxn(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        cancelled: ?*const std.atomic.Value(bool),
    ) ![]u8 {
        const rows = try self.collectMetadataSnapshotRowsTxn(alloc, txn, group_id, cancelled);
        defer freeMetadataSnapshotRows(alloc, rows);
        return try encodeMetadataSnapshot(alloc, rows);
    }

    fn collectMetadataSnapshotRowsTxn(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        cancelled: ?*const std.atomic.Value(bool),
    ) ![]docstore.OwnedKVPair {
        _ = self;
        var rows = std.ArrayListUnmanaged(docstore.OwnedKVPair).empty;
        errdefer {
            for (rows.items) |row| {
                alloc.free(row.key);
                alloc.free(row.value);
            }
            rows.deinit(alloc);
        }
        var buf: [256]u8 = undefined;
        for (metadata_snapshot_projections) |descriptor| {
            if (cancelled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
            switch (descriptor.key) {
                .prefix => |key_fn| try appendMetadataPrefixRowsTxn(alloc, txn, &rows, try key_fn(&buf, group_id)),
                .point => |key_fn| try appendMetadataPointRowTxn(alloc, txn, &rows, try key_fn(&buf, group_id)),
            }
        }
        std.sort.pdq(docstore.OwnedKVPair, rows.items, {}, metadataSnapshotRowLessThan);
        return try rows.toOwnedSlice(alloc);
    }

    fn invalidateProjectedPlacementGroup(self: *RaftApplyStore, group_id: u64) void {
        var i = self.projected_placement_intents.items.len;
        while (i > 0) {
            i -= 1;
            if (self.projected_placement_intents.items[i].metadata_group_id != group_id) continue;
            const removed = self.projected_placement_intents.swapRemove(i);
            freePlacementIntent(self.alloc, removed.intent);
        }
        _ = self.loaded_placement_groups.remove(group_id);
    }

    fn notifyMetadataSnapshotInstalled(self: *RaftApplyStore, group_id: u64) void {
        const kinds = [_]ProjectionSignalKind{
            .table,
            .range,
            .store,
            .placement_intent,
            .reconcile_lease,
            .shuffle_join_lease,
            .split_transition,
            .merge_transition,
            .schema_progress,
            .restore_progress,
            .restore_job,
            .replication_source_status,
        };
        for (kinds) |kind| self.notifyProjectionListeners(.{
            .kind = kind,
            .metadata_group_id = group_id,
        });
    }

    fn applyBatch(ptr: *anyopaque, batch: raft_state_machine.ApplyBatch) !void {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        try self.writeBatch(batch.group_id, batch.commit_index, batch.entries_bytes);
    }

    fn writeBatch(self: *RaftApplyStore, group_id: u64, commit_index: u64, entries_bytes: []const u8) !void {
        var outcome = try self.applyCommittedBatchInternal(group_id, commit_index, entries_bytes, false);
        defer outcome.deinit();
    }

    /// Applies one committed Raft batch and returns only projection changes
    /// that actually mutated durable state. Listener signals are dispatched
    /// after commit and before this method returns.
    pub fn applyCommittedBatch(
        self: *RaftApplyStore,
        group_id: u64,
        commit_index: u64,
        entries_bytes: []const u8,
    ) !CommittedApplyOutcome {
        return try self.applyCommittedBatchInternal(group_id, commit_index, entries_bytes, true);
    }

    fn applyCommittedBatchInternal(
        self: *RaftApplyStore,
        group_id: u64,
        commit_index: u64,
        entries_bytes: []const u8,
        collect_transition_deltas: bool,
    ) !CommittedApplyOutcome {
        var value = try self.alloc.alloc(u8, @sizeOf(u64) + entries_bytes.len);
        defer self.alloc.free(value);
        std.mem.writeInt(u64, value[0..8], commit_index, .little);
        @memcpy(value[8..], entries_bytes);

        const owned_entries = try self.alloc.dupe(u8, entries_bytes);
        errdefer self.alloc.free(owned_entries);

        const io = self.io_impl.io();
        self.apply_mutex.lockUncancelable(io);
        var apply_locked = true;
        defer if (apply_locked) self.apply_mutex.unlock(io);
        try self.batches.ensureUnusedCapacity(self.alloc, 1);
        std.debug.assert(self.active_outcome == null);
        var outcome = CommittedApplyOutcome{
            .alloc = self.alloc,
            .collect_transition_deltas = collect_transition_deltas,
        };
        errdefer outcome.deinit();
        self.active_outcome = &outcome;
        defer self.active_outcome = null;

        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        var txn = try self.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(key, value);
        try self.projectEntriesTxn(&txn, group_id, entries_bytes);
        if (outcome.failure) |err| return err;
        try txn.commit();

        if (self.batches.getPtr(group_id)) |existing| {
            self.alloc.free(existing.entries_bytes);
            existing.* = .{
                .commit_index = commit_index,
                .entries_bytes = owned_entries,
            };
        } else {
            self.batches.putAssumeCapacity(group_id, .{
                .commit_index = commit_index,
                .entries_bytes = owned_entries,
            });
        }
        self.active_outcome = null;
        self.dispatchCommittedOutcome(&outcome);
        self.apply_mutex.unlock(io);
        apply_locked = false;
        return outcome;
    }

    fn ensureLoaded(self: *RaftApplyStore, group_id: u64) !?*OwnedBatch {
        if (self.batches.getPtr(group_id)) |batch| return batch;

        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        if (encoded.len < @sizeOf(u64)) return error.InvalidMetadataApplyBatch;

        const commit_index = std.mem.readInt(u64, encoded[0..8], .little);
        const owned_entries = try self.alloc.dupe(u8, encoded[8..]);
        errdefer self.alloc.free(owned_entries);
        try self.batches.put(self.alloc, group_id, .{
            .commit_index = commit_index,
            .entries_bytes = owned_entries,
        });
        return self.batches.getPtr(group_id);
    }

    fn keyForGroup(buf: []u8, group_id: u64) ![]const u8 {
        return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_raft_apply:{d}", .{group_id});
    }

    fn projectEntriesTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, entries_bytes: []const u8) !void {
        const decoded = raft_state_machine.decodeCommittedEntries(self.alloc, entries_bytes) catch |err| switch (err) {
            error.InvalidCommittedEntriesEncoding => return,
            else => return err,
        };
        defer self.alloc.free(decoded);

        for (decoded) |entry| {
            if (entry.entry_type != .normal) continue;
            var command = (try decodeTransitionCommand(self.alloc, entry.data)) orelse continue;
            defer command.deinit(self.alloc);
            try self.applyTransitionCommandTxn(txn, group_id, command);
        }
    }

    fn applyTransitionCommandTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, command: TransitionCommand) !void {
        try validateTransitionCommandDataGroupIds(command);
        switch (command) {
            .initialize_metadata_incarnation => |incarnation| {
                if (!metadata_incarnation.isValid(incarnation)) return error.InvalidMetadataIncarnation;
                var key_buf: [160]u8 = undefined;
                const key = try metadataIncarnationKeyForGroup(&key_buf, group_id);
                const existing = txn.get(key) catch |err| switch (err) {
                    error.NotFound => {
                        try txn.put(key, &incarnation);
                        return;
                    },
                    else => return err,
                };
                if (existing.len != @sizeOf(metadata_incarnation.MetadataClusterIncarnation)) {
                    return error.InvalidMetadataIncarnation;
                }
                var committed: metadata_incarnation.MetadataClusterIncarnation = undefined;
                @memcpy(&committed, existing);
                if (!metadata_incarnation.isValid(committed)) return error.InvalidMetadataIncarnation;
                // Initialization is first-writer-wins. A proposal accepted by a
                // superseded leader may race a later term, but it must never
                // replace the cluster identity selected by the committed log.
            },
            .upsert_node => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try nodeKeyForGroup(&key_buf, group_id, record.node_id);
                const value = try encodeNodeRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .register_node => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try nodeKeyForGroup(&key_buf, group_id, record.node_id);
                const value = try self.encodeRegistrationNodeRecordTxn(txn, group_id, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_node => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try nodeKeyForGroup(&key_buf, group_id, record.node_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .request_node_shutdown => |record| {
                try self.applyNodeShutdownRequestTxn(txn, group_id, record.node_id);
            },
            .cancel_node_shutdown => |record| {
                try self.applyNodeShutdownCancelTxn(txn, group_id, record.node_id);
            },
            .finalize_node_shutdown => |record| {
                try self.applyNodeShutdownFinalizeTxn(txn, group_id, record.node_id);
            },
            .upsert_store => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
                const applied = try self.normalizeStoreUpsertDrainIntentTxn(txn, group_id, record);
                defer metadata_table_manager.freeStore(self.alloc, applied);
                const value = try encodeStoreRecord(self.alloc, applied);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .store,
                    .metadata_group_id = group_id,
                    .store_id = record.store_id,
                    .node_id = applied.node_id,
                });
                try self.retryPendingNodeShutdownFinalizeTxn(txn, group_id, applied.node_id);
            },
            .register_store => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
                const applied = try self.normalizeStoreDrainIntentTxn(txn, group_id, record);
                const value = try encodeStoreRecord(self.alloc, applied);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .store,
                    .metadata_group_id = group_id,
                    .store_id = record.store_id,
                    .node_id = record.node_id,
                });
                try self.retryPendingNodeShutdownFinalizeTxn(txn, group_id, record.node_id);
            },
            .remove_store => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
                const existing = try self.loadStoreRecordTxn(txn, group_id, record.store_id);
                defer if (existing) |store| metadata_table_manager.freeStore(self.alloc, store);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .store,
                    .metadata_group_id = group_id,
                    .store_id = record.store_id,
                });
                if (existing) |store| try self.retryPendingNodeShutdownFinalizeTxn(txn, group_id, store.node_id);
            },
            .upsert_replica_intent => |replacement| {
                try self.applyPlacementCompareAndUpsertTxn(
                    txn,
                    group_id,
                    replacement.expected_metadata_version,
                    replacement.expected_version_fence,
                    replacement.expected_target_drain_requested,
                    replacement.replacement,
                );
            },
            .remove_replica_intent => |record| {
                try self.applyPlacementRemoveIfVersionTxn(
                    txn,
                    group_id,
                    record.group_id,
                    record.local_node_id,
                    record.expected_metadata_version,
                );
                try self.retryPendingNodeShutdownFinalizeTxn(txn, group_id, record.local_node_id);
            },
            .upsert_table => |record| {
                try self.applyTableUpsertTxn(txn, group_id, record);
            },
            .compare_and_replace_table => |replacement| {
                try self.applyTableCompareAndReplaceTxn(
                    txn,
                    group_id,
                    replacement.expected,
                    replacement.replacement,
                );
            },
            .remove_table => |record| {
                const fence = try self.loadTableTransitionFenceTxn(
                    txn,
                    group_id,
                    record.table_id,
                );
                if (fence.active() or
                    fence.generation != record.expected_transition_generation)
                    return;
                const existing_table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (existing_table_name) |name| self.alloc.free(name);
                var key_buf: [160]u8 = undefined;
                const key = try tableKeyForGroup(&key_buf, group_id, record.table_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .table,
                    .metadata_group_id = group_id,
                    .table_name = existing_table_name,
                    .table_id = record.table_id,
                });
            },
            .upsert_schema_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [192]u8 = undefined;
                const key = try schemaProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id);
                const value = try encodeSchemaProgressRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .schema_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                });
            },
            .remove_schema_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [192]u8 = undefined;
                const key = try schemaProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .schema_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                });
            },
            .upsert_restore_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [224]u8 = undefined;
                const key = try restoreProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id, record.group_id);
                const value = try encodeRestoreProgressRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .restore_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                    .group_id = record.group_id,
                });
            },
            .remove_restore_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [224]u8 = undefined;
                const key = try restoreProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id, record.group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .restore_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                    .group_id = record.group_id,
                });
            },
            .upsert_replication_source_status => |record| {
                try self.applyReplicationSourceStatusUpsertTxn(txn, group_id, record);
            },
            .claim_replication_source_cutover => |claim| {
                try self.applyReplicationSourceCutoverClaimTxn(
                    txn,
                    group_id,
                    claim.expected_replication_sources_json,
                    claim.expected_authority_id,
                    claim.record,
                );
            },
            .complete_replication_source_retirement => |expected| {
                try self.applyReplicationSourceRetirementCompletionTxn(
                    txn,
                    group_id,
                    expected,
                );
            },
            .upsert_range => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [160]u8 = undefined;
                const key = try rangeKeyForGroup(&key_buf, group_id, record.group_id);
                const value = try encodeRangeRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .group_id = record.group_id,
                });
            },
            .complete_restore_range => |expected| complete: {
                var key_buf: [160]u8 = undefined;
                const key = try rangeKeyForGroup(&key_buf, group_id, expected.group_id);
                const encoded = txn.get(key) catch |err| switch (err) {
                    error.NotFound => break :complete,
                    else => return err,
                };
                var current = try decodeRangeRecord(self.alloc, encoded);
                defer metadata_table_manager.freeRange(self.alloc, current);
                if (!metadata_table_manager.restoreIntentMatchesRange(expected, current)) break :complete;

                const table_name = try self.lookupTableNameTxn(txn, group_id, current.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                try metadata_table_manager.clearOwnedRangeRestoreIntent(self.alloc, &current);
                const value = try encodeRangeRecord(self.alloc, current);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = current.table_id,
                    .group_id = current.group_id,
                });
            },
            .remove_range => |record| {
                const existing = blk: {
                    var key_buf: [160]u8 = undefined;
                    const existing_key = try rangeKeyForGroup(&key_buf, group_id, record.group_id);
                    const encoded = txn.get(existing_key) catch |err| switch (err) {
                        error.NotFound => break :blk null,
                        else => return err,
                    };
                    const decoded = try decodeRangeRecord(self.alloc, encoded);
                    break :blk decoded;
                };
                defer if (existing) |record_existing| metadata_table_manager.freeRange(self.alloc, record_existing);
                const existing_table_id = if (existing) |record_existing| record_existing.table_id else 0;
                const table_name = if (existing) |record_existing|
                    try self.lookupTableNameTxn(txn, group_id, record_existing.table_id)
                else
                    null;
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [160]u8 = undefined;
                const key = try rangeKeyForGroup(&key_buf, group_id, record.group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = existing_table_id,
                    .group_id = record.group_id,
                });
            },
            .admit_split_transition => |admission| {
                try self.applySplitAdmissionTxn(txn, group_id, admission.expected_source_epoch, admission.record);
            },
            .upsert_split_transition => |record| {
                try self.applySplitTransitionUpsertTxn(txn, group_id, record);
            },
            .remove_split_transition => |record| {
                try self.applySplitTransitionRemovalTxn(txn, group_id, record.transition_id);
            },
            .upsert_merge_transition => |record| {
                try self.applyMergeTransitionUpsertTxn(txn, group_id, record);
            },
            .remove_merge_transition => |record| {
                try self.applyMergeTransitionRemovalTxn(txn, group_id, record.transition_id);
            },
            .upsert_reconcile_lease => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try reconcileLeaseKeyForGroup(&key_buf, group_id);
                const value = try encodeReconcileLeaseRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .reconcile_lease,
                    .metadata_group_id = group_id,
                });
            },
            .remove_reconcile_lease => {
                var key_buf: [160]u8 = undefined;
                const key = try reconcileLeaseKeyForGroup(&key_buf, group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .reconcile_lease,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_shuffle_join_lease => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try shuffleJoinLeaseKeyForGroup(&key_buf, group_id, record.job_id);
                const value = try encodeShuffleJoinLeaseRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .shuffle_join_lease,
                    .metadata_group_id = group_id,
                });
            },
            .remove_shuffle_join_lease => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try shuffleJoinLeaseKeyForGroup(&key_buf, group_id, record.job_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .shuffle_join_lease,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_restore_job => |record| {
                var key_buf: [256]u8 = undefined;
                const key = try restoreJobKeyForGroup(&key_buf, group_id, record.key);
                try txn.put(key, record.value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .restore_job,
                    .metadata_group_id = group_id,
                });
            },
            .remove_restore_job => |record| {
                var key_buf: [256]u8 = undefined;
                const key = try restoreJobKeyForGroup(&key_buf, group_id, record.key);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .restore_job,
                    .metadata_group_id = group_id,
                });
            },
            .remove_restore_jobs => |record| {
                for (record.keys) |logical_key| {
                    var key_buf: [256]u8 = undefined;
                    const key = try restoreJobKeyForGroup(&key_buf, group_id, logical_key);
                    txn.delete(key) catch |err| switch (err) {
                        error.NotFound => {},
                        else => return err,
                    };
                    self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                }
                self.notifyProjectionListeners(.{
                    .kind = .restore_job,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_reallocation_request => |record| admit: {
                var active_key_buf: [160]u8 = undefined;
                const active_key = try reallocationRequestKeyForGroup(&active_key_buf, group_id);
                const active: ?[]const u8 = txn.get(active_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (active) |encoded| {
                    var pos: usize = 0;
                    const current = try readReallocationRequestRecord(encoded, &pos);
                    if (pos != encoded.len) return error.InvalidMetadataRecord;
                    // Reapplying the active command is idempotent and must not
                    // manufacture a successor generation.
                    if (current.request_id == record.request_id) break :admit;

                    // Keep the published generation stable while stores
                    // acknowledge it, but retain one durable rerun latch. A
                    // newer trigger may safely replace this private pending
                    // value because no store can observe it until promotion.
                    var pending_key_buf: [168]u8 = undefined;
                    const pending_key = try pendingReallocationRequestKeyForGroup(&pending_key_buf, group_id);
                    const value = try encodeReallocationRequestRecord(self.alloc, record);
                    defer self.alloc.free(value);
                    try txn.put(pending_key, value);
                    break :admit;
                }

                const value = try encodeReallocationRequestRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(active_key, value);
                // This key should only coexist with an active request. Remove
                // any orphan defensively when establishing a new generation.
                var pending_key_buf: [168]u8 = undefined;
                const pending_key = try pendingReallocationRequestKeyForGroup(&pending_key_buf, group_id);
                txn.delete(pending_key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = active_key });
            },
            .remove_reallocation_request => |expected| remove: {
                var active_key_buf: [160]u8 = undefined;
                const active_key = try reallocationRequestKeyForGroup(&active_key_buf, group_id);
                const encoded = txn.get(active_key) catch |err| switch (err) {
                    error.NotFound => break :remove,
                    else => return err,
                };
                var pos: usize = 0;
                const current = try readReallocationRequestRecord(encoded, &pos);
                if (pos != encoded.len) return error.InvalidMetadataRecord;
                if (current.request_id != expected.expected_request_id) break :remove;

                var pending_key_buf: [168]u8 = undefined;
                const pending_key = try pendingReallocationRequestKeyForGroup(&pending_key_buf, group_id);
                const pending_encoded: ?[]const u8 = txn.get(pending_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (pending_encoded) |pending_value| {
                    var pending_pos: usize = 0;
                    const pending = try readReallocationRequestRecord(pending_value, &pending_pos);
                    if (pending_pos != pending_value.len) return error.InvalidMetadataRecord;
                    const promoted_value = try encodeReallocationRequestRecord(self.alloc, pending);
                    defer self.alloc.free(promoted_value);
                    try txn.put(active_key, promoted_value);
                    try txn.delete(pending_key);
                } else {
                    txn.delete(active_key) catch |err| switch (err) {
                        error.NotFound => {},
                        else => return err,
                    };
                }
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = active_key });
            },
            .upsert_extension_package => |record| {
                var key_buf: [256]u8 = undefined;
                const key = try extensionPackageKeyForGroup(&key_buf, group_id, record.name, record.version);
                const value = try encodeExtensionPackageRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_extension_package => |record| {
                var key_buf: [256]u8 = undefined;
                const key = try extensionPackageKeyForGroup(&key_buf, group_id, record.name, record.version);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .upsert_installed_extension => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try installedExtensionKeyForGroup(&key_buf, group_id, record.name);
                const value = try encodeInstalledExtensionRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_installed_extension => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try installedExtensionKeyForGroup(&key_buf, group_id, record.name);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .upsert_extension_member => |record| {
                var key_buf: [256]u8 = undefined;
                const key = try extensionMemberKeyForGroup(&key_buf, group_id, record.extension_name, record.object_kind, record.object_name);
                const value = try encodeExtensionMemberRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_extension_member => |record| {
                var key_buf: [256]u8 = undefined;
                const key = try extensionMemberKeyForGroup(&key_buf, group_id, record.extension_name, record.object_kind, record.object_name);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .upsert_extension_dependency => |record| {
                var key_buf: [320]u8 = undefined;
                const key = try extensionDependencyKeyForGroup(&key_buf, group_id, record.extension_name, record.required_extension_name, record.package_name);
                const value = try encodeExtensionDependencyRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_extension_dependency => |record| {
                var key_buf: [320]u8 = undefined;
                const key = try extensionDependencyKeyForGroup(&key_buf, group_id, record.extension_name, record.required_extension_name, record.package_name);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .apply_extension_lifecycle => |delta| {
                try self.applyExtensionLifecycleDeltaTxn(txn, group_id, delta);
            },
        }
    }

    fn applyTableUpsertTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.TableRecord,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, record.table_id);
        const encoded_existing = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (encoded_existing) |encoded| {
            const existing = try decodeTableRecord(self.alloc, encoded);
            defer metadata_table_manager.freeTable(self.alloc, existing);
            if (metadata_table_manager.tableDefinitionsEqual(existing, record)) return;
        }
        if ((try self.loadTableTransitionFenceTxn(txn, group_id, record.table_id)).active()) return;
        try self.putTableRecordTxn(txn, group_id, key, record);
    }

    fn applyReplicationSourceStatusUpsertTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.ReplicationSourceStatusRecord,
    ) !void {
        var key_buf: [224]u8 = undefined;
        const key = try replicationSourceStatusKeyForGroup(
            &key_buf,
            group_id,
            record.table_id,
            record.source_ordinal,
        );
        const encoded_existing = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (encoded_existing) |encoded| {
            const existing = try decodeReplicationSourceStatusRecord(self.alloc, encoded);
            defer metadata_table_manager.freeReplicationSourceStatus(self.alloc, existing);
            if (existing.cutover_authority_id != 0) {
                if (!replicationCutoverAuthorityIdentityEqual(existing, record))
                    return;
                if (!replicationCutoverRetirementEqual(existing, record))
                    return;
            }
            if (existing.cutover_authority_id == 0 and
                record.cutover_authority_id != 0)
                return;
        } else if (record.cutover_authority_id != 0) {
            // Exact cutover authority is admitted only through the catalog- and
            // prior-authority-CAS command below.
            return;
        }
        try self.putReplicationSourceStatusTxn(txn, group_id, key, record);
    }

    fn applyReplicationSourceCutoverClaimTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        expected_replication_sources_json: []const u8,
        expected_authority_id: u64,
        record: metadata.ReplicationSourceStatusRecord,
    ) !void {
        var table_key_buf: [160]u8 = undefined;
        const table_key = try tableKeyForGroup(
            &table_key_buf,
            group_id,
            record.table_id,
        );
        const encoded_table = txn.get(table_key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        const table = try decodeTableRecord(self.alloc, encoded_table);
        defer metadata_table_manager.freeTable(self.alloc, table);
        if (!std.mem.eql(
            u8,
            table.replication_sources_json,
            expected_replication_sources_json,
        )) return;

        var status_key_buf: [224]u8 = undefined;
        const status_key = try replicationSourceStatusKeyForGroup(
            &status_key_buf,
            group_id,
            record.table_id,
            record.source_ordinal,
        );
        const encoded_existing = txn.get(status_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (encoded_existing) |encoded| {
            const existing = try decodeReplicationSourceStatusRecord(self.alloc, encoded);
            defer metadata_table_manager.freeReplicationSourceStatus(self.alloc, existing);
            if (replicationCutoverAuthorityIdentityEqual(existing, record)) {
                if (!replicationCutoverRetirementEqual(existing, record)) return;
                // Provider cleanup can legitimately fail after authority is
                // claimed but before any snapshot checkpoint exists. Permit
                // that exact authority to retry preparation, while refusing
                // to roll an advanced snapshot/streaming status backward.
                if (std.mem.eql(u8, existing.phase, "failed") and
                    std.mem.eql(
                        u8,
                        existing.cutover_mode,
                        "exported_snapshot_pending",
                    ) and
                    existing.snapshot_offset == 0 and
                    existing.prepared_checkpoint.len == 0 and
                    existing.stream_checkpoint.len == 0)
                {
                    try self.putReplicationSourceStatusTxn(
                        txn,
                        group_id,
                        status_key,
                        record,
                    );
                }
                return;
            }
            if (existing.cutover_authority_id != expected_authority_id or
                existing.retired_cutover_authority_id != 0 or
                record.cutover_authority_id == expected_authority_id)
                return;
            if (expected_authority_id == 0) {
                if (record.retired_cutover_authority_id != 0 or
                    record.retired_slot_name.len != 0 or
                    record.retired_publication_name.len != 0)
                    return;
            } else if (record.retired_cutover_authority_id != expected_authority_id or
                !std.mem.eql(u8, record.retired_slot_name, existing.slot_name) or
                !std.mem.eql(
                    u8,
                    record.retired_publication_name,
                    existing.publication_name,
                ))
            {
                return;
            }
        } else if (expected_authority_id != 0 or
            record.retired_cutover_authority_id != 0 or
            record.retired_slot_name.len != 0 or
            record.retired_publication_name.len != 0)
        {
            return;
        }
        try self.putReplicationSourceStatusTxn(txn, group_id, status_key, record);
    }

    fn applyReplicationSourceRetirementCompletionTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        expected: metadata.ReplicationSourceStatusRecord,
    ) !void {
        var key_buf: [224]u8 = undefined;
        const key = try replicationSourceStatusKeyForGroup(
            &key_buf,
            group_id,
            expected.table_id,
            expected.source_ordinal,
        );
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        var current = try decodeReplicationSourceStatusRecord(
            self.alloc,
            encoded,
        );
        defer metadata_table_manager.freeReplicationSourceStatus(
            self.alloc,
            current,
        );
        if (!replicationCutoverAuthorityIdentityEqual(current, expected) or
            !replicationCutoverRetirementEqual(current, expected))
            return;

        const retired_authority_id = current.retired_cutover_authority_id;
        const retired_slot_name = current.retired_slot_name;
        const retired_publication_name = current.retired_publication_name;
        defer {
            current.retired_cutover_authority_id = retired_authority_id;
            current.retired_slot_name = retired_slot_name;
            current.retired_publication_name = retired_publication_name;
        }
        current.retired_cutover_authority_id = 0;
        current.retired_slot_name = "";
        current.retired_publication_name = "";
        current.updated_at_ms = @max(current.updated_at_ms, expected.updated_at_ms);
        try self.putReplicationSourceStatusTxn(txn, group_id, key, current);
    }

    fn putReplicationSourceStatusTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        key: []const u8,
        record: metadata.ReplicationSourceStatusRecord,
    ) !void {
        const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
        defer if (table_name) |name| self.alloc.free(name);
        const value = try encodeReplicationSourceStatusRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .replication_source_status,
            .metadata_group_id = group_id,
            .table_name = table_name,
            .table_id = record.table_id,
        });
    }

    fn applyTableCompareAndReplaceTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        expected: metadata.TableRecord,
        replacement: metadata.TableRecord,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, expected.table_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        const current = try decodeTableRecord(self.alloc, encoded);
        defer metadata_table_manager.freeTable(self.alloc, current);
        if (!metadata_table_manager.tableDefinitionsEqual(current, expected)) return;
        if ((try self.loadTableTransitionFenceTxn(txn, group_id, expected.table_id)).active()) return;
        if (metadata_table_manager.tableDefinitionsEqual(current, replacement)) return;
        try self.putTableRecordTxn(txn, group_id, key, replacement);
    }

    fn putTableRecordTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        key: []const u8,
        record: metadata.TableRecord,
    ) !void {
        const value = try encodeTableRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .table,
            .metadata_group_id = group_id,
            .table_name = record.name,
            .table_id = record.table_id,
        });
    }

    fn loadTableTransitionFenceTxn(
        _: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        table_id: u64,
    ) !TableTransitionFence {
        var key_buf: [192]u8 = undefined;
        const key = try tableTransitionFenceKeyForGroup(&key_buf, group_id, table_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return .{},
            else => return err,
        };
        return try decodeTableTransitionFence(encoded);
    }

    fn updateTableTransitionFenceTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        table_id: u64,
        was_active: bool,
        is_active: bool,
    ) !void {
        if (table_id == 0 or was_active == is_active) return;
        var fence = try self.loadTableTransitionFenceTxn(txn, group_id, table_id);
        if (fence.generation == std.math.maxInt(u64))
            return error.TableTransitionGenerationExhausted;
        if (is_active) {
            if (fence.active_count == std.math.maxInt(u32))
                return error.TableTransitionCountExhausted;
            fence.active_count += 1;
        } else {
            if (fence.active_count == 0)
                return error.InvalidTableTransitionFence;
            fence.active_count -= 1;
        }
        fence.generation += 1;

        var key_buf: [192]u8 = undefined;
        const key = try tableTransitionFenceKeyForGroup(&key_buf, group_id, table_id);
        var encoded: [@sizeOf(u64) + @sizeOf(u32)]u8 = undefined;
        encodeTableTransitionFence(&encoded, fence);
        try txn.put(key, &encoded);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
    }

    fn applySplitTransitionUpsertTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.SplitTransitionRecord,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try splitTransitionKeyForGroup(&key_buf, group_id, record.transition_id);
        const encoded_existing = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const encoded = encoded_existing orelse {
            // New splits must use admit_split_transition, which atomically
            // validates the source epoch, range identity, and table contract.
            return;
        };
        var existing_active = false;
        const existing = try decodeSplitTransitionRecord(self.alloc, encoded);
        defer metadata_table_manager.freeSplitTransitionRecord(self.alloc, existing);
        if (!splitTransitionUpdateAllowed(existing, record)) return;
        existing_active = splitTransitionActive(existing);

        const value = try encodeSplitTransitionRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        try self.updateTableTransitionFenceTxn(
            txn,
            group_id,
            record.table_contract.table_id,
            existing_active,
            splitTransitionActive(record),
        );
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .split_transition,
            .metadata_group_id = group_id,
            .group_id = record.source_group_id,
        });
        self.notifyCommittedTransition(.{ .upsert_split = record });
    }

    fn applySplitTransitionRemovalTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        transition_id: u64,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try splitTransitionKeyForGroup(&key_buf, group_id, transition_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        const existing = try decodeSplitTransitionRecord(self.alloc, encoded);
        defer metadata_table_manager.freeSplitTransitionRecord(self.alloc, existing);
        if (!transitionPhaseTerminal(existing.phase)) return;

        try txn.delete(key);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .split_transition,
            .metadata_group_id = group_id,
        });
        self.notifyCommittedTransition(.{ .remove_split = transition_id });
    }

    fn applyMergeTransitionUpsertTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.MergeTransitionRecord,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try mergeTransitionKeyForGroup(&key_buf, group_id, record.transition_id);
        const encoded_existing = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        var existing_active = false;
        if (encoded_existing) |encoded| {
            const existing = try decodeMergeTransitionRecord(self.alloc, encoded);
            defer metadata_table_manager.freeMergeTransitionRecord(self.alloc, existing);
            if (!mergeTransitionUpdateAllowed(existing, record)) return;
            existing_active = !transitionPhaseTerminal(existing.phase);
        } else if (!try self.mergeTransitionAdmissionMatchesTxn(
            txn,
            group_id,
            record,
        )) {
            return;
        }

        const value = try encodeMergeTransitionRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        try self.updateTableTransitionFenceTxn(
            txn,
            group_id,
            record.table_contract.table_id,
            existing_active,
            !transitionPhaseTerminal(record.phase),
        );
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .merge_transition,
            .metadata_group_id = group_id,
            .group_id = record.receiver_group_id,
        });
        self.notifyCommittedTransition(.{ .upsert_merge = record });
    }

    fn applyMergeTransitionRemovalTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        transition_id: u64,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try mergeTransitionKeyForGroup(&key_buf, group_id, transition_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        const existing = try decodeMergeTransitionRecord(self.alloc, encoded);
        defer metadata_table_manager.freeMergeTransitionRecord(self.alloc, existing);
        if (!transitionPhaseTerminal(existing.phase)) return;

        try txn.delete(key);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .merge_transition,
            .metadata_group_id = group_id,
        });
        self.notifyCommittedTransition(.{ .remove_merge = transition_id });
    }

    /// Atomically reserves the next source-local split epoch and publishes its
    /// transition. Failed compare-and-set preconditions are deterministic
    /// no-ops because a committed Raft command must always remain applicable.
    fn applySplitAdmissionTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        expected_source_epoch: u64,
        record: metadata.SplitTransitionRecord,
    ) !void {
        const split_key = record.split_key orelse return error.InvalidSplitAdmission;

        var transition_key_buf: [160]u8 = undefined;
        const transition_key = try splitTransitionKeyForGroup(&transition_key_buf, group_id, record.transition_id);
        const existing_transition = txn.get(transition_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existing_transition) |encoded| {
            const existing = try decodeSplitTransitionRecord(self.alloc, encoded);
            defer metadata_table_manager.freeSplitTransitionRecord(self.alloc, existing);
            if (splitTransitionActive(existing)) return;
        }

        var source_key_buf: [160]u8 = undefined;
        const source_key = try rangeKeyForGroup(&source_key_buf, group_id, record.source_group_id);
        const encoded_source = txn.get(source_key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        var source = try decodeRangeRecord(self.alloc, encoded_source);
        defer metadata_table_manager.freeRange(self.alloc, source);
        if (source.split_attempt_epoch != expected_source_epoch or
            expected_source_epoch == std.math.maxInt(u64) or
            record.attempt_epoch != expected_source_epoch + 1 or
            source.table_id != record.table_contract.table_id or
            !rangeMatchesTransitionIdentity(
                source,
                record.table_contract.source_identity,
            ) or
            !optionalBytesEqual(source.end_key, record.source_range_end) or
            !keyStrictlyInsideRange(split_key, source.start_key, source.end_key))
        {
            return;
        }
        if (!try self.tableMatchesTransitionContractTxn(
            txn,
            group_id,
            record.table_contract,
        )) return;

        var destination_key_buf: [160]u8 = undefined;
        const destination_key = try rangeKeyForGroup(&destination_key_buf, group_id, record.destination_group_id);
        if (txn.get(destination_key)) |_| {
            return;
        } else |err| switch (err) {
            error.NotFound => {},
            else => return err,
        }

        var prefix_buf: [128]u8 = undefined;
        const prefix = try splitTransitionPrefixForGroup(&prefix_buf, group_id);
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(prefix);
        while (entry) |kv| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            const existing = try decodeSplitTransitionRecord(self.alloc, kv.value);
            defer metadata_table_manager.freeSplitTransitionRecord(self.alloc, existing);
            if (splitTransitionActive(existing) and existing.source_group_id == record.source_group_id) return;
        }

        source.split_attempt_epoch = record.attempt_epoch;
        const source_value = try encodeRangeRecord(self.alloc, source);
        defer self.alloc.free(source_value);
        const transition_value = try encodeSplitTransitionRecord(self.alloc, record);
        defer self.alloc.free(transition_value);
        try txn.put(source_key, source_value);
        try txn.put(transition_key, transition_value);
        try self.updateTableTransitionFenceTxn(
            txn,
            group_id,
            record.table_contract.table_id,
            false,
            splitTransitionActive(record),
        );

        const table_name = try self.lookupTableNameTxn(txn, group_id, source.table_id);
        defer if (table_name) |name| self.alloc.free(name);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = source_key });
        self.notifyProjectionListeners(.{
            .kind = .range,
            .metadata_group_id = group_id,
            .table_name = table_name,
            .table_id = source.table_id,
            .group_id = source.group_id,
        });
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = transition_key });
        self.notifyProjectionListeners(.{
            .kind = .split_transition,
            .metadata_group_id = group_id,
            .group_id = record.source_group_id,
        });
        self.notifyCommittedTransition(.{ .upsert_split = record });
    }

    fn tableMatchesTransitionContractTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        contract: metadata.TransitionTableContract,
    ) !bool {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, contract.table_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        const table = try decodeTableRecord(self.alloc, encoded);
        defer metadata_table_manager.freeTable(self.alloc, table);
        return table.table_id == contract.table_id and
            std.mem.eql(u8, table.name, contract.table_name) and
            std.mem.eql(u8, table.schema_json, contract.schema_json) and
            std.mem.eql(u8, table.indexes_json, contract.indexes_json);
    }

    fn mergeTransitionAdmissionMatchesTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.MergeTransitionRecord,
    ) !bool {
        if (record.phase != .prepare or
            record.donor_group_id == record.receiver_group_id)
            return false;
        if (!try self.tableMatchesTransitionContractTxn(
            txn,
            group_id,
            record.table_contract,
        )) return false;

        var donor_key_buf: [160]u8 = undefined;
        const donor_key = try rangeKeyForGroup(
            &donor_key_buf,
            group_id,
            record.donor_group_id,
        );
        const encoded_donor = txn.get(donor_key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        const donor = try decodeRangeRecord(self.alloc, encoded_donor);
        defer metadata_table_manager.freeRange(self.alloc, donor);

        var receiver_key_buf: [160]u8 = undefined;
        const receiver_key = try rangeKeyForGroup(
            &receiver_key_buf,
            group_id,
            record.receiver_group_id,
        );
        const encoded_receiver = txn.get(receiver_key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        const receiver = try decodeRangeRecord(self.alloc, encoded_receiver);
        defer metadata_table_manager.freeRange(self.alloc, receiver);

        return donor.table_id == record.table_contract.table_id and
            receiver.table_id == record.table_contract.table_id and
            rangesAreAdjacent(donor, receiver) and
            rangeMatchesTransitionIdentity(
                donor,
                record.table_contract.source_identity,
            ) and
            rangeMatchesTransitionIdentity(
                receiver,
                record.table_contract.target_identity,
            );
    }

    fn applyExtensionLifecycleDeltaTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, delta: ExtensionLifecycleDelta) !void {
        // Extension lifecycle commands are atomic. Preflight every structural
        // table write before touching any extension row so an active range
        // transition rejects the whole delta rather than publishing half of it.
        for (delta.upsert_tables) |record| {
            var key_buf: [160]u8 = undefined;
            const key = try tableKeyForGroup(&key_buf, group_id, record.table_id);
            const encoded_existing = txn.get(key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (encoded_existing) |encoded| {
                const existing = try decodeTableRecord(self.alloc, encoded);
                defer metadata_table_manager.freeTable(self.alloc, existing);
                if (metadata_table_manager.tableDefinitionsEqual(existing, record)) continue;
            }
            if ((try self.loadTableTransitionFenceTxn(txn, group_id, record.table_id)).active()) return;
        }
        for (delta.upsert_tables) |record| try self.applyTableUpsertTxn(txn, group_id, record);
        for (delta.remove_extension_dependencies) |record| {
            var key_buf: [320]u8 = undefined;
            const key = try extensionDependencyKeyForGroup(&key_buf, group_id, record.extension_name, record.required_extension_name, record.package_name);
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.remove_extension_members) |record| {
            var key_buf: [256]u8 = undefined;
            const key = try extensionMemberKeyForGroup(&key_buf, group_id, record.extension_name, record.object_kind, record.object_name);
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.remove_installed_extensions) |name| {
            var key_buf: [192]u8 = undefined;
            const key = try installedExtensionKeyForGroup(&key_buf, group_id, name);
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.upsert_installed_extensions) |record| {
            var key_buf: [192]u8 = undefined;
            const key = try installedExtensionKeyForGroup(&key_buf, group_id, record.name);
            const value = try encodeInstalledExtensionRecord(self.alloc, record);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.upsert_extension_dependencies) |record| {
            var key_buf: [320]u8 = undefined;
            const key = try extensionDependencyKeyForGroup(&key_buf, group_id, record.extension_name, record.required_extension_name, record.package_name);
            const value = try encodeExtensionDependencyRecord(self.alloc, record);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
        for (delta.upsert_extension_members) |record| {
            var key_buf: [256]u8 = undefined;
            const key = try extensionMemberKeyForGroup(&key_buf, group_id, record.extension_name, record.object_kind, record.object_name);
            const value = try encodeExtensionMemberRecord(self.alloc, record);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
        }
    }

    fn encodeRegistrationNodeRecordTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.NodeRecord,
    ) ![]u8 {
        var applied = record;
        const existing = try self.loadNodeRecordTxn(txn, group_id, record.node_id);
        defer if (existing) |existing_record| metadata_table_manager.freeNode(self.alloc, existing_record);
        if (existing) |existing_record| {
            applied.lifecycle = existing_record.lifecycle;
        }
        return try encodeNodeRecord(self.alloc, applied);
    }

    fn applyNodeShutdownRequestTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !void {
        const existing = try self.loadNodeRecordTxn(txn, group_id, node_id);
        defer if (existing) |record| metadata_table_manager.freeNode(self.alloc, record);
        // A retried request is idempotent across every accepted shutdown
        // phase. Only cancellation may move finalizing back to active; keeping
        // this state monotonic preserves the one-shot finalize intent while
        // placement or store observations drain.
        if (existing == null or !metadata_table_manager.nodeLifecycleFinalizing(existing.?.lifecycle)) {
            try self.setNodeLifecycleTxn(txn, group_id, node_id, metadata_table_manager.node_lifecycle_draining, true);
        }
        try self.setNodeStoresDrainRequestedTxn(txn, group_id, node_id, true);
    }

    fn applyNodeShutdownCancelTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !void {
        try self.setNodeLifecycleTxn(txn, group_id, node_id, metadata_table_manager.node_lifecycle_active, false);
        try self.setNodeStoresDrainRequestedTxn(txn, group_id, node_id, false);
    }

    fn applyNodeShutdownFinalizeTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !void {
        var node_key_buf: [160]u8 = undefined;
        const node_key = try nodeKeyForGroup(&node_key_buf, group_id, node_id);

        const existing_node = try self.loadNodeRecordTxn(txn, group_id, node_id);
        defer if (existing_node) |record| metadata_table_manager.freeNode(self.alloc, record);
        var draining_node = false;
        if (existing_node) |record| {
            // Explicit cancellation wins if it commits before finalization.
            // Other debt races retain a durable finalizing lifecycle below so
            // the accepted one-shot request is not silently lost.
            if (metadata_table_manager.nodeLifecycleActive(record.lifecycle)) return;
            draining_node = true;
        }

        // Placement ownership is the durable deletion fence. Observational
        // store debt below may lag, but finalization must never remove the
        // node/store records while a placement still targets this node.
        var has_termination_debt = try self.nodeHasPlacementIntentTxn(txn, group_id, node_id);

        const StoreRef = struct {
            store_id: u64,
            node_id: u64,
        };
        var stores_to_delete = std.ArrayListUnmanaged(StoreRef).empty;
        defer stores_to_delete.deinit(self.alloc);

        var prefix_buf: [128]u8 = undefined;
        const prefix = try storePrefixForGroup(&prefix_buf, group_id);
        {
            var cur = try txn.openCursor();
            defer cur.close();
            var entry = try cur.seekAtOrAfter(prefix);
            while (entry) |kv| : (entry = try cur.next()) {
                if (!std.mem.startsWith(u8, kv.key, prefix)) break;
                const store = try decodeStoreRecord(self.alloc, kv.value);
                defer metadata_table_manager.freeStore(self.alloc, store);
                if (store.node_id == node_id) {
                    if (!draining_node and !store.drain_requested) return;
                    has_termination_debt = has_termination_debt or metadata_table_manager.storeHasTerminationDebt(store);
                    try stores_to_delete.append(self.alloc, .{ .store_id = store.store_id, .node_id = store.node_id });
                }
            }
        }

        if (has_termination_debt) {
            // Persist the accepted finalization request in the same record
            // whose draining lifecycle already fences new placement. Store
            // status and placement-removal commands retry only this node when
            // they clear debt, avoiding a metadata-wide polling scan.
            try self.setNodeLifecycleTxn(
                txn,
                group_id,
                node_id,
                metadata_table_manager.node_lifecycle_finalizing,
                true,
            );
            return;
        }

        txn.delete(node_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = node_key });

        for (stores_to_delete.items) |store| {
            var key_buf: [160]u8 = undefined;
            const key = try storeKeyForGroup(&key_buf, group_id, store.store_id);
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            self.notifyProjectionListeners(.{
                .kind = .store,
                .metadata_group_id = group_id,
                .store_id = store.store_id,
                .node_id = store.node_id,
            });
        }
    }

    fn retryPendingNodeShutdownFinalizeTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        node_id: u64,
    ) !void {
        const node = (try self.loadNodeRecordTxn(txn, group_id, node_id)) orelse return;
        defer metadata_table_manager.freeNode(self.alloc, node);
        if (!metadata_table_manager.nodeLifecycleFinalizing(node.lifecycle)) return;
        try self.applyNodeShutdownFinalizeTxn(txn, group_id, node_id);
    }

    fn nodeHasPlacementIntentTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        metadata_group_id: u64,
        node_id: u64,
    ) !bool {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try placementPrefixForGroup(&prefix_buf, metadata_group_id);
        var cur = try txn.openCursor();
        defer cur.close();
        var entry = try cur.seekAtOrAfter(prefix);
        while (entry) |kv| : (entry = try cur.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            const intent = try decodePlacementIntent(self.alloc, kv.value);
            defer raft_reconciler.freeIntentOwned(self.alloc, intent);
            if (intent.record.local_node_id == node_id) return true;
        }
        return false;
    }

    fn setNodeLifecycleTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        node_id: u64,
        lifecycle: []const u8,
        create_if_missing: bool,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try nodeKeyForGroup(&key_buf, group_id, node_id);

        const existing = try self.loadNodeRecordTxn(txn, group_id, node_id);
        defer if (existing) |existing_record| metadata_table_manager.freeNode(self.alloc, existing_record);

        if (existing) |existing_record| {
            if (std.mem.eql(u8, existing_record.lifecycle, lifecycle)) return;
            var applied = existing_record;
            applied.lifecycle = lifecycle;
            const value = try encodeNodeRecord(self.alloc, applied);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            return;
        }

        if (!create_if_missing) return;
        const record: metadata.NodeRecord = .{
            .node_id = node_id,
            .role = "data",
            .lifecycle = lifecycle,
        };
        const value = try encodeNodeRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
    }

    fn setNodeStoresDrainRequestedTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        node_id: u64,
        drain_requested: bool,
    ) !void {
        var updates = std.ArrayListUnmanaged(metadata.StoreRecord).empty;
        defer {
            for (updates.items) |record| metadata_table_manager.freeStore(self.alloc, record);
            updates.deinit(self.alloc);
        }

        var prefix_buf: [128]u8 = undefined;
        const prefix = try storePrefixForGroup(&prefix_buf, group_id);
        {
            var cur = try txn.openCursor();
            defer cur.close();
            var entry = try cur.seekAtOrAfter(prefix);
            while (entry) |kv| : (entry = try cur.next()) {
                if (!std.mem.startsWith(u8, kv.key, prefix)) break;
                var store = try decodeStoreRecord(self.alloc, kv.value);
                var store_owned = true;
                errdefer if (store_owned) metadata_table_manager.freeStore(self.alloc, store);
                if (store.node_id == node_id and store.drain_requested != drain_requested) {
                    store.drain_requested = drain_requested;
                    try updates.append(self.alloc, store);
                    store_owned = false;
                } else {
                    store_owned = false;
                    metadata_table_manager.freeStore(self.alloc, store);
                }
            }
        }

        for (updates.items) |record| {
            var key_buf: [160]u8 = undefined;
            const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
            const value = try encodeStoreRecord(self.alloc, record);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            self.notifyProjectionListeners(.{
                .kind = .store,
                .metadata_group_id = group_id,
                .store_id = record.store_id,
                .node_id = record.node_id,
            });
        }
    }

    fn normalizeStoreDrainIntentTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.StoreRecord,
    ) !metadata.StoreRecord {
        var applied = record;
        if (try self.nodeDrainRequestedTxn(txn, group_id, record.node_id)) {
            applied.drain_requested = true;
            return applied;
        }
        const existing = try self.loadStoreRecordTxn(txn, group_id, record.store_id);
        defer if (existing) |existing_record| metadata_table_manager.freeStore(self.alloc, existing_record);
        if (existing) |existing_record| {
            applied.drain_requested = existing_record.drain_requested;
        } else {
            applied.drain_requested = false;
        }
        return applied;
    }

    fn normalizeStoreUpsertDrainIntentTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.StoreRecord,
    ) !metadata.StoreRecord {
        var applied = try metadata_table_manager.cloneStore(self.alloc, record);
        if (try self.nodeDrainRequestedTxn(txn, group_id, record.node_id)) {
            applied.drain_requested = true;
        }
        return applied;
    }

    fn loadNodeRecordTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !?metadata.NodeRecord {
        var key_buf: [160]u8 = undefined;
        const key = try nodeKeyForGroup(&key_buf, group_id, node_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeNodeRecord(self.alloc, encoded);
    }

    fn nodeDrainRequestedTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !bool {
        const node = (try self.loadNodeRecordTxn(txn, group_id, node_id)) orelse return false;
        defer metadata_table_manager.freeNode(self.alloc, node);
        return !metadata_table_manager.nodeLifecycleActive(node.lifecycle);
    }

    fn loadPlacementIntentTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        placement_key: []const u8,
    ) !?raft_reconciler.PlacementIntent {
        const encoded = txn.get(placement_key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodePlacementIntent(self.alloc, encoded);
    }

    fn loadPlacementVersionFenceTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        metadata_group_id: u64,
        range_group_id: u64,
        local_node_id: u64,
    ) !u64 {
        _ = self;
        var key_buf: [192]u8 = undefined;
        const key = try placementVersionKeyForGroup(&key_buf, metadata_group_id, range_group_id, local_node_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return 0,
            else => return err,
        };
        if (encoded.len != @sizeOf(u64)) return error.InvalidPlacementVersionFence;
        return std.mem.readInt(u64, encoded[0..@sizeOf(u64)], .little);
    }

    fn writePlacementVersionFenceTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        metadata_group_id: u64,
        range_group_id: u64,
        local_node_id: u64,
        version: u64,
    ) !void {
        _ = self;
        var key_buf: [192]u8 = undefined;
        const key = try placementVersionKeyForGroup(&key_buf, metadata_group_id, range_group_id, local_node_id);
        var encoded: [@sizeOf(u64)]u8 = undefined;
        std.mem.writeInt(u64, &encoded, version, .little);
        try txn.put(key, &encoded);
    }

    fn advancePlacementVersionFenceTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        metadata_group_id: u64,
        range_group_id: u64,
        local_node_id: u64,
        observed_version: u64,
    ) !u64 {
        const current = try self.loadPlacementVersionFenceTxn(txn, metadata_group_id, range_group_id, local_node_id);
        const basis = @max(current, observed_version);
        if (basis == std.math.maxInt(u64)) return error.PlacementVersionExhausted;
        const next = basis + 1;
        try self.writePlacementVersionFenceTxn(txn, metadata_group_id, range_group_id, local_node_id, next);
        return next;
    }

    fn placementTargetDrainRequestedTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        metadata_group_id: u64,
        intent: raft_reconciler.PlacementIntent,
    ) !?bool {
        const node = (try self.loadNodeRecordTxn(txn, metadata_group_id, intent.record.local_node_id)) orelse return null;
        defer metadata_table_manager.freeNode(self.alloc, node);
        var drain_requested = !metadata_table_manager.nodeLifecycleActive(node.lifecycle);
        if (intent.store_id != 0) {
            const store = (try self.loadStoreRecordTxn(txn, metadata_group_id, intent.store_id)) orelse return null;
            defer metadata_table_manager.freeStore(self.alloc, store);
            if (store.node_id != intent.record.local_node_id) return null;
            drain_requested = drain_requested or store.drain_requested;
        }
        return drain_requested;
    }

    fn applyPlacementCompareAndUpsertTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        metadata_group_id: u64,
        expected_metadata_version: ?u64,
        expected_version_fence: u64,
        expected_target_drain_requested: bool,
        replacement: raft_reconciler.PlacementIntent,
    ) !void {
        const target_drain_requested = (try self.placementTargetDrainRequestedTxn(
            txn,
            metadata_group_id,
            replacement,
        )) orelse return;
        if (target_drain_requested != expected_target_drain_requested) return;
        if (target_drain_requested and
            replacement.serving_state != .draining and
            replacement.serving_state != .retiring)
        {
            return;
        }

        var key_buf: [192]u8 = undefined;
        const key = try placementKeyForGroup(
            &key_buf,
            metadata_group_id,
            replacement.record.group_id,
            replacement.record.local_node_id,
        );
        const existing = try self.loadPlacementIntentTxn(txn, key);
        defer if (existing) |intent| raft_reconciler.freeIntentOwned(self.alloc, intent);
        const current_version_fence = try self.loadPlacementVersionFenceTxn(
            txn,
            metadata_group_id,
            replacement.record.group_id,
            replacement.record.local_node_id,
        );
        if (current_version_fence != expected_version_fence) return;
        if (expected_metadata_version) |expected| {
            if (existing == null or
                existing.?.record.metadata_version != expected or
                replacement.record.metadata_version != expected or
                expected != expected_version_fence)
            {
                return;
            }
        } else if (existing != null or replacement.record.metadata_version != 0) {
            return;
        }

        const next_version = try self.advancePlacementVersionFenceTxn(
            txn,
            metadata_group_id,
            replacement.record.group_id,
            replacement.record.local_node_id,
            if (existing) |intent| intent.record.metadata_version else 0,
        );
        var applied = replacement;
        applied.record.metadata_version = next_version;
        const value = try encodePlacementIntent(self.alloc, applied);
        defer self.alloc.free(value);
        try txn.put(key, value);
        try self.upsertProjectedPlacementIntent(metadata_group_id, applied);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = metadata_group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .placement_intent,
            .metadata_group_id = metadata_group_id,
            .group_id = applied.record.group_id,
            .node_id = applied.record.local_node_id,
        });
    }

    fn applyPlacementRemoveIfVersionTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        metadata_group_id: u64,
        range_group_id: u64,
        local_node_id: u64,
        expected_metadata_version: u64,
    ) !void {
        var key_buf: [192]u8 = undefined;
        const key = try placementKeyForGroup(&key_buf, metadata_group_id, range_group_id, local_node_id);
        const existing = (try self.loadPlacementIntentTxn(txn, key)) orelse return;
        defer raft_reconciler.freeIntentOwned(self.alloc, existing);
        if (existing.record.metadata_version != expected_metadata_version) return;
        _ = try self.advancePlacementVersionFenceTxn(
            txn,
            metadata_group_id,
            range_group_id,
            local_node_id,
            existing.record.metadata_version,
        );
        try txn.delete(key);
        self.removeProjectedPlacementIntent(metadata_group_id, range_group_id, local_node_id);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = metadata_group_id, .key = key });
        self.notifyProjectionListeners(.{
            .kind = .placement_intent,
            .metadata_group_id = metadata_group_id,
            .group_id = range_group_id,
            .node_id = local_node_id,
        });
    }

    fn loadStoreRecordTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, store_id: u64) !?metadata.StoreRecord {
        var key_buf: [160]u8 = undefined;
        const key = try storeKeyForGroup(&key_buf, group_id, store_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeStoreRecord(self.alloc, encoded);
    }

    fn notifyProjectionListeners(self: *RaftApplyStore, signal: ProjectionSignal) void {
        if (self.active_outcome) |outcome| {
            outcome.appendProjection(signal) catch |err| outcome.recordFailure(err);
            return;
        }
        for (self.projection_listeners.items) |listener| listener.onProjectionSignal(signal);
    }

    fn notifyCommittedKeyListeners(self: *RaftApplyStore, signal: CommittedKeySignal) void {
        if (self.active_outcome) |outcome| {
            outcome.appendCommittedKey(signal) catch |err| outcome.recordFailure(err);
            return;
        }
        for (self.committed_key_listeners.items) |listener| listener.onCommittedKey(signal);
    }

    fn notifyCommittedTransition(self: *RaftApplyStore, delta: CommittedTransitionDelta) void {
        const outcome = self.active_outcome orelse {
            std.debug.assert(false);
            return;
        };
        if (!outcome.collect_transition_deltas) return;
        outcome.appendTransition(delta) catch |err| outcome.recordFailure(err);
    }

    fn dispatchCommittedOutcome(self: *RaftApplyStore, outcome: *const CommittedApplyOutcome) void {
        std.debug.assert(outcome.failure == null);
        for (outcome.projection_signals.items) |owned| {
            for (self.projection_listeners.items) |listener| listener.onProjectionSignal(owned.signal);
        }
        for (outcome.committed_keys.items) |owned| {
            const signal = CommittedKeySignal{
                .metadata_group_id = owned.metadata_group_id,
                .key = owned.key,
            };
            for (self.committed_key_listeners.items) |listener| listener.onCommittedKey(signal);
        }
    }

    fn lookupTableName(self: *RaftApplyStore, group_id: u64, table_id: u64) !?[]u8 {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, table_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        const table = try decodeTableRecord(self.alloc, encoded);
        defer metadata_table_manager.freeTable(self.alloc, table);
        return try self.alloc.dupe(u8, table.name);
    }

    fn lookupTableNameTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, table_id: u64) !?[]u8 {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, table_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const table = try decodeTableRecord(self.alloc, encoded);
        defer metadata_table_manager.freeTable(self.alloc, table);
        return try self.alloc.dupe(u8, table.name);
    }
};

fn replicationCutoverAuthorityIdentityEqual(
    a: metadata.ReplicationSourceStatusRecord,
    b: metadata.ReplicationSourceStatusRecord,
) bool {
    return a.table_id == b.table_id and
        a.source_ordinal == b.source_ordinal and
        a.cutover_intent_id == b.cutover_intent_id and
        a.cutover_authority_id == b.cutover_authority_id and
        std.mem.eql(u8, &a.cutover_config_fingerprint, &b.cutover_config_fingerprint) and
        std.mem.eql(u8, &a.cutover_provider_identity, &b.cutover_provider_identity) and
        std.mem.eql(u8, a.source_kind, b.source_kind) and
        std.mem.eql(u8, a.external_table, b.external_table) and
        std.mem.eql(u8, a.slot_name, b.slot_name) and
        std.mem.eql(u8, a.publication_name, b.publication_name);
}

fn replicationCutoverRetirementEqual(
    a: metadata.ReplicationSourceStatusRecord,
    b: metadata.ReplicationSourceStatusRecord,
) bool {
    return a.retired_cutover_authority_id == b.retired_cutover_authority_id and
        std.mem.eql(u8, a.retired_slot_name, b.retired_slot_name) and
        std.mem.eql(
            u8,
            a.retired_publication_name,
            b.retired_publication_name,
        );
}

fn replicationCutoverRetirementCleared(
    record: metadata.ReplicationSourceStatusRecord,
) bool {
    return record.retired_cutover_authority_id == 0 and
        record.retired_slot_name.len == 0 and
        record.retired_publication_name.len == 0;
}

const transition_magic = "afmd1";
const runtime_status_record_version: u16 = 12;
const group_status_record_version: u16 = 4;
// Store records predate framing and are read by mixed-version metadata
// replicas. Keep the existing prefix byte-for-byte compatible and put new
// optional state in a self-identifying trailer that older readers ignore.
const store_record_extension_magic = "afsx1";
const store_record_extension_version: u16 = 1;
const reallocation_request_extension_magic = "afrr1";
const reallocation_request_extension_version: u16 = 1;

const TransitionTag = enum(u8) {
    initialize_metadata_incarnation = 45,
    upsert_node = 1,
    remove_node = 2,
    upsert_store = 3,
    remove_store = 4,
    upsert_replica_intent = 5,
    remove_replica_intent = 6,
    upsert_table = 7,
    remove_table = 8,
    upsert_schema_progress = 9,
    remove_schema_progress = 10,
    upsert_restore_progress = 11,
    remove_restore_progress = 12,
    upsert_replication_source_status = 13,
    upsert_range = 15,
    remove_range = 16,
    upsert_split_transition = 17,
    remove_split_transition = 18,
    upsert_merge_transition = 19,
    remove_merge_transition = 20,
    upsert_reconcile_lease = 21,
    remove_reconcile_lease = 22,
    upsert_shuffle_join_lease = 23,
    remove_shuffle_join_lease = 24,
    upsert_reallocation_request = 25,
    remove_reallocation_request = 26,
    request_node_shutdown = 27,
    cancel_node_shutdown = 28,
    register_node = 29,
    register_store = 30,
    finalize_node_shutdown = 31,
    upsert_extension_package = 32,
    remove_extension_package = 33,
    upsert_installed_extension = 34,
    remove_installed_extension = 35,
    upsert_extension_member = 36,
    remove_extension_member = 37,
    upsert_extension_dependency = 38,
    remove_extension_dependency = 39,
    apply_extension_lifecycle = 40,
    upsert_restore_job = 41,
    remove_restore_job = 42,
    remove_restore_jobs = 43,
    admit_split_transition = 44,
    complete_restore_range = 46,
    compare_and_replace_table = 47,
    claim_replication_source_cutover = 48,
    complete_replication_source_retirement = 49,
};

pub fn encodeTransitionCommand(alloc: std.mem.Allocator, command: TransitionCommand) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, transition_magic);
    switch (command) {
        .initialize_metadata_incarnation => |incarnation| {
            try out.append(alloc, @intFromEnum(TransitionTag.initialize_metadata_incarnation));
            try out.appendSlice(alloc, &incarnation);
        },
        .upsert_node => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_node));
            try appendNodeRecord(alloc, &out, record);
        },
        .register_node => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.register_node));
            try appendNodeRecord(alloc, &out, record);
        },
        .remove_node => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_node));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .request_node_shutdown => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.request_node_shutdown));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .cancel_node_shutdown => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.cancel_node_shutdown));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .finalize_node_shutdown => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.finalize_node_shutdown));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .upsert_store => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_store));
            try appendStoreRecord(alloc, &out, record);
        },
        .register_store => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.register_store));
            try appendStoreRecord(alloc, &out, record);
        },
        .remove_store => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_store));
            try appendInt(alloc, &out, u64, record.store_id);
        },
        .upsert_replica_intent => |replacement| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_replica_intent));
            try out.append(alloc, @intFromBool(replacement.expected_metadata_version != null));
            if (replacement.expected_metadata_version) |version| try appendInt(alloc, &out, u64, version);
            try appendInt(alloc, &out, u64, replacement.expected_version_fence);
            try out.append(alloc, @intFromBool(replacement.expected_target_drain_requested));
            try appendPlacementIntent(alloc, &out, replacement.replacement);
        },
        .remove_replica_intent => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_replica_intent));
            try appendInt(alloc, &out, u64, record.group_id);
            try appendInt(alloc, &out, u64, record.local_node_id);
            try appendInt(alloc, &out, u64, record.expected_metadata_version);
        },
        .upsert_table => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_table));
            try appendTableRecord(alloc, &out, record);
        },
        .compare_and_replace_table => |replacement| {
            try out.append(alloc, @intFromEnum(TransitionTag.compare_and_replace_table));
            try appendFramedTableRecord(alloc, &out, replacement.expected);
            try appendFramedTableRecord(alloc, &out, replacement.replacement);
        },
        .remove_table => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_table));
            try appendInt(alloc, &out, u64, record.table_id);
            try appendInt(
                alloc,
                &out,
                u64,
                record.expected_transition_generation,
            );
        },
        .upsert_schema_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_schema_progress));
            try appendSchemaProgressRecord(alloc, &out, record);
        },
        .remove_schema_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_schema_progress));
            try appendInt(alloc, &out, u64, record.table_id);
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .upsert_restore_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_restore_progress));
            try appendRestoreProgressRecord(alloc, &out, record);
        },
        .remove_restore_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_restore_progress));
            try appendInt(alloc, &out, u64, record.table_id);
            try appendInt(alloc, &out, u64, record.node_id);
            try appendInt(alloc, &out, u64, record.group_id);
        },
        .upsert_replication_source_status => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_replication_source_status));
            try appendReplicationSourceStatusRecord(alloc, &out, record);
        },
        .claim_replication_source_cutover => |claim| {
            try out.append(alloc, @intFromEnum(TransitionTag.claim_replication_source_cutover));
            try appendRequiredString(alloc, &out, claim.expected_replication_sources_json);
            try appendInt(alloc, &out, u64, claim.expected_authority_id);
            try appendReplicationSourceStatusRecord(alloc, &out, claim.record);
        },
        .complete_replication_source_retirement => |record| {
            try out.append(
                alloc,
                @intFromEnum(
                    TransitionTag.complete_replication_source_retirement,
                ),
            );
            try appendReplicationSourceStatusRecord(alloc, &out, record);
        },
        .upsert_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_range));
            try appendRangeRecord(alloc, &out, record);
        },
        .complete_restore_range => |identity| {
            try out.append(alloc, @intFromEnum(TransitionTag.complete_restore_range));
            try appendRestoreIntentIdentity(alloc, &out, identity);
        },
        .remove_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_range));
            try appendInt(alloc, &out, u64, record.group_id);
        },
        .admit_split_transition => |admission| {
            try out.append(alloc, @intFromEnum(TransitionTag.admit_split_transition));
            try appendInt(alloc, &out, u64, admission.expected_source_epoch);
            try appendSplitTransitionRecord(alloc, &out, admission.record);
        },
        .upsert_split_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_split_transition));
            try appendSplitTransitionRecord(alloc, &out, record);
        },
        .remove_split_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_split_transition));
            try appendInt(alloc, &out, u64, record.transition_id);
        },
        .upsert_merge_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_merge_transition));
            try appendMergeTransitionRecord(alloc, &out, record);
        },
        .remove_merge_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_merge_transition));
            try appendInt(alloc, &out, u64, record.transition_id);
        },
        .upsert_reconcile_lease => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_reconcile_lease));
            try appendReconcileLeaseRecord(alloc, &out, record);
        },
        .remove_reconcile_lease => {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_reconcile_lease));
        },
        .upsert_shuffle_join_lease => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_shuffle_join_lease));
            try appendShuffleJoinLeaseRecord(alloc, &out, record);
        },
        .remove_shuffle_join_lease => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_shuffle_join_lease));
            try appendInt(alloc, &out, u64, record.job_id);
        },
        .upsert_reallocation_request => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_reallocation_request));
            try appendReallocationRequestRecord(alloc, &out, record);
        },
        .remove_reallocation_request => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_reallocation_request));
            try appendInt(alloc, &out, u128, record.expected_request_id);
        },
        .upsert_extension_package => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_extension_package));
            try appendJsonRecord(alloc, &out, record);
        },
        .remove_extension_package => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_extension_package));
            try appendRequiredString(alloc, &out, record.name);
            try appendRequiredString(alloc, &out, record.version);
        },
        .upsert_installed_extension => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_installed_extension));
            try appendJsonRecord(alloc, &out, record);
        },
        .remove_installed_extension => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_installed_extension));
            try appendRequiredString(alloc, &out, record.name);
        },
        .upsert_extension_member => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_extension_member));
            try appendJsonRecord(alloc, &out, record);
        },
        .remove_extension_member => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_extension_member));
            try appendRequiredString(alloc, &out, record.extension_name);
            try appendRequiredString(alloc, &out, @tagName(record.object_kind));
            try appendRequiredString(alloc, &out, record.object_name);
        },
        .upsert_extension_dependency => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_extension_dependency));
            try appendJsonRecord(alloc, &out, record);
        },
        .remove_extension_dependency => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_extension_dependency));
            try appendRequiredString(alloc, &out, record.extension_name);
            try appendRequiredString(alloc, &out, record.required_extension_name);
            try appendRequiredString(alloc, &out, record.package_name);
        },
        .apply_extension_lifecycle => |delta| {
            try out.append(alloc, @intFromEnum(TransitionTag.apply_extension_lifecycle));
            try appendJsonRecord(alloc, &out, delta);
        },
        .upsert_restore_job => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_restore_job));
            try appendRequiredString(alloc, &out, record.key);
            try appendRequiredString(alloc, &out, record.value);
        },
        .remove_restore_job => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_restore_job));
            try appendRequiredString(alloc, &out, record.key);
        },
        .remove_restore_jobs => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_restore_jobs));
            try appendInt(alloc, &out, u32, @intCast(record.keys.len));
            for (record.keys) |key| try appendRequiredString(alloc, &out, key);
        },
    }
    return try out.toOwnedSlice(alloc);
}

pub fn decodeTransitionCommand(alloc: std.mem.Allocator, encoded: []const u8) !?TransitionCommand {
    if (encoded.len < transition_magic.len + 1) return null;
    if (!std.mem.eql(u8, encoded[0..transition_magic.len], transition_magic)) return null;

    var pos: usize = transition_magic.len;
    const tag = std.enums.fromInt(TransitionTag, encoded[pos]) orelse
        return error.InvalidMetadataTransitionEncoding;
    pos += 1;

    return switch (tag) {
        .initialize_metadata_incarnation => blk: {
            if (pos + @sizeOf(metadata_incarnation.MetadataClusterIncarnation) != encoded.len) {
                return error.InvalidMetadataTransitionEncoding;
            }
            var incarnation: metadata_incarnation.MetadataClusterIncarnation = undefined;
            @memcpy(&incarnation, encoded[pos..][0..incarnation.len]);
            break :blk .{ .initialize_metadata_incarnation = incarnation };
        },
        .upsert_node => .{
            .upsert_node = try readNodeRecord(alloc, encoded, &pos),
        },
        .register_node => .{
            .register_node = try readNodeRecord(alloc, encoded, &pos),
        },
        .remove_node => .{
            .remove_node = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .request_node_shutdown => .{
            .request_node_shutdown = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .cancel_node_shutdown => .{
            .cancel_node_shutdown = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .finalize_node_shutdown => .{
            .finalize_node_shutdown = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_store => .{
            .upsert_store = try readStoreRecord(alloc, encoded, &pos),
        },
        .register_store => .{
            .register_store = try readStoreRecord(alloc, encoded, &pos),
        },
        .remove_store => .{
            .remove_store = .{ .store_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_replica_intent => blk: {
            const has_expected = try readInt(encoded, &pos, u8);
            if (has_expected > 1) return error.InvalidMetadataTransitionEncoding;
            const expected_metadata_version = if (has_expected == 1)
                try readInt(encoded, &pos, u64)
            else
                null;
            const expected_version_fence = try readInt(encoded, &pos, u64);
            const expected_target_drain_requested = try readInt(encoded, &pos, u8);
            if (expected_target_drain_requested > 1) return error.InvalidMetadataTransitionEncoding;
            break :blk .{ .upsert_replica_intent = .{
                .expected_metadata_version = expected_metadata_version,
                .expected_version_fence = expected_version_fence,
                .expected_target_drain_requested = expected_target_drain_requested == 1,
                .replacement = try readPlacementIntent(alloc, encoded, &pos),
            } };
        },
        .remove_replica_intent => .{
            .remove_replica_intent = .{
                .group_id = try readInt(encoded, &pos, u64),
                .local_node_id = try readInt(encoded, &pos, u64),
                .expected_metadata_version = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_table => .{
            .upsert_table = try readTableRecord(alloc, encoded, &pos),
        },
        .compare_and_replace_table => blk: {
            const expected = try readFramedTableRecord(alloc, encoded, &pos);
            errdefer metadata_table_manager.freeTable(alloc, expected);
            const replacement = try readFramedTableRecord(alloc, encoded, &pos);
            break :blk .{ .compare_and_replace_table = .{
                .expected = expected,
                .replacement = replacement,
            } };
        },
        .remove_table => .{
            .remove_table = .{
                .table_id = try readInt(encoded, &pos, u64),
                .expected_transition_generation = try readInt(
                    encoded,
                    &pos,
                    u64,
                ),
            },
        },
        .upsert_schema_progress => .{
            .upsert_schema_progress = try readSchemaProgressRecord(encoded, &pos),
        },
        .remove_schema_progress => .{
            .remove_schema_progress = .{
                .table_id = try readInt(encoded, &pos, u64),
                .node_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_restore_progress => .{
            .upsert_restore_progress = try readRestoreProgressRecord(alloc, encoded, &pos),
        },
        .remove_restore_progress => .{
            .remove_restore_progress = .{
                .table_id = try readInt(encoded, &pos, u64),
                .node_id = try readInt(encoded, &pos, u64),
                .group_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_replication_source_status => .{
            .upsert_replication_source_status = try readReplicationSourceStatusRecord(alloc, encoded, &pos),
        },
        .claim_replication_source_cutover => blk: {
            const expected_replication_sources_json = try readRequiredString(alloc, encoded, &pos);
            errdefer alloc.free(expected_replication_sources_json);
            const expected_authority_id = try readInt(encoded, &pos, u64);
            const record = try readReplicationSourceStatusRecord(alloc, encoded, &pos);
            break :blk .{ .claim_replication_source_cutover = .{
                .expected_replication_sources_json = expected_replication_sources_json,
                .expected_authority_id = expected_authority_id,
                .record = record,
            } };
        },
        .complete_replication_source_retirement => .{
            .complete_replication_source_retirement = try readReplicationSourceStatusRecord(alloc, encoded, &pos),
        },
        .upsert_range => .{
            .upsert_range = try readRangeRecord(alloc, encoded, &pos),
        },
        .complete_restore_range => .{
            .complete_restore_range = try readRestoreIntentIdentity(alloc, encoded, &pos),
        },
        .remove_range => .{
            .remove_range = .{ .group_id = try readInt(encoded, &pos, u64) },
        },
        .admit_split_transition => .{
            .admit_split_transition = .{
                .expected_source_epoch = try readInt(encoded, &pos, u64),
                .record = try readSplitTransitionRecord(alloc, encoded, &pos),
            },
        },
        .upsert_split_transition => .{
            .upsert_split_transition = try readSplitTransitionRecord(alloc, encoded, &pos),
        },
        .remove_split_transition => .{
            .remove_split_transition = .{ .transition_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_merge_transition => .{
            .upsert_merge_transition = try readMergeTransitionRecord(alloc, encoded, &pos),
        },
        .remove_merge_transition => .{
            .remove_merge_transition = .{ .transition_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_reconcile_lease => .{
            .upsert_reconcile_lease = try readReconcileLeaseRecord(encoded, &pos),
        },
        .remove_reconcile_lease => .{
            .remove_reconcile_lease = .{},
        },
        .upsert_shuffle_join_lease => .{
            .upsert_shuffle_join_lease = try readShuffleJoinLeaseRecord(encoded, &pos),
        },
        .remove_shuffle_join_lease => .{
            .remove_shuffle_join_lease = .{
                .job_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_reallocation_request => .{
            .upsert_reallocation_request = try readReallocationRequestRecord(encoded, &pos),
        },
        .remove_reallocation_request => .{
            .remove_reallocation_request = .{
                .expected_request_id = try readInt(encoded, &pos, u128),
            },
        },
        .upsert_extension_package => .{
            .upsert_extension_package = try readJsonRecord(extension_domain.PackageManifest, alloc, encoded, &pos),
        },
        .remove_extension_package => .{
            .remove_extension_package = .{
                .name = try readRequiredString(alloc, encoded, &pos),
                .version = try readRequiredString(alloc, encoded, &pos),
            },
        },
        .upsert_installed_extension => .{
            .upsert_installed_extension = try readJsonRecord(extension_domain.InstalledExtension, alloc, encoded, &pos),
        },
        .remove_installed_extension => .{
            .remove_installed_extension = .{
                .name = try readRequiredString(alloc, encoded, &pos),
            },
        },
        .upsert_extension_member => .{
            .upsert_extension_member = try readJsonRecord(extension_domain.ExtensionMember, alloc, encoded, &pos),
        },
        .remove_extension_member => blk: {
            const extension_name = try readRequiredString(alloc, encoded, &pos);
            errdefer alloc.free(extension_name);
            const kind_name = try readRequiredString(alloc, encoded, &pos);
            defer alloc.free(kind_name);
            const object_kind = std.meta.stringToEnum(extension_domain.ExtensionObjectKind, kind_name) orelse return error.InvalidMetadataTransitionEncoding;
            const object_name = try readRequiredString(alloc, encoded, &pos);
            break :blk .{
                .remove_extension_member = .{
                    .extension_name = extension_name,
                    .object_kind = object_kind,
                    .object_name = object_name,
                },
            };
        },
        .upsert_extension_dependency => .{
            .upsert_extension_dependency = try readJsonRecord(extension_domain.ExtensionDependency, alloc, encoded, &pos),
        },
        .remove_extension_dependency => .{
            .remove_extension_dependency = .{
                .extension_name = try readRequiredString(alloc, encoded, &pos),
                .required_extension_name = try readRequiredString(alloc, encoded, &pos),
                .package_name = try readRequiredString(alloc, encoded, &pos),
            },
        },
        .apply_extension_lifecycle => .{
            .apply_extension_lifecycle = try readJsonRecord(ExtensionLifecycleDelta, alloc, encoded, &pos),
        },
        .upsert_restore_job => .{
            .upsert_restore_job = .{
                .key = try readRequiredString(alloc, encoded, &pos),
                .value = try readRequiredString(alloc, encoded, &pos),
            },
        },
        .remove_restore_job => .{
            .remove_restore_job = .{ .key = try readRequiredString(alloc, encoded, &pos) },
        },
        .remove_restore_jobs => .{
            .remove_restore_jobs = .{ .keys = try readRequiredStrings(alloc, encoded, &pos, 4096) },
        },
    };
}

fn encodePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendPlacementIntent(alloc, &out, intent);
    return try out.toOwnedSlice(alloc);
}

fn encodeNodeRecord(alloc: std.mem.Allocator, record: metadata.NodeRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendNodeRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeStoreRecord(alloc: std.mem.Allocator, record: metadata.StoreRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendStoreRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeTableRecord(alloc: std.mem.Allocator, record: metadata.TableRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendTableRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeRangeRecord(alloc: std.mem.Allocator, record: metadata.RangeRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendRangeRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeSchemaProgressRecord(alloc: std.mem.Allocator, record: metadata.SchemaProgressRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendSchemaProgressRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeRestoreProgressRecord(alloc: std.mem.Allocator, record: metadata.RestoreProgressRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendRestoreProgressRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeReplicationSourceStatusRecord(alloc: std.mem.Allocator, record: metadata.ReplicationSourceStatusRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendReplicationSourceStatusRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeReconcileLeaseRecord(alloc: std.mem.Allocator, record: metadata.ReconcileLeaseRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendReconcileLeaseRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeShuffleJoinLeaseRecord(alloc: std.mem.Allocator, record: metadata.ShuffleJoinLeaseRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendShuffleJoinLeaseRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeReallocationRequestRecord(alloc: std.mem.Allocator, record: metadata.ReallocationRequestRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendReallocationRequestRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeExtensionPackageRecord(alloc: std.mem.Allocator, record: extension_domain.PackageManifest) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(record, .{})});
}

fn encodeInstalledExtensionRecord(alloc: std.mem.Allocator, record: extension_domain.InstalledExtension) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(record, .{})});
}

fn encodeExtensionMemberRecord(alloc: std.mem.Allocator, record: extension_domain.ExtensionMember) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(record, .{})});
}

fn encodeExtensionDependencyRecord(alloc: std.mem.Allocator, record: extension_domain.ExtensionDependency) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(record, .{})});
}

fn encodeSplitTransitionRecord(alloc: std.mem.Allocator, record: metadata.SplitTransitionRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendSplitTransitionRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeMergeTransitionRecord(alloc: std.mem.Allocator, record: metadata.MergeTransitionRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendMergeTransitionRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn decodeSplitTransitionRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.SplitTransitionRecord {
    var pos: usize = 0;
    return try readSplitTransitionRecord(alloc, encoded, &pos);
}

fn decodeMergeTransitionRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.MergeTransitionRecord {
    var pos: usize = 0;
    return try readMergeTransitionRecord(alloc, encoded, &pos);
}

fn decodeTableRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.TableRecord {
    var pos: usize = 0;
    return try readTableRecord(alloc, encoded, &pos);
}

fn decodeRangeRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.RangeRecord {
    var pos: usize = 0;
    return try readRangeRecord(alloc, encoded, &pos);
}

fn decodeSchemaProgressRecord(encoded: []const u8) !metadata.SchemaProgressRecord {
    var pos: usize = 0;
    return try readSchemaProgressRecord(encoded, &pos);
}

fn decodeRestoreProgressRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.RestoreProgressRecord {
    var pos: usize = 0;
    return try readRestoreProgressRecord(alloc, encoded, &pos);
}

fn decodeReplicationSourceStatusRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.ReplicationSourceStatusRecord {
    var pos: usize = 0;
    return try readReplicationSourceStatusRecord(alloc, encoded, &pos);
}

fn decodeExtensionPackageRecord(alloc: std.mem.Allocator, encoded: []const u8) !extension_domain.PackageManifest {
    var value = try std.json.parseFromSliceLeaky(extension_domain.PackageManifest, alloc, encoded, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer value.deinitOwned(alloc);
    try value.validate();
    return value;
}

fn decodeInstalledExtensionRecord(alloc: std.mem.Allocator, encoded: []const u8) !extension_domain.InstalledExtension {
    var value = try std.json.parseFromSliceLeaky(extension_domain.InstalledExtension, alloc, encoded, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer value.deinitOwned(alloc);
    try value.validate();
    return value;
}

fn decodeExtensionMemberRecord(alloc: std.mem.Allocator, encoded: []const u8) !extension_domain.ExtensionMember {
    var value = try std.json.parseFromSliceLeaky(extension_domain.ExtensionMember, alloc, encoded, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer value.deinitOwned(alloc);
    try value.validate();
    return value;
}

fn decodeExtensionDependencyRecord(alloc: std.mem.Allocator, encoded: []const u8) !extension_domain.ExtensionDependency {
    var value = try std.json.parseFromSliceLeaky(extension_domain.ExtensionDependency, alloc, encoded, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer value.deinitOwned(alloc);
    try value.validate();
    return value;
}

fn decodePlacementIntent(alloc: std.mem.Allocator, encoded: []const u8) !raft_reconciler.PlacementIntent {
    var pos: usize = 0;
    return try readPlacementIntent(alloc, encoded, &pos);
}

fn clonePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) !raft_reconciler.PlacementIntent {
    return try raft_reconciler.cloneIntentOwned(alloc, intent);
}

fn freePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) void {
    raft_reconciler.freeIntentOwned(alloc, intent);
}

fn decodeNodeRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.NodeRecord {
    var pos: usize = 0;
    return try readNodeRecord(alloc, encoded, &pos);
}

fn decodeStoreRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.StoreRecord {
    var pos: usize = 0;
    return try readStoreRecord(alloc, encoded, &pos);
}

fn appendNodeRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.NodeRecord,
) !void {
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u32, @intCast(record.role.len));
    try out.appendSlice(alloc, record.role);
    try appendInt(alloc, out, u32, @intCast(record.lifecycle.len));
    try out.appendSlice(alloc, record.lifecycle);
}

fn readNodeRecord(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !metadata.NodeRecord {
    const node_id = try readInt(encoded, pos, u64);
    const role_len = try readInt(encoded, pos, u32);
    if (pos.* + role_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const role = try alloc.dupe(u8, encoded[pos.* .. pos.* + role_len]);
    errdefer alloc.free(role);
    pos.* += role_len;
    const lifecycle = if (pos.* < encoded.len) blk: {
        const lifecycle_len = try readInt(encoded, pos, u32);
        if (pos.* + lifecycle_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
        const value = try alloc.dupe(u8, encoded[pos.* .. pos.* + lifecycle_len]);
        pos.* += lifecycle_len;
        break :blk value;
    } else try alloc.dupe(u8, metadata_table_manager.node_lifecycle_active);
    return .{
        .node_id = node_id,
        .role = role,
        .lifecycle = lifecycle,
    };
}

fn appendStoreRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.StoreRecord,
) !void {
    try appendInt(alloc, out, u64, record.store_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u32, @intCast(record.api_url.len));
    try out.appendSlice(alloc, record.api_url);
    try appendInt(alloc, out, u32, @intCast(record.raft_url.len));
    try out.appendSlice(alloc, record.raft_url);
    try appendInt(alloc, out, u32, @intCast(record.role.len));
    try out.appendSlice(alloc, record.role);
    try appendInt(alloc, out, u32, @intCast(record.health_class.len));
    try out.appendSlice(alloc, record.health_class);
    try appendInt(alloc, out, u32, @intCast(record.failure_domain.len));
    try out.appendSlice(alloc, record.failure_domain);
    try out.append(alloc, if (record.live) 1 else 0);
    try appendInt(alloc, out, u64, record.capacity_bytes);
    try appendInt(alloc, out, u64, record.available_bytes);
    try appendInt(alloc, out, u32, record.lease_pressure);
    try appendInt(alloc, out, u32, record.read_load);
    try appendInt(alloc, out, u32, record.write_load);
    try appendInt(alloc, out, u32, record.active_backfills);
    try appendInt(alloc, out, u16, record.backfill_progress_millis);
    try appendInt(alloc, out, u32, @intCast(record.group_statuses.len));
    for (record.group_statuses) |group_status| try appendGroupStatusRecord(alloc, out, group_status);
    try appendInt(alloc, out, u32, @intCast(record.runtime_statuses.len));
    for (record.runtime_statuses) |runtime_status| try appendRuntimeGroupStatusRecord(alloc, out, runtime_status);
    try out.append(alloc, if (record.drain_requested) 1 else 0);
    try appendStoreRecordExtensions(alloc, out, record);
}

fn appendStoreRecordExtensions(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.StoreRecord,
) !void {
    var observation_count: u32 = 0;
    for (record.group_statuses) |status| {
        if (status.observed_reallocation_request_id != 0) observation_count += 1;
    }
    if (observation_count == 0) return;

    try out.appendSlice(alloc, store_record_extension_magic);
    try appendInt(alloc, out, u16, store_record_extension_version);
    try appendInt(alloc, out, u32, observation_count);
    for (record.group_statuses, 0..) |status, status_index| {
        if (status.observed_reallocation_request_id == 0) continue;
        // Group reports are intentionally permitted to contain duplicate group
        // IDs so reconciliation can treat the report as ambiguous and fail
        // closed. Address the extension by position so that ambiguity remains
        // representable in the committed command instead of making apply fail.
        try appendInt(alloc, out, u32, @intCast(status_index));
        try appendInt(alloc, out, u128, status.observed_reallocation_request_id);
    }
}

fn readStoreRecord(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !metadata.StoreRecord {
    const store_id = try readInt(encoded, pos, u64);
    const node_id = try readInt(encoded, pos, u64);
    const api_url_len = try readInt(encoded, pos, u32);
    if (pos.* + api_url_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const api_url = try alloc.dupe(u8, encoded[pos.* .. pos.* + api_url_len]);
    errdefer alloc.free(api_url);
    pos.* += api_url_len;
    const raft_url_len = try readInt(encoded, pos, u32);
    if (pos.* + raft_url_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const raft_url = try alloc.dupe(u8, encoded[pos.* .. pos.* + raft_url_len]);
    errdefer alloc.free(raft_url);
    pos.* += raft_url_len;
    const role_len = try readInt(encoded, pos, u32);
    if (pos.* + role_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const role = try alloc.dupe(u8, encoded[pos.* .. pos.* + role_len]);
    errdefer alloc.free(role);
    pos.* += role_len;
    const health_class_len = try readInt(encoded, pos, u32);
    if (pos.* + health_class_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const health_class = try alloc.dupe(u8, encoded[pos.* .. pos.* + health_class_len]);
    errdefer alloc.free(health_class);
    pos.* += health_class_len;
    const failure_domain_len = try readInt(encoded, pos, u32);
    if (pos.* + failure_domain_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const failure_domain = try alloc.dupe(u8, encoded[pos.* .. pos.* + failure_domain_len]);
    errdefer alloc.free(failure_domain);
    pos.* += failure_domain_len;
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const live = encoded[pos.*] != 0;
    pos.* += 1;
    const capacity_bytes = try readInt(encoded, pos, u64);
    const available_bytes = try readInt(encoded, pos, u64);
    const lease_pressure = try readInt(encoded, pos, u32);
    const read_load = try readInt(encoded, pos, u32);
    const write_load = try readInt(encoded, pos, u32);
    const active_backfills = if (pos.* < encoded.len) try readInt(encoded, pos, u32) else 0;
    const backfill_progress_millis = if (pos.* < encoded.len) try readInt(encoded, pos, u16) else 1000;
    const group_status_count = if (pos.* < encoded.len) try readInt(encoded, pos, u32) else 0;
    const group_statuses = try alloc.alloc(metadata.GroupStatusReport, group_status_count);
    var initialized: usize = 0;
    errdefer {
        for (group_statuses[0..initialized]) |record| metadata_table_manager.freeGroupStatus(alloc, record);
        if (group_statuses.len > 0) alloc.free(group_statuses);
    }
    while (initialized < group_status_count) : (initialized += 1) {
        group_statuses[initialized] = try readGroupStatusRecord(alloc, encoded, pos);
    }
    const runtime_status_count = if (pos.* < encoded.len) try readInt(encoded, pos, u32) else 0;
    const runtime_statuses = try alloc.alloc(metadata.RuntimeGroupStatusReport, runtime_status_count);
    var initialized_runtime_statuses: usize = 0;
    errdefer {
        for (runtime_statuses[0..initialized_runtime_statuses]) |record| metadata_table_manager.freeRuntimeGroupStatusReport(alloc, record);
        if (runtime_statuses.len > 0) alloc.free(runtime_statuses);
    }
    while (initialized_runtime_statuses < runtime_status_count) : (initialized_runtime_statuses += 1) {
        runtime_statuses[initialized_runtime_statuses] = try readRuntimeGroupStatusRecord(alloc, encoded, pos);
    }
    const drain_requested = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    try readStoreRecordExtensions(encoded, pos, group_statuses);
    return .{
        .store_id = store_id,
        .node_id = node_id,
        .api_url = api_url,
        .raft_url = raft_url,
        .role = role,
        .health_class = health_class,
        .failure_domain = failure_domain,
        .live = live,
        .drain_requested = drain_requested,
        .capacity_bytes = capacity_bytes,
        .available_bytes = available_bytes,
        .lease_pressure = lease_pressure,
        .read_load = read_load,
        .write_load = write_load,
        .active_backfills = active_backfills,
        .backfill_progress_millis = backfill_progress_millis,
        .group_statuses = group_statuses,
        .runtime_statuses = runtime_statuses,
    };
}

fn readStoreRecordExtensions(
    encoded: []const u8,
    pos: *usize,
    group_statuses: []metadata.GroupStatusReport,
) !void {
    if (pos.* == encoded.len) return;
    if (pos.* + store_record_extension_magic.len > encoded.len or
        !std.mem.eql(
            u8,
            encoded[pos.* .. pos.* + store_record_extension_magic.len],
            store_record_extension_magic,
        ))
    {
        return error.InvalidMetadataTransitionEncoding;
    }
    pos.* += store_record_extension_magic.len;

    const version = try readInt(encoded, pos, u16);
    if (version != store_record_extension_version) return error.InvalidMetadataTransitionEncoding;
    const observation_count = try readInt(encoded, pos, u32);
    var observation_index: u32 = 0;
    while (observation_index < observation_count) : (observation_index += 1) {
        const status_index = try readInt(encoded, pos, u32);
        const request_id = try readInt(encoded, pos, u128);
        if (request_id == 0 or status_index >= group_statuses.len) return error.InvalidMetadataTransitionEncoding;
        const status = &group_statuses[status_index];
        if (status.observed_reallocation_request_id != 0) return error.InvalidMetadataTransitionEncoding;
        status.observed_reallocation_request_id = request_id;
    }
    if (pos.* != encoded.len) return error.InvalidMetadataTransitionEncoding;
}

fn appendRuntimeGroupStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeGroupStatusReport,
) !void {
    try appendInt(alloc, out, u16, runtime_status_record_version);
    try appendInt(alloc, out, u64, record.table_id);
    try appendRequiredString(alloc, out, record.table_name);
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.store_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u64, record.updated_at_ns);
    try appendRequiredString(alloc, out, record.source);
    try appendRequiredString(alloc, out, record.freshness);
    try appendInt(alloc, out, u64, record.topology_generation);
    try appendInt(alloc, out, u64, record.lsm_root_generation);
    try appendInt(alloc, out, u64, record.status_generation);
    try appendInt(alloc, out, u64, record.doc_count);
    try appendInt(alloc, out, u64, record.disk_bytes);
    try out.append(alloc, if (record.disk_bytes_known) 1 else 0);
    try appendInt(alloc, out, u64, record.created_at_millis);
    try appendInt(alloc, out, u32, record.index_count);
    try appendRuntimeEnrichmentStatusRecord(alloc, out, record.enrichment);
    try out.append(alloc, if (record.async_indexing_active) 1 else 0);
    try out.append(alloc, if (record.async_startup_active) 1 else 0);
    try out.append(alloc, if (record.async_dense_catch_up_active) 1 else 0);
    try out.append(alloc, if (record.async_bulk_coalescing_active) 1 else 0);
    try appendRuntimeDocIdentityStatusRecord(alloc, out, record.doc_identity);
    try appendRuntimeDocSetPlanningStatusRecord(alloc, out, record.doc_set_planning);
    try appendInt(alloc, out, u32, @intCast(record.indexes.len));
    for (record.indexes) |index| try appendRuntimeIndexStatusRecord(alloc, out, index);
}

fn appendRuntimeEnrichmentStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeEnrichmentStatusReport,
) !void {
    try out.append(alloc, if (record.enabled) 1 else 0);
    try out.append(alloc, if (record.lease_owned) 1 else 0);
    try out.append(alloc, if (record.has_lease) 1 else 0);
    try appendInt(alloc, out, u64, record.acquisition_count);
    try appendInt(alloc, out, u64, record.lease_acquire_failures);
    try appendInt(alloc, out, u64, record.lost_leases);
    try appendInt(alloc, out, u64, record.last_acquired_ms);
    try appendInt(alloc, out, u64, record.target_sequence);
    try appendInt(alloc, out, u64, record.applied_sequence);
    try appendRequiredString(alloc, out, record.projection_checkpoint_status);
    try appendInt(alloc, out, u64, record.projection_checkpoint_applied_sequence);
    try appendInt(alloc, out, u64, record.projection_checkpoint_generation);
    try appendInt(alloc, out, u64, record.projection_checkpoint_config_hash);
    try appendInt(alloc, out, u64, record.checkpoint_replay_tail_sequence_count);
    try appendInt(alloc, out, u64, record.processed_requests);
    try appendInt(alloc, out, u64, record.error_count);
    try appendInt(alloc, out, u64, record.retryable_error_count);
    try appendInt(alloc, out, u64, record.fatal_error_count);
    try appendInt(alloc, out, u32, record.consecutive_retry_count);
    try appendInt(alloc, out, u64, record.next_retry_at_ms);
    try out.append(alloc, if (record.retrying) 1 else 0);
    try out.append(alloc, if (record.worker_failed) 1 else 0);
    try out.append(alloc, if (record.worker_started) 1 else 0);
    try out.append(alloc, if (record.stalled) 1 else 0);
    try appendInt(alloc, out, u64, record.skip_by_hash_count);
    try appendInt(alloc, out, u64, record.skipped_source_count);
    try appendInt(alloc, out, u64, record.codec_decode_failures);
    try appendInt(alloc, out, u64, record.embed_batches_started);
    try appendInt(alloc, out, u64, record.embed_batches_completed);
    try appendInt(alloc, out, u64, record.embed_items_started);
    try appendInt(alloc, out, u64, record.embed_items_completed);
    try appendInt(alloc, out, u64, record.active_embed_batch_items);
    try appendInt(alloc, out, u64, record.active_embed_batch_bytes);
    try appendInt(alloc, out, u64, record.active_embed_batch_max_bytes);
    try appendInt(alloc, out, u64, record.active_embed_batch_started_ms);
    try appendInt(alloc, out, u64, record.last_embed_batch_items);
    try appendInt(alloc, out, u64, record.last_embed_batch_bytes);
    try appendInt(alloc, out, u64, record.last_embed_batch_max_bytes);
    try appendInt(alloc, out, u64, record.last_embed_batch_completed_ms);
    try appendInt(alloc, out, u64, record.last_embed_batch_ns);
    try appendInt(alloc, out, u64, record.total_embed_ns);
    try appendInt(alloc, out, u64, record.dense_artifact_bytes_written);
    try appendInt(alloc, out, u64, record.sparse_artifact_bytes_written);
    try appendInt(alloc, out, u64, record.chunk_artifact_bytes_written);
    try appendInt(alloc, out, u64, record.artifact_bytes_written);
}

fn readRuntimeEnrichmentStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
    version: u16,
) !metadata.RuntimeEnrichmentStatusReport {
    if (pos.* + 3 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const enabled = encoded[pos.*] != 0;
    pos.* += 1;
    const lease_owned = encoded[pos.*] != 0;
    pos.* += 1;
    const has_lease = encoded[pos.*] != 0;
    pos.* += 1;
    const acquisition_count = try readInt(encoded, pos, u64);
    const lease_acquire_failures = try readInt(encoded, pos, u64);
    const lost_leases = try readInt(encoded, pos, u64);
    const last_acquired_ms = try readInt(encoded, pos, u64);
    const target_sequence = try readInt(encoded, pos, u64);
    const applied_sequence = try readInt(encoded, pos, u64);
    const projection_checkpoint_status = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(projection_checkpoint_status);
    const projection_checkpoint_applied_sequence = try readInt(encoded, pos, u64);
    const projection_checkpoint_generation = try readInt(encoded, pos, u64);
    const projection_checkpoint_config_hash = try readInt(encoded, pos, u64);
    const checkpoint_replay_tail_sequence_count = try readInt(encoded, pos, u64);
    const processed_requests = try readInt(encoded, pos, u64);
    const error_count = try readInt(encoded, pos, u64);
    const retryable_error_count = try readInt(encoded, pos, u64);
    const fatal_error_count = try readInt(encoded, pos, u64);
    const consecutive_retry_count = if (version >= 12) try readInt(encoded, pos, u32) else 0;
    const next_retry_at_ms = if (version >= 12) try readInt(encoded, pos, u64) else 0;
    if (pos.* + 4 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const retrying = encoded[pos.*] != 0;
    pos.* += 1;
    const worker_failed = encoded[pos.*] != 0;
    pos.* += 1;
    const worker_started = encoded[pos.*] != 0;
    pos.* += 1;
    const stalled = encoded[pos.*] != 0;
    pos.* += 1;
    return .{
        .enabled = enabled,
        .lease_owned = lease_owned,
        .has_lease = has_lease,
        .acquisition_count = acquisition_count,
        .lease_acquire_failures = lease_acquire_failures,
        .lost_leases = lost_leases,
        .last_acquired_ms = last_acquired_ms,
        .target_sequence = target_sequence,
        .applied_sequence = applied_sequence,
        .projection_checkpoint_status = projection_checkpoint_status,
        .projection_checkpoint_applied_sequence = projection_checkpoint_applied_sequence,
        .projection_checkpoint_generation = projection_checkpoint_generation,
        .projection_checkpoint_config_hash = projection_checkpoint_config_hash,
        .checkpoint_replay_tail_sequence_count = checkpoint_replay_tail_sequence_count,
        .processed_requests = processed_requests,
        .error_count = error_count,
        .retryable_error_count = retryable_error_count,
        .fatal_error_count = fatal_error_count,
        .consecutive_retry_count = consecutive_retry_count,
        .next_retry_at_ms = next_retry_at_ms,
        .retrying = retrying,
        .worker_failed = worker_failed,
        .worker_started = worker_started,
        .stalled = stalled,
        .skip_by_hash_count = try readInt(encoded, pos, u64),
        .skipped_source_count = try readInt(encoded, pos, u64),
        .codec_decode_failures = try readInt(encoded, pos, u64),
        .embed_batches_started = try readInt(encoded, pos, u64),
        .embed_batches_completed = try readInt(encoded, pos, u64),
        .embed_items_started = try readInt(encoded, pos, u64),
        .embed_items_completed = try readInt(encoded, pos, u64),
        .active_embed_batch_items = try readInt(encoded, pos, u64),
        .active_embed_batch_bytes = try readInt(encoded, pos, u64),
        .active_embed_batch_max_bytes = try readInt(encoded, pos, u64),
        .active_embed_batch_started_ms = try readInt(encoded, pos, u64),
        .last_embed_batch_items = try readInt(encoded, pos, u64),
        .last_embed_batch_bytes = try readInt(encoded, pos, u64),
        .last_embed_batch_max_bytes = try readInt(encoded, pos, u64),
        .last_embed_batch_completed_ms = if (version >= 10) try readInt(encoded, pos, u64) else 0,
        .last_embed_batch_ns = try readInt(encoded, pos, u64),
        .total_embed_ns = try readInt(encoded, pos, u64),
        .dense_artifact_bytes_written = try readInt(encoded, pos, u64),
        .sparse_artifact_bytes_written = try readInt(encoded, pos, u64),
        .chunk_artifact_bytes_written = try readInt(encoded, pos, u64),
        .artifact_bytes_written = try readInt(encoded, pos, u64),
    };
}

fn readRuntimeGroupStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RuntimeGroupStatusReport {
    const version = try readInt(encoded, pos, u16);
    if (version == 0 or version > runtime_status_record_version) return error.InvalidMetadataTransitionEncoding;
    const table_id = try readInt(encoded, pos, u64);
    const table_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(table_name);
    const group_id = try readInt(encoded, pos, u64);
    const store_id = try readInt(encoded, pos, u64);
    const node_id = try readInt(encoded, pos, u64);
    const updated_at_ns = try readInt(encoded, pos, u64);
    const source = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(source);
    const freshness = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(freshness);
    const topology_generation = try readInt(encoded, pos, u64);
    const lsm_root_generation = try readInt(encoded, pos, u64);
    const status_generation = try readInt(encoded, pos, u64);
    const doc_count = try readInt(encoded, pos, u64);
    const disk_bytes = if (version >= 2) try readInt(encoded, pos, u64) else 0;
    const disk_bytes_known = if (version >= 8) blk: {
        if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const created_at_millis = if (version >= 2) try readInt(encoded, pos, u64) else 0;
    const index_count = try readInt(encoded, pos, u32);
    const enrichment = if (version >= 9)
        try readRuntimeEnrichmentStatusRecord(alloc, encoded, pos, version)
    else blk: {
        if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
        const enabled = encoded[pos.*] != 0;
        pos.* += 1;
        const target_sequence = try readInt(encoded, pos, u64);
        const applied_sequence = try readInt(encoded, pos, u64);
        const lifecycle_field_count: usize = if (version >= 8) 4 else 2;
        if (pos.* + lifecycle_field_count > encoded.len) return error.InvalidMetadataTransitionEncoding;
        const retrying = encoded[pos.*] != 0;
        pos.* += 1;
        const worker_failed = encoded[pos.*] != 0;
        pos.* += 1;
        const worker_started = if (version >= 8) started: {
            const value = encoded[pos.*] != 0;
            pos.* += 1;
            break :started value;
        } else false;
        const stalled = if (version >= 8) stalled_value: {
            const value = encoded[pos.*] != 0;
            pos.* += 1;
            break :stalled_value value;
        } else false;
        break :blk metadata.RuntimeEnrichmentStatusReport{
            .enabled = enabled,
            .target_sequence = target_sequence,
            .applied_sequence = applied_sequence,
            .projection_checkpoint_status = try alloc.dupe(u8, if (enabled) "rebuilding" else "clean"),
            .retrying = retrying,
            .worker_failed = worker_failed,
            .worker_started = worker_started,
            .stalled = stalled,
        };
    };
    errdefer alloc.free(enrichment.projection_checkpoint_status);
    if (pos.* + 4 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const async_indexing_active = encoded[pos.*] != 0;
    pos.* += 1;
    const async_startup_active = encoded[pos.*] != 0;
    pos.* += 1;
    const async_dense_catch_up_active = encoded[pos.*] != 0;
    pos.* += 1;
    const async_bulk_coalescing_active = encoded[pos.*] != 0;
    pos.* += 1;
    const doc_identity = if (version >= 3) try readRuntimeDocIdentityStatusRecord(encoded, pos) else metadata.RuntimeDocIdentityStatusReport{};
    const doc_set_planning = if (version >= 3) try readRuntimeDocSetPlanningStatusRecord(encoded, pos, version) else metadata.RuntimeDocSetPlanningStatusReport{};
    const runtime_index_count = try readInt(encoded, pos, u32);
    const indexes = try alloc.alloc(metadata.RuntimeIndexStatusReport, runtime_index_count);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |record| metadata_table_manager.freeRuntimeIndexStatusReport(alloc, record);
        if (indexes.len > 0) alloc.free(indexes);
    }
    while (initialized < runtime_index_count) : (initialized += 1) {
        indexes[initialized] = try readRuntimeIndexStatusRecord(alloc, encoded, pos, version);
    }
    return .{
        .table_id = table_id,
        .table_name = table_name,
        .group_id = group_id,
        .store_id = store_id,
        .node_id = node_id,
        .updated_at_ns = updated_at_ns,
        .source = source,
        .freshness = freshness,
        .topology_generation = topology_generation,
        .lsm_root_generation = lsm_root_generation,
        .status_generation = status_generation,
        .doc_count = doc_count,
        .disk_bytes = disk_bytes,
        .disk_bytes_known = disk_bytes_known,
        .created_at_millis = created_at_millis,
        .index_count = index_count,
        .enrichment = enrichment,
        .async_indexing_active = async_indexing_active,
        .async_startup_active = async_startup_active,
        .async_dense_catch_up_active = async_dense_catch_up_active,
        .async_bulk_coalescing_active = async_bulk_coalescing_active,
        .doc_identity = doc_identity,
        .doc_set_planning = doc_set_planning,
        .indexes = indexes,
    };
}

fn appendRuntimeDocIdentityStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeDocIdentityStatusReport,
) !void {
    try appendInt(alloc, out, u64, record.namespace_table_id);
    try appendInt(alloc, out, u64, record.namespace_shard_id);
    try appendInt(alloc, out, u64, record.namespace_range_id);
    try appendInt(alloc, out, u32, record.next_ordinal);
    try appendInt(alloc, out, u64, record.allocated_ordinals);
    try appendInt(alloc, out, u64, record.ordinal_capacity_remaining);
    try out.append(alloc, if (record.ordinal_capacity_exhausted) 1 else 0);
    try out.append(alloc, if (record.rebuild_required) 1 else 0);
    try appendInt(alloc, out, u64, record.state_rows);
    try appendInt(alloc, out, u64, record.live_ordinals);
    try appendInt(alloc, out, u64, record.tombstone_ordinals);
    try appendInt(alloc, out, u64, record.min_created_generation);
    try appendInt(alloc, out, u64, record.max_created_generation);
    try appendInt(alloc, out, u64, record.min_deleted_generation);
    try appendInt(alloc, out, u64, record.max_deleted_generation);
    try appendInt(alloc, out, u64, record.scanned_primary_docs);
    try appendInt(alloc, out, u64, record.primary_docs_missing_ordinals);
    try appendInt(alloc, out, u64, record.primary_docs_missing_identity_state);
    try appendInt(alloc, out, u64, record.primary_docs_with_tombstone_ordinals);
    try out.append(alloc, if (record.complete) 1 else 0);
}

fn readRuntimeDocIdentityStatusRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.RuntimeDocIdentityStatusReport {
    const namespace_table_id = try readInt(encoded, pos, u64);
    const namespace_shard_id = try readInt(encoded, pos, u64);
    const namespace_range_id = try readInt(encoded, pos, u64);
    const next_ordinal = try readInt(encoded, pos, u32);
    const allocated_ordinals = try readInt(encoded, pos, u64);
    const ordinal_capacity_remaining = try readInt(encoded, pos, u64);
    if (pos.* + 2 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const ordinal_capacity_exhausted = encoded[pos.*] != 0;
    pos.* += 1;
    const rebuild_required = encoded[pos.*] != 0;
    pos.* += 1;
    const state_rows = try readInt(encoded, pos, u64);
    const live_ordinals = try readInt(encoded, pos, u64);
    const tombstone_ordinals = try readInt(encoded, pos, u64);
    const min_created_generation = try readInt(encoded, pos, u64);
    const max_created_generation = try readInt(encoded, pos, u64);
    const min_deleted_generation = try readInt(encoded, pos, u64);
    const max_deleted_generation = try readInt(encoded, pos, u64);
    const scanned_primary_docs = try readInt(encoded, pos, u64);
    const primary_docs_missing_ordinals = try readInt(encoded, pos, u64);
    const primary_docs_missing_identity_state = try readInt(encoded, pos, u64);
    const primary_docs_with_tombstone_ordinals = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const complete = encoded[pos.*] != 0;
    pos.* += 1;
    return .{
        .namespace_table_id = namespace_table_id,
        .namespace_shard_id = namespace_shard_id,
        .namespace_range_id = namespace_range_id,
        .next_ordinal = next_ordinal,
        .allocated_ordinals = allocated_ordinals,
        .ordinal_capacity_remaining = ordinal_capacity_remaining,
        .ordinal_capacity_exhausted = ordinal_capacity_exhausted,
        .rebuild_required = rebuild_required,
        .state_rows = state_rows,
        .live_ordinals = live_ordinals,
        .tombstone_ordinals = tombstone_ordinals,
        .min_created_generation = min_created_generation,
        .max_created_generation = max_created_generation,
        .min_deleted_generation = min_deleted_generation,
        .max_deleted_generation = max_deleted_generation,
        .scanned_primary_docs = scanned_primary_docs,
        .primary_docs_missing_ordinals = primary_docs_missing_ordinals,
        .primary_docs_missing_identity_state = primary_docs_missing_identity_state,
        .primary_docs_with_tombstone_ordinals = primary_docs_with_tombstone_ordinals,
        .complete = complete,
    };
}

fn appendRuntimeDocSetPlanningStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeDocSetPlanningStatusReport,
) !void {
    try appendInt(alloc, out, u64, record.resolved_set_count);
    try appendInt(alloc, out, u64, record.all_set_count);
    try appendInt(alloc, out, u64, record.none_set_count);
    try appendInt(alloc, out, u64, record.doc_key_list_count);
    try appendInt(alloc, out, u64, record.ordinal_list_count);
    try appendInt(alloc, out, u64, record.ordinal_bitmap_count);
    try appendInt(alloc, out, u64, record.doc_key_list_docs);
    try appendInt(alloc, out, u64, record.ordinal_list_docs);
    try appendInt(alloc, out, u64, record.ordinal_bitmap_docs);
    try appendInt(alloc, out, u64, record.missing_ordinal_coverage_count);
    try appendInt(alloc, out, u64, record.bitmap_promotion_count);
    try appendInt(alloc, out, u64, record.unsupported_filter_shape_count);
    try appendInt(alloc, out, u64, record.stale_identity_generation_rejection_count);
}

fn readRuntimeDocSetPlanningStatusRecord(
    encoded: []const u8,
    pos: *usize,
    version: u16,
) !metadata.RuntimeDocSetPlanningStatusReport {
    return .{
        .resolved_set_count = try readInt(encoded, pos, u64),
        .all_set_count = try readInt(encoded, pos, u64),
        .none_set_count = try readInt(encoded, pos, u64),
        .doc_key_list_count = try readInt(encoded, pos, u64),
        .ordinal_list_count = try readInt(encoded, pos, u64),
        .ordinal_bitmap_count = try readInt(encoded, pos, u64),
        .doc_key_list_docs = try readInt(encoded, pos, u64),
        .ordinal_list_docs = try readInt(encoded, pos, u64),
        .ordinal_bitmap_docs = try readInt(encoded, pos, u64),
        .missing_ordinal_coverage_count = try readInt(encoded, pos, u64),
        .bitmap_promotion_count = try readInt(encoded, pos, u64),
        .unsupported_filter_shape_count = try readInt(encoded, pos, u64),
        .stale_identity_generation_rejection_count = if (version >= 4) try readInt(encoded, pos, u64) else 0,
    };
}

fn appendRuntimeIndexStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeIndexStatusReport,
) !void {
    try appendRequiredString(alloc, out, record.name);
    try appendRequiredString(alloc, out, record.kind);
    try appendInt(alloc, out, u64, record.doc_count);
    try appendInt(alloc, out, u64, record.term_count);
    try appendInt(alloc, out, u64, record.edge_count);
    try appendInt(alloc, out, u64, record.node_count);
    try appendInt(alloc, out, u64, record.root_node);
    try appendInt(alloc, out, u64, record.coverage_produced_count);
    try appendInt(alloc, out, u64, record.coverage_skipped_count);
    try appendInt(alloc, out, u64, record.coverage_terminal_failed_count);
    try appendInt(alloc, out, u64, record.coverage_generation);
    try appendInt(alloc, out, u64, record.coverage_config_hash);
    try out.append(alloc, if (record.coverage_identity_ready) 1 else 0);
    try out.append(alloc, if (record.coverage_summary_ready) 1 else 0);
    try out.append(alloc, if (record.backfill_active) 1 else 0);
    try appendInt(alloc, out, u16, record.backfill_progress_millis);
    try appendInt(alloc, out, u64, record.replay_applied_sequence);
    try appendInt(alloc, out, u64, record.replay_target_sequence);
    try out.append(alloc, if (record.replay_catch_up_required) 1 else 0);
    try appendOptionalString(alloc, out, record.load_error);
}

fn readRuntimeIndexStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
    version: u16,
) !metadata.RuntimeIndexStatusReport {
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const kind = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(kind);
    const doc_count = try readInt(encoded, pos, u64);
    const term_count = try readInt(encoded, pos, u64);
    const edge_count = try readInt(encoded, pos, u64);
    const node_count = try readInt(encoded, pos, u64);
    const root_node = try readInt(encoded, pos, u64);
    const coverage_produced_count = if (version >= 5) try readInt(encoded, pos, u64) else 0;
    const coverage_skipped_count = if (version >= 5) try readInt(encoded, pos, u64) else 0;
    const coverage_terminal_failed_count = if (version >= 5) try readInt(encoded, pos, u64) else 0;
    const coverage_generation = if (version >= 7) try readInt(encoded, pos, u64) else 0;
    const coverage_config_hash = if (version >= 6) try readInt(encoded, pos, u64) else 0;
    const coverage_identity_ready = if (version >= 7) blk: {
        if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
        const ready = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk ready;
    } else false;
    const coverage_summary_ready = if (version >= 6) blk: {
        if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
        const ready = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk ready;
    } else false;
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const backfill_active = encoded[pos.*] != 0;
    pos.* += 1;
    const backfill_progress_millis = try readInt(encoded, pos, u16);
    const replay_applied_sequence = try readInt(encoded, pos, u64);
    const replay_target_sequence = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const replay_catch_up_required = encoded[pos.*] != 0;
    pos.* += 1;
    const load_error = if (version >= 11) try readOptionalString(alloc, encoded, pos) else null;
    errdefer if (load_error) |value| alloc.free(value);
    return .{
        .name = name,
        .kind = kind,
        .load_error = load_error,
        .doc_count = doc_count,
        .term_count = term_count,
        .edge_count = edge_count,
        .node_count = node_count,
        .root_node = root_node,
        .coverage_produced_count = coverage_produced_count,
        .coverage_skipped_count = coverage_skipped_count,
        .coverage_terminal_failed_count = coverage_terminal_failed_count,
        .coverage_generation = coverage_generation,
        .coverage_config_hash = coverage_config_hash,
        .coverage_identity_ready = coverage_identity_ready,
        .coverage_summary_ready = coverage_summary_ready,
        .backfill_active = backfill_active,
        .backfill_progress_millis = backfill_progress_millis,
        .replay_applied_sequence = replay_applied_sequence,
        .replay_target_sequence = replay_target_sequence,
        .replay_catch_up_required = replay_catch_up_required,
    };
}

fn appendGroupStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.GroupStatusReport,
) !void {
    try appendInt(alloc, out, u16, group_status_record_version);
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.relocation_generation);
    try appendInt(alloc, out, u64, record.raft_applied_index);
    try appendInt(alloc, out, u64, record.raft_term);
    try appendInt(alloc, out, u64, record.raft_membership_index);
    try appendInt(alloc, out, u64, record.doc_count);
    try appendInt(alloc, out, u64, record.disk_bytes);
    try out.append(alloc, if (record.disk_bytes_known) 1 else 0);
    try out.append(alloc, if (record.empty) 1 else 0);
    try appendInt(alloc, out, u32, 0);
    try appendInt(alloc, out, u64, record.updated_at_millis);
    try out.append(alloc, if (record.local_leader) 1 else 0);
    try appendInt(alloc, out, u64, record.created_at_millis);
    try out.append(alloc, if (record.transition_pending) 1 else 0);
    try out.append(alloc, if (record.replay_required) 1 else 0);
    try out.append(alloc, if (record.replay_caught_up) 1 else 0);
    try out.append(alloc, if (record.cutover_ready) 1 else 0);
    try out.append(alloc, if (record.reads_ready_after_cutover) 1 else 0);
    try out.append(alloc, if (record.local_voter) 1 else 0);
    try appendInt(alloc, out, u16, record.voter_count);
    try out.append(alloc, if (record.joint_consensus) 1 else 0);
    try out.append(alloc, if (record.voter_set_known) 1 else 0);
    try out.appendSlice(alloc, &record.voter_set_fingerprint);
}

fn readGroupStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.GroupStatusReport {
    _ = alloc;
    const version = try readInt(encoded, pos, u16);
    if (version == 0 or version > group_status_record_version) return error.InvalidMetadataTransitionEncoding;
    const group_id = try readInt(encoded, pos, u64);
    const relocation_generation = if (version >= 2) try readInt(encoded, pos, u64) else 0;
    const raft_applied_index = if (version >= 2) try readInt(encoded, pos, u64) else 0;
    const raft_term = if (version >= 4) try readInt(encoded, pos, u64) else 0;
    const raft_membership_index = if (version >= 4) try readInt(encoded, pos, u64) else 0;
    const doc_count = try readInt(encoded, pos, u64);
    const disk_bytes = try readInt(encoded, pos, u64);
    const disk_bytes_known = if (version >= 3) blk: {
        if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const empty = encoded[pos.*] != 0;
    pos.* += 1;
    const median_key_len = try readInt(encoded, pos, u32);
    if (pos.* + median_key_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    pos.* += median_key_len;
    const updated_at_millis = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    const local_leader = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const created_at_millis = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    const transition_pending = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const replay_required = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const replay_caught_up = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const cutover_ready = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const reads_ready_after_cutover = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const local_voter = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const voter_count = if (pos.* + @sizeOf(u16) <= encoded.len)
        try readInt(encoded, pos, u16)
    else
        0;
    const joint_consensus = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const voter_set_known = encoded[pos.*] != 0;
    pos.* += 1;
    if (pos.* + metadata_table_manager.voter_set_fingerprint_len > encoded.len) {
        return error.InvalidMetadataTransitionEncoding;
    }
    var voter_set_fingerprint: metadata_table_manager.VoterSetFingerprint = undefined;
    @memcpy(&voter_set_fingerprint, encoded[pos.* .. pos.* + metadata_table_manager.voter_set_fingerprint_len]);
    pos.* += metadata_table_manager.voter_set_fingerprint_len;
    return .{
        .group_id = group_id,
        .relocation_generation = relocation_generation,
        .raft_applied_index = raft_applied_index,
        .raft_term = raft_term,
        .raft_membership_index = raft_membership_index,
        .doc_count = doc_count,
        .disk_bytes = disk_bytes,
        .disk_bytes_known = disk_bytes_known,
        .empty = empty,
        .created_at_millis = created_at_millis,
        .updated_at_millis = updated_at_millis,
        .local_leader = local_leader,
        .local_voter = local_voter,
        .voter_count = voter_count,
        .voter_set_known = voter_set_known,
        .voter_set_fingerprint = voter_set_fingerprint,
        .joint_consensus = joint_consensus,
        .transition_pending = transition_pending,
        .replay_required = replay_required,
        .replay_caught_up = replay_caught_up,
        .cutover_ready = cutover_ready,
        .reads_ready_after_cutover = reads_ready_after_cutover,
    };
}

fn appendPlacementIntent(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    intent: raft_reconciler.PlacementIntent,
) !void {
    try appendInt(alloc, out, u64, intent.record.group_id);
    try appendInt(alloc, out, u64, intent.record.replica_id);
    try appendInt(alloc, out, u64, intent.record.local_node_id);
    try appendInt(alloc, out, u64, intent.record.metadata_version);
    try out.append(alloc, @intFromEnum(intent.record.bootstrap_mode));
    try appendInt(alloc, out, u32, @intCast(intent.peer_node_ids.len));
    for (intent.peer_node_ids) |node_id| try appendInt(alloc, out, u64, node_id);
    try appendInt(alloc, out, u64, intent.store_id);
    const source_tag: u8 = if (intent.record.snapshot_bootstrap != null)
        1
    else if (intent.record.backup_restore_bootstrap != null)
        2
    else
        0;
    try out.append(alloc, source_tag);
    switch (source_tag) {
        1 => {
            const snapshot = intent.record.snapshot_bootstrap.?;
            try appendInt(alloc, out, u64, snapshot.from_node_id);
            try appendInt(alloc, out, u64, snapshot.term);
            try appendInt(alloc, out, u32, @intCast(snapshot.snapshot_id.len));
            try out.appendSlice(alloc, snapshot.snapshot_id);
            try appendInt(alloc, out, u32, @intCast(snapshot.uri.len));
            try out.appendSlice(alloc, snapshot.uri);
        },
        2 => {
            const backup = intent.record.backup_restore_bootstrap.?;
            try backup.validate();
            try appendInt(alloc, out, u32, @intCast(backup.backup_id.len));
            try out.appendSlice(alloc, backup.backup_id);
            try appendInt(alloc, out, u32, @intCast(backup.artifact_backup_id.len));
            try out.appendSlice(alloc, backup.artifact_backup_id);
            try appendInt(alloc, out, u32, @intCast(backup.location.len));
            try out.appendSlice(alloc, backup.location);
            try appendInt(alloc, out, u32, @intCast(backup.snapshot_path.len));
            try out.appendSlice(alloc, backup.snapshot_path);
            try appendInt(alloc, out, u32, @intCast(backup.connection.len));
            try out.appendSlice(alloc, backup.connection);
            try appendInt(alloc, out, u64, backup.artifact_size_bytes);
            try appendInt(alloc, out, u32, @intCast(backup.artifact_sha256.len));
            try out.appendSlice(alloc, backup.artifact_sha256);
        },
        else => {},
    }
    try out.append(alloc, @intFromEnum(intent.serving_state));
    try appendInt(alloc, out, u64, intent.relocation_generation);
    try appendInt(alloc, out, u64, intent.relocation_source_node_id);
    try appendInt(alloc, out, u64, intent.relocation_source_store_id);
    try appendInt(alloc, out, u64, intent.relocation_doc_count_watermark);
    try appendInt(alloc, out, u64, intent.relocation_disk_bytes_watermark);
    try appendInt(alloc, out, u64, intent.relocation_target_sequence);
    try appendInt(alloc, out, u64, intent.relocation_applied_sequence);
}

fn appendTableRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.TableRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u16, record.desired_replica_count);
    try appendInt(alloc, out, u32, record.min_ranges);
    try appendInt(alloc, out, u32, @intCast(record.name.len));
    try out.appendSlice(alloc, record.name);
    try appendInt(alloc, out, u32, @intCast(record.description.len));
    try out.appendSlice(alloc, record.description);
    try appendInt(alloc, out, u32, @intCast(record.schema_json.len));
    try out.appendSlice(alloc, record.schema_json);
    try appendInt(alloc, out, u32, @intCast(record.read_schema_json.len));
    try out.appendSlice(alloc, record.read_schema_json);
    try appendInt(alloc, out, u32, @intCast(record.indexes_json.len));
    try out.appendSlice(alloc, record.indexes_json);
    try appendInt(alloc, out, u32, @intCast(record.replication_sources_json.len));
    try out.appendSlice(alloc, record.replication_sources_json);
    try appendInt(alloc, out, u32, @intCast(record.placement_role.len));
    try out.appendSlice(alloc, record.placement_role);
    try appendInt(alloc, out, u32, @intCast(record.restore_backup_id.len));
    try out.appendSlice(alloc, record.restore_backup_id);
    try appendInt(alloc, out, u32, @intCast(record.restore_location.len));
    try out.appendSlice(alloc, record.restore_location);
}

fn appendFramedTableRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.TableRecord,
) !void {
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(alloc);
    try appendTableRecord(alloc, &encoded, record);
    try appendInt(alloc, out, u32, @intCast(encoded.items.len));
    try out.appendSlice(alloc, encoded.items);
}

fn appendSchemaProgressRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.SchemaProgressRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u32, record.schema_version);
}

fn appendRestoreProgressRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RestoreProgressRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u32, @intCast(record.backup_id.len));
    try out.appendSlice(alloc, record.backup_id);
    try appendInt(alloc, out, u32, @intCast(record.location.len));
    try out.appendSlice(alloc, record.location);
    try appendInt(alloc, out, u32, @intCast(record.snapshot_path.len));
    try out.appendSlice(alloc, record.snapshot_path);
    try out.append(alloc, if (record.primary_restored) 1 else 0);
    try out.append(alloc, if (record.runtime_repair_complete) 1 else 0);
    try appendInt(alloc, out, u32, @intCast(record.phase.len));
    try out.appendSlice(alloc, record.phase);
    try appendInt(alloc, out, u32, @intCast(record.last_error.len));
    try out.appendSlice(alloc, record.last_error);
    try appendInt(alloc, out, u64, record.updated_at_ms);
    try appendInt(alloc, out, u32, @intCast(record.artifact_sha256.len));
    try out.appendSlice(alloc, record.artifact_sha256);
    try appendInt(alloc, out, u32, @intCast(record.artifact_backup_id.len));
    try out.appendSlice(alloc, record.artifact_backup_id);
}

fn appendReplicationSourceStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ReplicationSourceStatusRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u32, record.source_ordinal);
    try appendInt(alloc, out, u32, @intCast(record.source_kind.len));
    try out.appendSlice(alloc, record.source_kind);
    try appendInt(alloc, out, u32, @intCast(record.external_table.len));
    try out.appendSlice(alloc, record.external_table);
    try appendInt(alloc, out, u32, @intCast(record.slot_name.len));
    try out.appendSlice(alloc, record.slot_name);
    try appendInt(alloc, out, u32, @intCast(record.publication_name.len));
    try out.appendSlice(alloc, record.publication_name);
    try appendInt(alloc, out, u32, @intCast(record.phase.len));
    try out.appendSlice(alloc, record.phase);
    try appendInt(alloc, out, u32, @intCast(record.checkpoint.len));
    try out.appendSlice(alloc, record.checkpoint);
    try appendInt(alloc, out, u64, record.snapshot_offset);
    try appendInt(alloc, out, u32, @intCast(record.stream_checkpoint.len));
    try out.appendSlice(alloc, record.stream_checkpoint);
    try appendInt(alloc, out, u32, @intCast(record.last_error.len));
    try out.appendSlice(alloc, record.last_error);
    try appendInt(alloc, out, u64, record.lag_records);
    try appendInt(alloc, out, u64, record.updated_at_ms);
    try appendInt(alloc, out, u32, @intCast(record.prepared_checkpoint.len));
    try out.appendSlice(alloc, record.prepared_checkpoint);
    try appendInt(alloc, out, u32, @intCast(record.cutover_mode.len));
    try out.appendSlice(alloc, record.cutover_mode);
    try appendInt(alloc, out, u64, record.consecutive_failures);
    try appendInt(alloc, out, u64, record.last_success_at_ms);
    try appendInt(alloc, out, u64, record.last_change_applied_at_ms);
    try appendInt(alloc, out, u32, @intCast(record.failure_class.len));
    try out.appendSlice(alloc, record.failure_class);
    try appendInt(alloc, out, u64, record.lag_millis);
    try appendInt(alloc, out, u64, record.last_source_commit_at_ms);
    try appendInt(alloc, out, u64, record.cutover_intent_id);
    try out.appendSlice(alloc, &record.cutover_config_fingerprint);
    try appendInt(alloc, out, u64, record.cutover_authority_id);
    try out.appendSlice(alloc, &record.cutover_provider_identity);
    try appendInt(alloc, out, u64, record.retired_cutover_authority_id);
    try appendRequiredString(alloc, out, record.retired_slot_name);
    try appendRequiredString(alloc, out, record.retired_publication_name);
}

fn appendRangeRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RangeRecord,
) !void {
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u32, @intCast(record.start_key.len));
    try out.appendSlice(alloc, record.start_key);
    if (record.end_key) |end| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(end.len));
        try out.appendSlice(alloc, end);
    } else {
        try out.append(alloc, 0);
    }
    try appendInt(alloc, out, u32, @intCast(record.restore_backup_id.len));
    try out.appendSlice(alloc, record.restore_backup_id);
    try appendInt(alloc, out, u32, @intCast(record.restore_location.len));
    try out.appendSlice(alloc, record.restore_location);
    try appendInt(alloc, out, u32, @intCast(record.restore_snapshot_path.len));
    try out.appendSlice(alloc, record.restore_snapshot_path);
    const range_id = if (record.range_id == 0) record.group_id else record.range_id;
    try appendInt(alloc, out, u64, range_id);
    try appendInt(alloc, out, u64, record.doc_identity_shard_id);
    try appendInt(alloc, out, u64, record.doc_identity_range_id);
    try appendInt(alloc, out, u64, record.split_attempt_epoch);
    try appendInt(alloc, out, u32, @intCast(record.restore_connection.len));
    try out.appendSlice(alloc, record.restore_connection);
    try appendInt(alloc, out, u64, record.restore_artifact_size_bytes);
    try appendInt(alloc, out, u32, @intCast(record.restore_artifact_sha256.len));
    try out.appendSlice(alloc, record.restore_artifact_sha256);
    try appendInt(alloc, out, u32, @intCast(record.restore_artifact_backup_id.len));
    try out.appendSlice(alloc, record.restore_artifact_backup_id);
    try out.appendSlice(alloc, &record.completed_restore_fingerprint);
}

fn appendSplitTransitionRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.SplitTransitionRecord,
) !void {
    if (record.transition_id == 0 or record.attempt_epoch == 0 or
        record.source_group_id == 0 or record.destination_group_id == 0)
    {
        return error.InvalidMetadataTransitionEncoding;
    }
    try appendInt(alloc, out, u64, record.transition_id);
    try appendInt(alloc, out, u64, record.attempt_epoch);
    try appendInt(alloc, out, u64, record.source_group_id);
    try appendInt(alloc, out, u64, record.destination_group_id);
    try out.append(alloc, @intFromEnum(record.phase));
    if (record.split_key) |split_key| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(split_key.len));
        try out.appendSlice(alloc, split_key);
    } else {
        try out.append(alloc, 0);
    }
    if (record.source_range_end) |end| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(end.len));
        try out.appendSlice(alloc, end);
    } else {
        try out.append(alloc, 0);
    }
    if (record.rollback_reason) |reason| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(reason.len));
        try out.appendSlice(alloc, reason);
    } else {
        try out.append(alloc, 0);
    }
    try record.table_contract.validateForSplit();
    try appendTransitionTableContract(alloc, out, record.table_contract);
}

fn appendMergeTransitionRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.MergeTransitionRecord,
) !void {
    if (record.transition_id == 0 or record.donor_group_id == 0 or
        record.receiver_group_id == 0)
    {
        return error.InvalidMetadataTransitionEncoding;
    }
    try appendInt(alloc, out, u64, record.transition_id);
    try appendInt(alloc, out, u64, record.donor_group_id);
    try appendInt(alloc, out, u64, record.receiver_group_id);
    try out.append(alloc, @intFromEnum(record.phase));
    if (record.rollback_reason) |reason| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(reason.len));
        try out.appendSlice(alloc, reason);
    } else {
        try out.append(alloc, 0);
    }
    try out.append(alloc, if (record.allow_doc_identity_reassignment) 1 else 0);
    try record.table_contract.validateForMerge(
        record.allow_doc_identity_reassignment,
    );
    try appendTransitionTableContract(alloc, out, record.table_contract);
}

fn appendTransitionTableContract(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    contract: metadata.TransitionTableContract,
) !void {
    try contract.validate();
    try appendInt(alloc, out, u64, contract.table_id);
    try appendRequiredString(alloc, out, contract.table_name);
    try appendRequiredString(alloc, out, contract.schema_json);
    try appendRequiredString(alloc, out, contract.indexes_json);
    try appendInt(alloc, out, u64, contract.source_identity.shard_id);
    try appendInt(alloc, out, u64, contract.source_identity.range_id);
    try appendInt(alloc, out, u64, contract.target_identity.shard_id);
    try appendInt(alloc, out, u64, contract.target_identity.range_id);
}

fn appendReconcileLeaseRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ReconcileLeaseRecord,
) !void {
    try appendInt(alloc, out, u64, record.owner_node_id);
    try appendInt(alloc, out, u64, record.expires_at_ms);
}

fn appendShuffleJoinLeaseRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ShuffleJoinLeaseRecord,
) !void {
    try appendInt(alloc, out, u64, record.job_id);
    try appendInt(alloc, out, u64, record.owner_group_id);
    try appendInt(alloc, out, u64, record.expires_at_ms);
}

fn appendReallocationRequestRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ReallocationRequestRecord,
) !void {
    if (!metadata.isValidReallocationRequest(record)) return error.InvalidMetadataRecord;
    try appendInt(alloc, out, u128, record.request_id);
    try appendInt(alloc, out, u64, record.requested_at_ms);
    if (record.barrier_protocol_version == 0) return;
    try out.appendSlice(alloc, reallocation_request_extension_magic);
    try appendInt(alloc, out, u16, reallocation_request_extension_version);
    try appendInt(alloc, out, u16, record.barrier_protocol_version);
    try out.appendSlice(alloc, &record.metadata_incarnation.?);
    try appendInt(alloc, out, u32, record.protected_metadata_member_count);
    try out.appendSlice(alloc, &record.protected_metadata_membership_fingerprint);
}

fn readTableRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const start = pos.*;
    const newest_record = readTableRecordWithRestoreIntent(alloc, encoded, pos) catch null;
    if (newest_record) |record| {
        if (pos.* == encoded.len) return record;
        metadata_table_manager.freeTable(alloc, record);
        pos.* = start;
    } else {
        pos.* = start;
    }

    const old_record = readTableRecordLegacy(alloc, encoded, pos) catch null;
    if (old_record) |record| {
        if (pos.* == encoded.len) return record;
        metadata_table_manager.freeTable(alloc, record);
        pos.* = start;
    } else {
        pos.* = start;
    }

    const record = try readTableRecordWithReadSchema(alloc, encoded, pos);
    if (pos.* != encoded.len) {
        metadata_table_manager.freeTable(alloc, record);
        return error.InvalidMetadataTransitionEncoding;
    }
    return record;
}

fn readFramedTableRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const record_len = try readInt(encoded, pos, u32);
    if (record_len == 0 or record_len > encoded.len - pos.*)
        return error.InvalidMetadataTransitionEncoding;
    const record_end = pos.* + record_len;
    var record_pos: usize = 0;
    const record = try readTableRecord(alloc, encoded[pos.*..record_end], &record_pos);
    pos.* = record_end;
    return record;
}

fn readTableRecordLegacy(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    return .{
        .table_id = table_id,
        .name = name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = try alloc.dupe(u8, ""),
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = try alloc.dupe(u8, ""),
        .restore_location = try alloc.dupe(u8, ""),
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readTableRecordWithReadSchema(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const read_schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(read_schema_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    return .{
        .table_id = table_id,
        .name = name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = try alloc.dupe(u8, ""),
        .restore_location = try alloc.dupe(u8, ""),
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readTableRecordWithRestoreIntent(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const read_schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(read_schema_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(placement_role);
    const restore_backup_id = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(restore_backup_id);
    const restore_location = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(restore_location);
    return .{
        .table_id = table_id,
        .name = name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readRequiredString(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) ![]u8 {
    const value_len = try readInt(encoded, pos, u32);
    if (pos.* + value_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const value = try alloc.dupe(u8, encoded[pos.* .. pos.* + value_len]);
    pos.* += value_len;
    return value;
}

fn readRequiredStrings(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
    max_count: usize,
) ![]const []const u8 {
    const count = try readInt(encoded, pos, u32);
    if (count == 0 or count > max_count) return error.InvalidMetadataTransitionEncoding;
    const values = try alloc.alloc([]const u8, count);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |value| alloc.free(value);
        alloc.free(values);
    }
    for (values) |*value| {
        value.* = try readRequiredString(alloc, encoded, pos);
        initialized += 1;
    }
    return values;
}

fn readJsonRecord(comptime T: type, alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !T {
    const json = try readRequiredString(alloc, encoded, pos);
    defer alloc.free(json);
    var value = try std.json.parseFromSliceLeaky(T, alloc, json, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer deinitExtensionJsonRecord(T, alloc, &value);
    if (@hasDecl(T, "validate")) try value.validate();
    return value;
}

fn appendRequiredString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    try appendInt(alloc, out, u32, @intCast(value.len));
    try out.appendSlice(alloc, value);
}

fn appendOptionalString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: ?[]const u8,
) !void {
    const present = value orelse {
        try out.append(alloc, 0);
        return;
    };
    try out.append(alloc, 1);
    try appendRequiredString(alloc, out, present);
}

fn appendRestoreIntentIdentity(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    identity: metadata.RestoreIntentIdentity,
) !void {
    try appendInt(alloc, out, u64, identity.group_id);
    try appendInt(alloc, out, u64, identity.table_id);
    try appendRequiredString(alloc, out, identity.backup_id);
    try appendRequiredString(alloc, out, identity.artifact_backup_id);
    try appendRequiredString(alloc, out, identity.location);
    try appendRequiredString(alloc, out, identity.snapshot_path);
    try appendRequiredString(alloc, out, identity.connection);
    try appendInt(alloc, out, u64, identity.artifact_size_bytes);
    try appendRequiredString(alloc, out, identity.artifact_sha256);
}

fn readRestoreIntentIdentity(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RestoreIntentIdentity {
    const group_id = try readInt(encoded, pos, u64);
    const table_id = try readInt(encoded, pos, u64);
    const backup_id = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(backup_id);
    const artifact_backup_id = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(artifact_backup_id);
    const location = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(location);
    const snapshot_path = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(snapshot_path);
    const connection = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(connection);
    const artifact_size_bytes = try readInt(encoded, pos, u64);
    const artifact_sha256 = try readRequiredString(alloc, encoded, pos);
    return .{
        .group_id = group_id,
        .table_id = table_id,
        .backup_id = backup_id,
        .artifact_backup_id = artifact_backup_id,
        .location = location,
        .snapshot_path = snapshot_path,
        .connection = connection,
        .artifact_size_bytes = artifact_size_bytes,
        .artifact_sha256 = artifact_sha256,
    };
}

fn appendJsonRecord(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), record: anytype) !void {
    const json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(record, .{})});
    defer alloc.free(json);
    try appendRequiredString(alloc, out, json);
}

fn deinitExtensionJsonRecord(comptime T: type, alloc: std.mem.Allocator, value: *T) void {
    if (@hasDecl(T, "deinitOwned")) {
        value.deinitOwned(alloc);
    }
}

fn freeExtensionLifecycleDelta(alloc: std.mem.Allocator, delta: ExtensionLifecycleDelta) void {
    for (delta.upsert_tables) |record| metadata_table_manager.freeTable(alloc, record);
    if (delta.upsert_tables.len > 0) alloc.free(@constCast(delta.upsert_tables));
    for (delta.upsert_installed_extensions) |record| {
        var owned = record;
        owned.deinitOwned(alloc);
    }
    if (delta.upsert_installed_extensions.len > 0) alloc.free(@constCast(delta.upsert_installed_extensions));
    for (delta.remove_installed_extensions) |name| alloc.free(@constCast(name));
    if (delta.remove_installed_extensions.len > 0) alloc.free(@constCast(delta.remove_installed_extensions));
    for (delta.upsert_extension_members) |record| {
        var owned = record;
        owned.deinitOwned(alloc);
    }
    if (delta.upsert_extension_members.len > 0) alloc.free(@constCast(delta.upsert_extension_members));
    for (delta.remove_extension_members) |record| {
        alloc.free(@constCast(record.extension_name));
        alloc.free(@constCast(record.object_name));
    }
    if (delta.remove_extension_members.len > 0) alloc.free(@constCast(delta.remove_extension_members));
    for (delta.upsert_extension_dependencies) |record| {
        var owned = record;
        owned.deinitOwned(alloc);
    }
    if (delta.upsert_extension_dependencies.len > 0) alloc.free(@constCast(delta.upsert_extension_dependencies));
    for (delta.remove_extension_dependencies) |record| {
        alloc.free(@constCast(record.extension_name));
        alloc.free(@constCast(record.required_extension_name));
        alloc.free(@constCast(record.package_name));
    }
    if (delta.remove_extension_dependencies.len > 0) alloc.free(@constCast(delta.remove_extension_dependencies));
}

fn readRangeRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RangeRecord {
    const group_id = try readInt(encoded, pos, u64);
    const table_id = try readInt(encoded, pos, u64);
    const start_len = try readInt(encoded, pos, u32);
    if (pos.* + start_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const start_key = try alloc.dupe(u8, encoded[pos.* .. pos.* + start_len]);
    errdefer alloc.free(start_key);
    pos.* += start_len;
    const end_key = try readOptionalString(alloc, encoded, pos);
    errdefer if (end_key) |value| alloc.free(value);
    const restore_backup_id = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_backup_id);
    const restore_location = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_location);
    const restore_snapshot_path = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_snapshot_path);
    const range_id = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        group_id;
    const doc_identity_shard_id = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    const doc_identity_range_id = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    // Range values are independently length-delimited by the metadata store.
    // Older v1 values end after the identity fields, so appended fields must
    // remain optional for rolling upgrades and snapshot/log replay.
    const split_attempt_epoch = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    const restore_connection = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_connection);
    const restore_artifact_size_bytes = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    const restore_artifact_sha256 = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_artifact_sha256);
    const restore_artifact_backup_id = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_artifact_backup_id);
    var completed_restore_fingerprint =
        metadata_table_manager.empty_restore_completion_fingerprint;
    if (pos.* < encoded.len) {
        if (encoded.len - pos.* <
            @sizeOf(metadata_table_manager.RestoreCompletionFingerprint))
        {
            return error.InvalidMetadataTransitionEncoding;
        }
        @memcpy(
            &completed_restore_fingerprint,
            encoded[pos.*..][0..@sizeOf(metadata_table_manager.RestoreCompletionFingerprint)],
        );
        pos.* += @sizeOf(metadata_table_manager.RestoreCompletionFingerprint);
    }
    return .{
        .group_id = group_id,
        .range_id = if (range_id == 0) group_id else range_id,
        .table_id = table_id,
        .start_key = start_key,
        .end_key = end_key,
        .doc_identity_shard_id = doc_identity_shard_id,
        .doc_identity_range_id = doc_identity_range_id,
        .split_attempt_epoch = split_attempt_epoch,
        .restore_backup_id = restore_backup_id,
        .restore_artifact_backup_id = restore_artifact_backup_id,
        .restore_location = restore_location,
        .restore_snapshot_path = restore_snapshot_path,
        .restore_connection = restore_connection,
        .restore_artifact_size_bytes = restore_artifact_size_bytes,
        .restore_artifact_sha256 = restore_artifact_sha256,
        .completed_restore_fingerprint = completed_restore_fingerprint,
    };
}

fn readSchemaProgressRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.SchemaProgressRecord {
    return .{
        .table_id = try readInt(encoded, pos, u64),
        .node_id = try readInt(encoded, pos, u64),
        .schema_version = try readInt(encoded, pos, u32),
    };
}

fn readRestoreProgressRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RestoreProgressRecord {
    const table_id = try readInt(encoded, pos, u64);
    const node_id = try readInt(encoded, pos, u64);
    const group_id = try readInt(encoded, pos, u64);
    const backup_id = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(backup_id);
    const location = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(location);
    const snapshot_path = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(snapshot_path);
    if (pos.* >= encoded.len) return error.InvalidRestoreProgressRecord;
    const primary_restored = switch (encoded[pos.*]) {
        0 => false,
        1 => true,
        else => return error.InvalidRestoreProgressRecord,
    };
    pos.* += 1;
    if (pos.* >= encoded.len) return error.InvalidRestoreProgressRecord;
    const runtime_repair_complete = switch (encoded[pos.*]) {
        0 => false,
        1 => true,
        else => return error.InvalidRestoreProgressRecord,
    };
    pos.* += 1;
    const phase = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(phase);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    const updated_at_ms = try readInt(encoded, pos, u64);
    const artifact_sha256 = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(artifact_sha256);
    const artifact_backup_id = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(artifact_backup_id);
    return .{
        .table_id = table_id,
        .node_id = node_id,
        .group_id = group_id,
        .backup_id = backup_id,
        .artifact_backup_id = artifact_backup_id,
        .location = location,
        .snapshot_path = snapshot_path,
        .artifact_sha256 = artifact_sha256,
        .primary_restored = primary_restored,
        .runtime_repair_complete = runtime_repair_complete,
        .phase = phase,
        .last_error = last_error,
        .updated_at_ms = updated_at_ms,
    };
}

fn readReplicationSourceStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.ReplicationSourceStatusRecord {
    const table_id = try readInt(encoded, pos, u64);
    const source_ordinal = try readInt(encoded, pos, u32);
    const source_kind = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(source_kind);
    const external_table = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(external_table);
    const slot_name = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(slot_name);
    const publication_name = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(publication_name);
    const phase = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(phase);
    const checkpoint = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(checkpoint);
    const snapshot_offset = if (pos.* + @sizeOf(u64) <= encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    const stream_checkpoint = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(stream_checkpoint);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    const lag_records = try readInt(encoded, pos, u64);
    const updated_at_ms = try readInt(encoded, pos, u64);
    const prepared_checkpoint = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(prepared_checkpoint);
    const cutover_mode = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(cutover_mode);
    const consecutive_failures = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const last_success_at_ms = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const last_change_applied_at_ms = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const failure_class = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(failure_class);
    const lag_millis = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const last_source_commit_at_ms = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const cutover_intent_id = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    var cutover_config_fingerprint =
        [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length;
    if (pos.* + cutover_config_fingerprint.len <= encoded.len) {
        @memcpy(&cutover_config_fingerprint, encoded[pos.*..][0..cutover_config_fingerprint.len]);
        pos.* += cutover_config_fingerprint.len;
    }
    const cutover_authority_id = try readInt(encoded, pos, u64);
    var cutover_provider_identity =
        [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length;
    if (pos.* + cutover_provider_identity.len > encoded.len)
        return error.InvalidMetadataTransitionEncoding;
    @memcpy(
        &cutover_provider_identity,
        encoded[pos.*..][0..cutover_provider_identity.len],
    );
    pos.* += cutover_provider_identity.len;
    const retired_cutover_authority_id = try readInt(encoded, pos, u64);
    const retired_slot_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(retired_slot_name);
    const retired_publication_name = try readRequiredString(
        alloc,
        encoded,
        pos,
    );
    errdefer alloc.free(retired_publication_name);
    return .{
        .table_id = table_id,
        .source_ordinal = source_ordinal,
        .source_kind = source_kind,
        .external_table = external_table,
        .cutover_mode = cutover_mode,
        .slot_name = slot_name,
        .publication_name = publication_name,
        .phase = phase,
        .checkpoint = checkpoint,
        .snapshot_offset = snapshot_offset,
        .prepared_checkpoint = prepared_checkpoint,
        .stream_checkpoint = stream_checkpoint,
        .last_error = last_error,
        .failure_class = failure_class,
        .lag_records = lag_records,
        .lag_millis = lag_millis,
        .consecutive_failures = consecutive_failures,
        .last_source_commit_at_ms = last_source_commit_at_ms,
        .last_success_at_ms = last_success_at_ms,
        .last_change_applied_at_ms = last_change_applied_at_ms,
        .cutover_intent_id = cutover_intent_id,
        .cutover_authority_id = cutover_authority_id,
        .cutover_config_fingerprint = cutover_config_fingerprint,
        .cutover_provider_identity = cutover_provider_identity,
        .retired_cutover_authority_id = retired_cutover_authority_id,
        .retired_slot_name = retired_slot_name,
        .retired_publication_name = retired_publication_name,
        .updated_at_ms = updated_at_ms,
    };
}

fn readPlacementIntent(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !raft_reconciler.PlacementIntent {
    const group_id = try readInt(encoded, pos, u64);
    const replica_id = try readInt(encoded, pos, u64);
    const local_node_id = try readInt(encoded, pos, u64);
    const metadata_version = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const bootstrap_mode = std.enums.fromInt(raft_catalog.ReplicaBootstrapMode, encoded[pos.*]) orelse
        return error.InvalidMetadataTransitionEncoding;
    pos.* += 1;
    const peer_count = try readInt(encoded, pos, u32);
    const peer_node_ids = try alloc.alloc(u64, peer_count);
    errdefer alloc.free(peer_node_ids);
    for (peer_node_ids) |*node_id| node_id.* = try readInt(encoded, pos, u64);
    const store_id = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    var snapshot_bootstrap: ?raft_catalog.SnapshotBootstrapRecord = null;
    var backup_restore_bootstrap: ?raft_catalog.BackupRestoreBootstrapRecord = null;
    if (pos.* < encoded.len) {
        const source_tag = encoded[pos.*];
        pos.* += 1;
        switch (source_tag) {
            0 => {},
            1 => {
                const from_node_id = try readInt(encoded, pos, u64);
                const term = try readInt(encoded, pos, u64);
                const snapshot_id = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(snapshot_id);
                const uri = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(uri);
                snapshot_bootstrap = .{
                    .from_node_id = from_node_id,
                    .term = term,
                    .snapshot_id = snapshot_id,
                    .uri = uri,
                };
            },
            2 => {
                const backup_id = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(backup_id);
                const artifact_backup_id = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(artifact_backup_id);
                const location = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(location);
                const snapshot_path = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(snapshot_path);
                const connection = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(connection);
                const artifact_size_bytes = try readInt(encoded, pos, u64);
                const artifact_sha256 = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(artifact_sha256);
                backup_restore_bootstrap = .{
                    .backup_id = backup_id,
                    .artifact_backup_id = artifact_backup_id,
                    .location = location,
                    .snapshot_path = snapshot_path,
                    .connection = connection,
                    .artifact_size_bytes = artifact_size_bytes,
                    .artifact_sha256 = artifact_sha256,
                };
                backup_restore_bootstrap.?.validate() catch
                    return error.InvalidMetadataTransitionEncoding;
            },
            else => return error.InvalidMetadataTransitionEncoding,
        }
    }
    var serving_state: raft_reconciler.PlacementServingState = .serving;
    var relocation_generation: u64 = 0;
    var relocation_source_node_id: u64 = 0;
    var relocation_source_store_id: u64 = 0;
    var relocation_doc_count_watermark: u64 = 0;
    var relocation_disk_bytes_watermark: u64 = 0;
    var relocation_target_sequence: u64 = 0;
    var relocation_applied_sequence: u64 = 0;
    if (pos.* < encoded.len) {
        serving_state = std.enums.fromInt(raft_reconciler.PlacementServingState, encoded[pos.*]) orelse
            return error.InvalidMetadataTransitionEncoding;
        pos.* += 1;
        relocation_generation = try readInt(encoded, pos, u64);
        relocation_source_node_id = try readInt(encoded, pos, u64);
        relocation_source_store_id = try readInt(encoded, pos, u64);
        relocation_doc_count_watermark = try readInt(encoded, pos, u64);
        relocation_disk_bytes_watermark = try readInt(encoded, pos, u64);
        relocation_target_sequence = try readInt(encoded, pos, u64);
        relocation_applied_sequence = try readInt(encoded, pos, u64);
    }
    return .{
        .record = .{
            .group_id = group_id,
            .replica_id = replica_id,
            .local_node_id = local_node_id,
            .metadata_version = metadata_version,
            .bootstrap_mode = bootstrap_mode,
            .snapshot_bootstrap = snapshot_bootstrap,
            .backup_restore_bootstrap = backup_restore_bootstrap,
        },
        .store_id = store_id,
        .peer_node_ids = peer_node_ids,
        .serving_state = serving_state,
        .relocation_generation = relocation_generation,
        .relocation_source_node_id = relocation_source_node_id,
        .relocation_source_store_id = relocation_source_store_id,
        .relocation_doc_count_watermark = relocation_doc_count_watermark,
        .relocation_disk_bytes_watermark = relocation_disk_bytes_watermark,
        .relocation_target_sequence = relocation_target_sequence,
        .relocation_applied_sequence = relocation_applied_sequence,
    };
}

fn readSplitTransitionRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.SplitTransitionRecord {
    const transition_id = try readInt(encoded, pos, u64);
    const attempt_epoch = try readInt(encoded, pos, u64);
    const source_group_id = try readInt(encoded, pos, u64);
    const destination_group_id = try readInt(encoded, pos, u64);
    if (transition_id == 0 or attempt_epoch == 0 or source_group_id == 0 or destination_group_id == 0)
        return error.InvalidMetadataTransitionEncoding;
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const phase = std.enums.fromInt(metadata.TransitionPhase, encoded[pos.*]) orelse
        return error.InvalidMetadataTransitionEncoding;
    pos.* += 1;
    const split_key = try readOptionalString(alloc, encoded, pos);
    errdefer if (split_key) |value| alloc.free(value);
    const source_range_end = try readOptionalString(alloc, encoded, pos);
    errdefer if (source_range_end) |value| alloc.free(value);
    const rollback_reason = try readOptionalString(alloc, encoded, pos);
    errdefer if (rollback_reason) |value| alloc.free(value);
    var table_contract = try readTransitionTableContract(alloc, encoded, pos);
    errdefer table_contract.deinitOwned(alloc);
    try table_contract.validateForSplit();
    return .{
        .transition_id = transition_id,
        .attempt_epoch = attempt_epoch,
        .source_group_id = source_group_id,
        .destination_group_id = destination_group_id,
        .phase = phase,
        .split_key = split_key,
        .source_range_end = source_range_end,
        .rollback_reason = rollback_reason,
        .table_contract = table_contract,
    };
}

fn readMergeTransitionRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.MergeTransitionRecord {
    const transition_id = try readInt(encoded, pos, u64);
    const donor_group_id = try readInt(encoded, pos, u64);
    const receiver_group_id = try readInt(encoded, pos, u64);
    if (transition_id == 0 or donor_group_id == 0 or receiver_group_id == 0)
        return error.InvalidMetadataTransitionEncoding;
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const phase = std.enums.fromInt(metadata.TransitionPhase, encoded[pos.*]) orelse
        return error.InvalidMetadataTransitionEncoding;
    pos.* += 1;
    const rollback_reason = try readOptionalString(alloc, encoded, pos);
    errdefer if (rollback_reason) |value| alloc.free(value);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const reassignment_tag = encoded[pos.*];
    if (reassignment_tag > 1) return error.InvalidMetadataTransitionEncoding;
    const allow_doc_identity_reassignment = reassignment_tag == 1;
    pos.* += 1;
    var table_contract = try readTransitionTableContract(alloc, encoded, pos);
    errdefer table_contract.deinitOwned(alloc);
    try table_contract.validateForMerge(allow_doc_identity_reassignment);
    return .{
        .transition_id = transition_id,
        .donor_group_id = donor_group_id,
        .receiver_group_id = receiver_group_id,
        .phase = phase,
        .rollback_reason = rollback_reason,
        .allow_doc_identity_reassignment = allow_doc_identity_reassignment,
        .table_contract = table_contract,
    };
}

fn readTransitionTableContract(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TransitionTableContract {
    const table_id = try readInt(encoded, pos, u64);
    const table_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(table_name);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const contract: metadata.TransitionTableContract = .{
        .table_id = table_id,
        .table_name = table_name,
        .schema_json = schema_json,
        .indexes_json = indexes_json,
        .source_identity = .{
            .shard_id = try readInt(encoded, pos, u64),
            .range_id = try readInt(encoded, pos, u64),
        },
        .target_identity = .{
            .shard_id = try readInt(encoded, pos, u64),
            .range_id = try readInt(encoded, pos, u64),
        },
    };
    try contract.validate();
    return contract;
}

fn readReconcileLeaseRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.ReconcileLeaseRecord {
    return .{
        .owner_node_id = try readInt(encoded, pos, u64),
        .expires_at_ms = try readInt(encoded, pos, u64),
    };
}

fn readShuffleJoinLeaseRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.ShuffleJoinLeaseRecord {
    return .{
        .job_id = try readInt(encoded, pos, u64),
        .owner_group_id = try readInt(encoded, pos, u64),
        .expires_at_ms = try readInt(encoded, pos, u64),
    };
}

fn readReallocationRequestRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.ReallocationRequestRecord {
    var record: metadata.ReallocationRequestRecord = .{
        .request_id = try readInt(encoded, pos, u128),
        .requested_at_ms = try readInt(encoded, pos, u64),
    };
    if (pos.* < encoded.len) {
        if (pos.* + reallocation_request_extension_magic.len > encoded.len or
            !std.mem.eql(
                u8,
                encoded[pos.* .. pos.* + reallocation_request_extension_magic.len],
                reallocation_request_extension_magic,
            ))
        {
            return error.InvalidMetadataRecord;
        }
        pos.* += reallocation_request_extension_magic.len;
        const version = try readInt(encoded, pos, u16);
        if (version != reallocation_request_extension_version) return error.InvalidMetadataRecord;
        record.barrier_protocol_version = try readInt(encoded, pos, u16);
        if (pos.* + @sizeOf(metadata.MetadataClusterIncarnation) > encoded.len) return error.InvalidMetadataRecord;
        var incarnation: metadata.MetadataClusterIncarnation = undefined;
        @memcpy(&incarnation, encoded[pos.*..][0..incarnation.len]);
        pos.* += incarnation.len;
        record.metadata_incarnation = incarnation;
        record.protected_metadata_member_count = try readInt(encoded, pos, u32);
        if (pos.* + record.protected_metadata_membership_fingerprint.len > encoded.len) return error.InvalidMetadataRecord;
        @memcpy(
            &record.protected_metadata_membership_fingerprint,
            encoded[pos.*..][0..record.protected_metadata_membership_fingerprint.len],
        );
        pos.* += record.protected_metadata_membership_fingerprint.len;
    }
    if (!metadata.isValidReallocationRequest(record)) return error.InvalidMetadataRecord;
    return record;
}

fn appendMetadataPrefixRowsTxn(
    alloc: std.mem.Allocator,
    txn: *docstore.DocStore.Txn,
    out: *std.ArrayListUnmanaged(docstore.OwnedKVPair),
    prefix: []const u8,
) !void {
    const rows = try docstore.DocStore.scanPrefixTxn(alloc, txn, prefix);
    var transferred = false;
    defer {
        if (!transferred) freeMetadataSnapshotRows(alloc, rows) else alloc.free(rows);
    }
    try out.appendSlice(alloc, rows);
    transferred = true;
}

fn appendMetadataPointRowTxn(
    alloc: std.mem.Allocator,
    txn: *docstore.DocStore.Txn,
    out: *std.ArrayListUnmanaged(docstore.OwnedKVPair),
    key: []const u8,
) !void {
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_value = try alloc.dupe(u8, borrowed);
    errdefer alloc.free(owned_value);
    try out.append(alloc, .{ .key = owned_key, .value = owned_value });
}

fn metadataSnapshotRowLessThan(_: void, lhs: docstore.OwnedKVPair, rhs: docstore.OwnedKVPair) bool {
    return std.mem.order(u8, lhs.key, rhs.key) == .lt;
}

fn encodeMetadataSnapshot(alloc: std.mem.Allocator, rows: []const docstore.OwnedKVPair) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, RaftApplyStore.metadata_snapshot_magic);
    try out.append(alloc, RaftApplyStore.metadata_snapshot_version);
    try appendInt(alloc, &out, u32, std.math.cast(u32, rows.len) orelse return error.MetadataSnapshotTooLarge);
    for (rows) |row| {
        try appendInt(alloc, &out, u32, std.math.cast(u32, row.key.len) orelse return error.MetadataSnapshotTooLarge);
        try out.appendSlice(alloc, row.key);
        try appendInt(alloc, &out, u32, std.math.cast(u32, row.value.len) orelse return error.MetadataSnapshotTooLarge);
        try out.appendSlice(alloc, row.value);
    }
    return try out.toOwnedSlice(alloc);
}

fn decodeMetadataSnapshotAlloc(alloc: std.mem.Allocator, encoded: []const u8) ![]docstore.OwnedKVPair {
    if (encoded.len < RaftApplyStore.metadata_snapshot_magic.len + 1 + @sizeOf(u32) or
        !std.mem.eql(u8, encoded[0..RaftApplyStore.metadata_snapshot_magic.len], RaftApplyStore.metadata_snapshot_magic) or
        encoded[RaftApplyStore.metadata_snapshot_magic.len] != RaftApplyStore.metadata_snapshot_version)
    {
        return error.InvalidMetadataSnapshot;
    }
    var pos: usize = RaftApplyStore.metadata_snapshot_magic.len + 1;
    const count = readInt(encoded, &pos, u32) catch return error.InvalidMetadataSnapshot;
    if (count > (encoded.len - pos) / 8) return error.InvalidMetadataSnapshot;
    const rows = try alloc.alloc(docstore.OwnedKVPair, count);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }
    while (initialized < rows.len) : (initialized += 1) {
        const key_len = readInt(encoded, &pos, u32) catch return error.InvalidMetadataSnapshot;
        if (key_len > encoded.len - pos) return error.InvalidMetadataSnapshot;
        const key = try alloc.dupe(u8, encoded[pos .. pos + key_len]);
        pos += key_len;
        errdefer alloc.free(key);
        const value_len = readInt(encoded, &pos, u32) catch return error.InvalidMetadataSnapshot;
        if (value_len > encoded.len - pos) return error.InvalidMetadataSnapshot;
        const value = try alloc.dupe(u8, encoded[pos .. pos + value_len]);
        pos += value_len;
        rows[initialized] = .{ .key = key, .value = value };
    }
    if (pos != encoded.len) return error.InvalidMetadataSnapshot;
    return rows;
}

fn freeMetadataSnapshotRows(alloc: std.mem.Allocator, rows: []const docstore.OwnedKVPair) void {
    for (rows) |row| {
        alloc.free(row.key);
        alloc.free(row.value);
    }
    alloc.free(rows);
}

fn validateMetadataSnapshotRows(
    alloc: std.mem.Allocator,
    group_id: u64,
    rows: []const docstore.OwnedKVPair,
) !void {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    try seen.ensureTotalCapacity(alloc, @intCast(rows.len));
    for (rows) |row| {
        if (!metadataSnapshotKeyBelongsToGroup(group_id, row.key)) return error.InvalidMetadataSnapshot;
        const result = try seen.getOrPut(alloc, row.key);
        if (result.found_existing) return error.InvalidMetadataSnapshot;
    }
}

fn metadataSnapshotKeyBelongsToGroup(group_id: u64, key: []const u8) bool {
    var buf: [256]u8 = undefined;
    for (RaftApplyStore.metadata_snapshot_projections) |descriptor| {
        const owned_key = switch (descriptor.key) {
            .prefix => |key_fn| key_fn(&buf, group_id) catch return false,
            .point => |key_fn| key_fn(&buf, group_id) catch return false,
        };
        switch (descriptor.key) {
            .prefix => if (std.mem.startsWith(u8, key, owned_key)) return true,
            .point => if (std.mem.eql(u8, key, owned_key)) return true,
        }
    }
    return false;
}

test "metadata raft apply store snapshot key registry covers every durable projection class" {
    const projection_count = @typeInfo(RaftApplyStore.MetadataSnapshotProjection).@"enum".fields.len;
    try std.testing.expectEqual(projection_count, RaftApplyStore.metadata_snapshot_projections.len);

    var seen = [_]bool{false} ** projection_count;
    var buf: [256]u8 = undefined;
    for (RaftApplyStore.metadata_snapshot_projections) |descriptor| {
        const ordinal = @intFromEnum(descriptor.projection);
        try std.testing.expect(!seen[ordinal]);
        seen[ordinal] = true;
        const key = switch (descriptor.key) {
            .prefix => |key_fn| try key_fn(&buf, 41),
            .point => |key_fn| try key_fn(&buf, 41),
        };
        try std.testing.expect(metadataSnapshotKeyBelongsToGroup(41, key));
        try std.testing.expect(!metadataSnapshotKeyBelongsToGroup(42, key));
    }
    for (seen) |present| try std.testing.expect(present);

    var descriptor_mask: u32 = 0;
    for (RaftApplyStore.metadata_snapshot_projections) |descriptor| {
        descriptor_mask |= RaftApplyStore.metadataSnapshotProjectionBit(descriptor.projection);
    }
    var command_mask: u32 = 0;
    inline for (std.meta.tags(std.meta.Tag(TransitionCommand))) |tag| {
        const mask = RaftApplyStore.transitionCommandProjectionMask(tag);
        try std.testing.expect(mask != 0);
        command_mask |= mask;
    }
    try std.testing.expectEqual(descriptor_mask, command_mask);
}

fn appendInt(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try out.appendSlice(alloc, &bytes);
}

fn readInt(encoded: []const u8, pos: *usize, comptime T: type) !T {
    if (pos.* + @sizeOf(T) > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const bytes: *const [@sizeOf(T)]u8 = @ptrCast(encoded[pos.* .. pos.* + @sizeOf(T)]);
    const value = std.mem.readInt(T, bytes, .little);
    pos.* += @sizeOf(T);
    return value;
}

fn readOptionalString(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !?[]u8 {
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const present = encoded[pos.*];
    pos.* += 1;
    if (present == 0) return null;
    const len = try readInt(encoded, pos, u32);
    if (pos.* + len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const reason = try alloc.dupe(u8, encoded[pos.* .. pos.* + len]);
    pos.* += len;
    return reason;
}

pub fn splitTransitionPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:split:{d}:", .{group_id});
}

pub fn placementPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_placement:{d}:", .{group_id});
}

pub fn placementVersionPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_placement_version:{d}:", .{group_id});
}

pub fn nodePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_node:{d}:", .{group_id});
}

pub fn storePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_store:{d}:", .{group_id});
}

pub fn tablePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_table:{d}:", .{group_id});
}

pub fn schemaProgressPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_schema_progress:{d}:", .{group_id});
}

pub fn restoreProgressPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_restore_progress:{d}:", .{group_id});
}

pub fn replicationSourceStatusPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_replication_source_status:{d}:", .{group_id});
}

pub fn extensionPackagePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_package:{d}:", .{group_id});
}

pub fn installedExtensionPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_installed_extension:{d}:", .{group_id});
}

pub fn extensionMemberPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_member:{d}:", .{group_id});
}

pub fn extensionDependencyPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_dependency:{d}:", .{group_id});
}

pub fn shuffleJoinLeasePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_shuffle_join_lease:{d}:", .{group_id});
}

pub fn restoreJobPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_restore_job:{d}:", .{group_id});
}

fn restoreJobKeyForGroup(buf: []u8, group_id: u64, logical_key: []const u8) ![]const u8 {
    try validateRestoreJobLogicalKey(logical_key);
    const prefix = try restoreJobPrefixForGroup(buf, group_id);
    if (prefix.len + logical_key.len > buf.len) return error.NoSpaceLeft;
    @memcpy(buf[prefix.len .. prefix.len + logical_key.len], logical_key);
    return buf[0 .. prefix.len + logical_key.len];
}

pub fn rangePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_range:{d}:", .{group_id});
}

pub fn mergeTransitionPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:merge:{d}:", .{group_id});
}

pub fn tableTransitionFencePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_table_transition_fence:{d}:", .{group_id});
}

pub fn reconcileLeaseKeyForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_reconcile_lease:{d}", .{group_id});
}

pub fn metadataIncarnationKeyForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_incarnation:{d}", .{group_id});
}

pub fn reallocationRequestKeyForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_reallocation_request:{d}", .{group_id});
}

fn pendingReallocationRequestKeyForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_reallocation_request_pending:{d}", .{group_id});
}

pub fn shuffleJoinLeaseKeyForGroup(buf: []u8, group_id: u64, job_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_shuffle_join_lease:{d}:{d}", .{ group_id, job_id });
}

fn splitTransitionKeyForGroup(buf: []u8, group_id: u64, transition_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:split:{d}:{d}", .{ group_id, transition_id });
}

fn mergeTransitionKeyForGroup(buf: []u8, group_id: u64, transition_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:merge:{d}:{d}", .{ group_id, transition_id });
}

fn tableKeyForGroup(buf: []u8, group_id: u64, table_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_table:{d}:{d}", .{ group_id, table_id });
}

fn tableTransitionFenceKeyForGroup(buf: []u8, group_id: u64, table_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_table_transition_fence:{d}:{d}", .{ group_id, table_id });
}

fn schemaProgressKeyForGroup(buf: []u8, group_id: u64, table_id: u64, node_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_schema_progress:{d}:{d}:{d}", .{ group_id, table_id, node_id });
}

fn restoreProgressKeyForGroup(buf: []u8, group_id: u64, table_id: u64, node_id: u64, range_group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_restore_progress:{d}:{d}:{d}:{d}", .{ group_id, table_id, node_id, range_group_id });
}

fn replicationSourceStatusKeyForGroup(buf: []u8, group_id: u64, table_id: u64, source_ordinal: u32) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_replication_source_status:{d}:{d}:{d}", .{ group_id, table_id, source_ordinal });
}

fn extensionPackageKeyForGroup(buf: []u8, group_id: u64, name: []const u8, version: []const u8) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_package:{d}:{s}:{s}", .{ group_id, name, version });
}

fn installedExtensionKeyForGroup(buf: []u8, group_id: u64, name: []const u8) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_installed_extension:{d}:{s}", .{ group_id, name });
}

fn extensionMemberKeyForGroup(buf: []u8, group_id: u64, extension_name: []const u8, object_kind: extension_domain.ExtensionObjectKind, object_name: []const u8) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_member:{d}:{s}:{s}:{s}", .{ group_id, extension_name, @tagName(object_kind), object_name });
}

fn extensionDependencyKeyForGroup(buf: []u8, group_id: u64, extension_name: []const u8, required_extension_name: []const u8, package_name: []const u8) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_extension_dependency:{d}:{s}:{s}:{s}", .{ group_id, extension_name, required_extension_name, package_name });
}

fn rangeKeyForGroup(buf: []u8, group_id: u64, range_group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_range:{d}:{d}", .{ group_id, range_group_id });
}

fn placementKeyForGroup(buf: []u8, group_id: u64, range_group_id: u64, local_node_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_placement:{d}:{d}:{d}", .{ group_id, range_group_id, local_node_id });
}

fn placementVersionKeyForGroup(buf: []u8, group_id: u64, range_group_id: u64, local_node_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_placement_version:{d}:{d}:{d}", .{ group_id, range_group_id, local_node_id });
}

fn nodeKeyForGroup(buf: []u8, group_id: u64, node_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_node:{d}:{d}", .{ group_id, node_id });
}

fn storeKeyForGroup(buf: []u8, group_id: u64, store_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_store:{d}:{d}", .{ group_id, store_id });
}

fn splitTransitionActive(record: metadata.SplitTransitionRecord) bool {
    return !transitionPhaseTerminal(record.phase);
}

fn rangeMatchesTransitionIdentity(
    record: metadata.RangeRecord,
    identity: transition_state.TransitionIdentity,
) bool {
    return metadata_table_manager.rangeDocIdentityShardId(record) ==
        identity.shard_id and
        metadata_table_manager.rangeDocIdentityRangeId(record) ==
            identity.range_id;
}

fn rangesAreAdjacent(
    a: metadata.RangeRecord,
    b: metadata.RangeRecord,
) bool {
    if (a.end_key) |a_end| {
        if (std.mem.eql(u8, a_end, b.start_key)) return true;
    }
    if (b.end_key) |b_end| {
        if (std.mem.eql(u8, b_end, a.start_key)) return true;
    }
    return false;
}

fn encodeTableTransitionFence(
    out: *[@sizeOf(u64) + @sizeOf(u32)]u8,
    fence: TableTransitionFence,
) void {
    std.mem.writeInt(u64, out[0..@sizeOf(u64)], fence.generation, .little);
    std.mem.writeInt(
        u32,
        out[@sizeOf(u64) .. @sizeOf(u64) + @sizeOf(u32)],
        fence.active_count,
        .little,
    );
}

fn decodeTableTransitionFence(encoded: []const u8) !TableTransitionFence {
    if (encoded.len != @sizeOf(u64) + @sizeOf(u32))
        return error.InvalidTableTransitionFence;
    return .{
        .generation = std.mem.readInt(u64, encoded[0..@sizeOf(u64)], .little),
        .active_count = std.mem.readInt(
            u32,
            encoded[@sizeOf(u64) .. @sizeOf(u64) + @sizeOf(u32)],
            .little,
        ),
    };
}

fn transitionPhaseTerminal(phase: metadata.TransitionPhase) bool {
    return phase == .finalized or phase == .rolled_back;
}

fn transitionPhaseCanAdvance(existing: metadata.TransitionPhase, incoming: metadata.TransitionPhase) bool {
    if (transitionPhaseTerminal(existing)) return incoming == existing;
    if (existing == .rolling_back) return incoming == .rolling_back or incoming == .rolled_back;
    if (transitionPhaseTerminal(incoming)) return true;
    return @intFromEnum(incoming) >= @intFromEnum(existing);
}

fn splitTransitionUpdateAllowed(existing: metadata.SplitTransitionRecord, incoming: metadata.SplitTransitionRecord) bool {
    return existing.transition_id == incoming.transition_id and
        existing.attempt_epoch == incoming.attempt_epoch and
        existing.source_group_id == incoming.source_group_id and
        existing.destination_group_id == incoming.destination_group_id and
        existing.table_contract.eql(incoming.table_contract) and
        optionalBytesEqual(existing.split_key, incoming.split_key) and
        optionalBytesEqual(existing.source_range_end, incoming.source_range_end) and
        transitionPhaseCanAdvance(existing.phase, incoming.phase) and
        !(existing.rollback_reason != null and incoming.rollback_reason == null);
}

fn mergeTransitionUpdateAllowed(existing: metadata.MergeTransitionRecord, incoming: metadata.MergeTransitionRecord) bool {
    return existing.transition_id == incoming.transition_id and
        existing.donor_group_id == incoming.donor_group_id and
        existing.receiver_group_id == incoming.receiver_group_id and
        existing.table_contract.eql(incoming.table_contract) and
        existing.allow_doc_identity_reassignment == incoming.allow_doc_identity_reassignment and
        transitionPhaseCanAdvance(existing.phase, incoming.phase) and
        !(existing.rollback_reason != null and incoming.rollback_reason == null);
}

fn optionalBytesEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

fn keyStrictlyInsideRange(key: []const u8, start_key: []const u8, end_key: ?[]const u8) bool {
    if (std.mem.order(u8, key, start_key) != .gt) return false;
    if (end_key) |end| return std.mem.order(u8, key, end) == .lt;
    return true;
}

test "metadata raft apply store persists batches across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-apply-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 13,
            .entries_bytes = "metadata-batch",
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const batch = (try store.latestBatch(21)) orelse return error.MissingMetadataBatch;
        try std.testing.expectEqual(@as(u64, 13), batch.commit_index);
        try std.testing.expectEqualStrings("metadata-batch", batch.entries_bytes);
    }
}

test "metadata raft apply store fences transition identity and active removal" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-transition-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = testTransitionTableRecord(),
    });
    defer std.testing.allocator.free(table_cmd);
    const split_source_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            21,
            test_transition_table_contract.source_identity,
            "doc:a",
            "doc:z",
        ),
    });
    defer std.testing.allocator.free(split_source_cmd);
    const merge_donor_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            31,
            test_merge_transition_table_contract.source_identity,
            "doc:m",
            "doc:z",
        ),
    });
    defer std.testing.allocator.free(merge_donor_cmd);
    const merge_receiver_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            30,
            test_merge_transition_table_contract.target_identity,
            "doc:a",
            "doc:m",
        ),
    });
    defer std.testing.allocator.free(merge_receiver_cmd);
    const split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .admit_split_transition = .{
            .expected_source_epoch = 0,
            .record = .{
                .transition_id = 501,
                .attempt_epoch = 1,
                .source_group_id = 21,
                .destination_group_id = 22,
                .phase = .prepare,
                .split_key = "doc:m",
                .source_range_end = "doc:z",
                .rollback_reason = "slow-peer",
                .table_contract = test_transition_table_contract,
            },
        },
    });
    defer std.testing.allocator.free(split_cmd);
    const merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_merge_transition = .{
            .transition_id = 601,
            .donor_group_id = 31,
            .receiver_group_id = 30,
            .phase = .prepare,
            .allow_doc_identity_reassignment = true,
            .table_contract = test_merge_transition_table_contract,
        },
    });
    defer std.testing.allocator.free(merge_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = split_source_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = merge_donor_cmd },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = merge_receiver_cmd },
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = split_cmd },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = merge_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 8,
            .entries_bytes = encoded_entries,
        });

        const splits = try store.listSplitTransitions(std.testing.allocator, 21);
        defer store.freeSplitTransitions(std.testing.allocator, splits);
        const merges = try store.listMergeTransitions(std.testing.allocator, 21);
        defer store.freeMergeTransitions(std.testing.allocator, merges);

        try std.testing.expectEqual(@as(usize, 1), splits.len);
        try std.testing.expectEqual(@as(u64, 501), splits[0].transition_id);
        try std.testing.expectEqualStrings("doc:m", splits[0].split_key.?);
        try std.testing.expectEqualStrings("doc:z", splits[0].source_range_end.?);
        try std.testing.expectEqualStrings("slow-peer", splits[0].rollback_reason.?);
        try std.testing.expect(splits[0].table_contract.eql(
            test_transition_table_contract,
        ));
        try std.testing.expectEqual(@as(usize, 1), merges.len);
        try std.testing.expectEqual(@as(u64, 601), merges[0].transition_id);
        try std.testing.expect(merges[0].allow_doc_identity_reassignment);
        try std.testing.expect(merges[0].table_contract.eql(
            test_merge_transition_table_contract,
        ));
    }

    const conflicting_split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 501,
            .attempt_epoch = 2,
            .source_group_id = 21,
            .destination_group_id = 22,
            .phase = .prepare,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
            .table_contract = test_transition_table_contract,
        },
    });
    defer std.testing.allocator.free(conflicting_split_cmd);
    const regressive_merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_merge_transition = .{
            .transition_id = 601,
            .donor_group_id = 31,
            .receiver_group_id = 30,
            .phase = .prepare,
            .allow_doc_identity_reassignment = true,
            .table_contract = test_merge_transition_table_contract,
        },
    });
    defer std.testing.allocator.free(regressive_merge_cmd);
    const drifted_contract: metadata.TransitionTableContract = .{
        .table_id = test_transition_table_contract.table_id,
        .table_name = test_transition_table_contract.table_name,
        .schema_json = "{\"title\":{\"type\":\"text\"}}",
        .indexes_json = test_transition_table_contract.indexes_json,
        .source_identity = test_transition_table_contract.source_identity,
        .target_identity = test_transition_table_contract.target_identity,
    };
    const drifted_merge_contract: metadata.TransitionTableContract = .{
        .table_id = test_merge_transition_table_contract.table_id,
        .table_name = test_merge_transition_table_contract.table_name,
        .schema_json = "{\"title\":{\"type\":\"text\"}}",
        .indexes_json = test_merge_transition_table_contract.indexes_json,
        .source_identity = test_merge_transition_table_contract.source_identity,
        .target_identity = test_merge_transition_table_contract.target_identity,
    };
    const contract_drift_split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 501,
            .attempt_epoch = 1,
            .source_group_id = 21,
            .destination_group_id = 22,
            .phase = .replay_deltas,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
            .rollback_reason = "slow-peer",
            .table_contract = drifted_contract,
        },
    });
    defer std.testing.allocator.free(contract_drift_split_cmd);
    const contract_drift_merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_merge_transition = .{
            .transition_id = 601,
            .donor_group_id = 31,
            .receiver_group_id = 30,
            .phase = .cutover_pending,
            .allow_doc_identity_reassignment = true,
            .table_contract = drifted_merge_contract,
        },
    });
    defer std.testing.allocator.free(contract_drift_merge_cmd);
    const remove_active_split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_split_transition = .{ .transition_id = 501 },
    });
    defer std.testing.allocator.free(remove_active_split_cmd);
    const remove_active_merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_merge_transition = .{ .transition_id = 601 },
    });
    defer std.testing.allocator.free(remove_active_merge_cmd);
    const encoded_stale_commands = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 9, .entry_type = .normal, .data = conflicting_split_cmd },
        .{ .term = 2, .index = 10, .entry_type = .normal, .data = regressive_merge_cmd },
        .{ .term = 2, .index = 11, .entry_type = .normal, .data = contract_drift_split_cmd },
        .{ .term = 2, .index = 12, .entry_type = .normal, .data = contract_drift_merge_cmd },
        .{ .term = 2, .index = 13, .entry_type = .normal, .data = remove_active_split_cmd },
        .{ .term = 2, .index = 14, .entry_type = .normal, .data = remove_active_merge_cmd },
    });
    defer std.testing.allocator.free(encoded_stale_commands);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 14,
            .entries_bytes = encoded_stale_commands,
        });

        const splits = try store.listSplitTransitions(std.testing.allocator, 21);
        defer store.freeSplitTransitions(std.testing.allocator, splits);
        const merges = try store.listMergeTransitions(std.testing.allocator, 21);
        defer store.freeMergeTransitions(std.testing.allocator, merges);

        try std.testing.expectEqual(@as(usize, 1), splits.len);
        try std.testing.expectEqual(@as(u64, 1), splits[0].attempt_epoch);
        try std.testing.expectEqual(metadata.TransitionPhase.prepare, splits[0].phase);
        try std.testing.expect(splits[0].table_contract.eql(
            test_transition_table_contract,
        ));
        try std.testing.expectEqual(@as(usize, 1), merges.len);
        try std.testing.expectEqual(metadata.TransitionPhase.prepare, merges[0].phase);
        try std.testing.expect(merges[0].table_contract.eql(
            test_merge_transition_table_contract,
        ));
    }

    const finalize_split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 501,
            .attempt_epoch = 1,
            .source_group_id = 21,
            .destination_group_id = 22,
            .phase = .finalized,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
            .rollback_reason = "slow-peer",
            .table_contract = test_transition_table_contract,
        },
    });
    defer std.testing.allocator.free(finalize_split_cmd);
    const finalize_merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_merge_transition = .{
            .transition_id = 601,
            .donor_group_id = 31,
            .receiver_group_id = 30,
            .phase = .finalized,
            .allow_doc_identity_reassignment = true,
            .table_contract = test_merge_transition_table_contract,
        },
    });
    defer std.testing.allocator.free(finalize_merge_cmd);
    const encoded_terminal = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 15, .entry_type = .normal, .data = finalize_split_cmd },
        .{ .term = 2, .index = 16, .entry_type = .normal, .data = finalize_merge_cmd },
    });
    defer std.testing.allocator.free(encoded_terminal);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 16,
            .entries_bytes = encoded_terminal,
        });

        const splits = try store.listSplitTransitions(std.testing.allocator, 21);
        defer store.freeSplitTransitions(std.testing.allocator, splits);
        const merges = try store.listMergeTransitions(std.testing.allocator, 21);
        defer store.freeMergeTransitions(std.testing.allocator, merges);
        try std.testing.expectEqual(metadata.TransitionPhase.finalized, splits[0].phase);
        try std.testing.expectEqual(metadata.TransitionPhase.finalized, merges[0].phase);
    }

    const remove_split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_split_transition = .{ .transition_id = 501 },
    });
    defer std.testing.allocator.free(remove_split_cmd);
    const remove_merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_merge_transition = .{ .transition_id = 601 },
    });
    defer std.testing.allocator.free(remove_merge_cmd);
    const encoded_terminal_remove = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 3, .index = 17, .entry_type = .normal, .data = remove_split_cmd },
        .{ .term = 3, .index = 18, .entry_type = .normal, .data = remove_merge_cmd },
    });
    defer std.testing.allocator.free(encoded_terminal_remove);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 18,
            .entries_bytes = encoded_terminal_remove,
        });

        const splits = try store.listSplitTransitions(std.testing.allocator, 21);
        defer store.freeSplitTransitions(std.testing.allocator, splits);
        const merges = try store.listMergeTransitions(std.testing.allocator, 21);
        defer store.freeMergeTransitions(std.testing.allocator, merges);
        try std.testing.expectEqual(@as(usize, 0), splits.len);
        try std.testing.expectEqual(@as(usize, 0), merges.len);
    }
}

test "metadata raft apply store returns only durable transition deltas" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-committed-transition-deltas", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const split = metadata.SplitTransitionRecord{
        .transition_id = 701,
        .attempt_epoch = 1,
        .source_group_id = 71,
        .destination_group_id = 72,
        .phase = .prepare,
        .split_key = "doc:m",
        .source_range_end = "doc:z",
        .table_contract = test_transition_table_contract,
    };
    const merge = metadata.MergeTransitionRecord{
        .transition_id = 801,
        .donor_group_id = 81,
        .receiver_group_id = 80,
        .phase = .prepare,
        .table_contract = test_transition_table_contract,
    };

    const table_upsert = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = testTransitionTableRecord(),
    });
    defer std.testing.allocator.free(table_upsert);
    const split_source_upsert = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            split.source_group_id,
            test_transition_table_contract.source_identity,
            "doc:a",
            "doc:z",
        ),
    });
    defer std.testing.allocator.free(split_source_upsert);
    const merge_donor_upsert = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            merge.donor_group_id,
            test_transition_table_contract.source_identity,
            "doc:m",
            "doc:z",
        ),
    });
    defer std.testing.allocator.free(merge_donor_upsert);
    const merge_receiver_upsert = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            merge.receiver_group_id,
            test_transition_table_contract.target_identity,
            "doc:a",
            "doc:m",
        ),
    });
    defer std.testing.allocator.free(merge_receiver_upsert);
    const setup_entries = try raft_state_machine.encodeCommittedEntries(
        std.testing.allocator,
        &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_upsert },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = split_source_upsert },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = merge_donor_upsert },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = merge_receiver_upsert },
        },
    );
    defer std.testing.allocator.free(setup_entries);
    var setup = try store.applyCommittedBatch(41, 4, setup_entries);
    setup.deinit();

    const split_upsert = try encodeTransitionCommand(std.testing.allocator, .{
        .admit_split_transition = .{
            .expected_source_epoch = 0,
            .record = split,
        },
    });
    defer std.testing.allocator.free(split_upsert);
    const merge_upsert = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_merge_transition = merge });
    defer std.testing.allocator.free(merge_upsert);
    const initial_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = split_upsert },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = merge_upsert },
    });
    defer std.testing.allocator.free(initial_entries);
    var initial = try store.applyCommittedBatch(41, 6, initial_entries);
    defer initial.deinit();
    try std.testing.expectEqual(@as(usize, 2), initial.transition_deltas.items.len);
    try std.testing.expectEqual(@as(u64, 701), initial.transition_deltas.items[0].upsert_split.transition_id);
    try std.testing.expectEqual(@as(u64, 801), initial.transition_deltas.items[1].upsert_merge.transition_id);

    const split_remove = try encodeTransitionCommand(std.testing.allocator, .{ .remove_split_transition = .{ .transition_id = 701 } });
    defer std.testing.allocator.free(split_remove);
    const merge_remove = try encodeTransitionCommand(std.testing.allocator, .{ .remove_merge_transition = .{ .transition_id = 801 } });
    defer std.testing.allocator.free(merge_remove);
    const active_remove_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = split_remove },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = merge_remove },
    });
    defer std.testing.allocator.free(active_remove_entries);
    var active_remove = try store.applyCommittedBatch(41, 8, active_remove_entries);
    defer active_remove.deinit();
    try std.testing.expectEqual(@as(usize, 0), active_remove.transition_deltas.items.len);
    const active_splits = try store.listSplitTransitions(std.testing.allocator, 41);
    defer store.freeSplitTransitions(std.testing.allocator, active_splits);
    const active_merges = try store.listMergeTransitions(std.testing.allocator, 41);
    defer store.freeMergeTransitions(std.testing.allocator, active_merges);
    try std.testing.expectEqual(@as(usize, 1), active_splits.len);
    try std.testing.expectEqual(@as(usize, 1), active_merges.len);

    var rolled_back_split = split;
    rolled_back_split.phase = .rolled_back;
    rolled_back_split.rollback_reason = "operator cancel";
    var rolled_back_merge = merge;
    rolled_back_merge.phase = .rolled_back;
    rolled_back_merge.rollback_reason = "operator cancel";
    const split_terminal = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_split_transition = rolled_back_split });
    defer std.testing.allocator.free(split_terminal);
    const merge_terminal = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_merge_transition = rolled_back_merge });
    defer std.testing.allocator.free(merge_terminal);
    const terminal_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = split_terminal },
        .{ .term = 1, .index = 10, .entry_type = .normal, .data = merge_terminal },
    });
    defer std.testing.allocator.free(terminal_entries);
    var terminal = try store.applyCommittedBatch(41, 10, terminal_entries);
    defer terminal.deinit();
    try std.testing.expectEqual(@as(usize, 2), terminal.transition_deltas.items.len);
    try std.testing.expectEqual(metadata.TransitionPhase.rolled_back, terminal.transition_deltas.items[0].upsert_split.phase);
    try std.testing.expectEqual(metadata.TransitionPhase.rolled_back, terminal.transition_deltas.items[1].upsert_merge.phase);

    const terminal_remove_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 11, .entry_type = .normal, .data = split_remove },
        .{ .term = 1, .index = 12, .entry_type = .normal, .data = merge_remove },
    });
    defer std.testing.allocator.free(terminal_remove_entries);
    var terminal_remove = try store.applyCommittedBatch(41, 12, terminal_remove_entries);
    defer terminal_remove.deinit();
    try std.testing.expectEqual(@as(usize, 2), terminal_remove.transition_deltas.items.len);
    try std.testing.expectEqual(@as(u64, 701), terminal_remove.transition_deltas.items[0].remove_split);
    try std.testing.expectEqual(@as(u64, 801), terminal_remove.transition_deltas.items[1].remove_merge);
}

test "metadata raft apply store publishes listeners only after commit" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-post-commit-listeners", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const Capture = struct {
        projections: usize = 0,
        keys: usize = 0,

        fn onProjection(ptr: *anyopaque, _: ProjectionSignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.projections += 1;
        }

        fn matchesKey(_: *anyopaque, _: CommittedKeySignal) bool {
            return true;
        }

        fn onKey(ptr: *anyopaque, _: CommittedKeySignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.keys += 1;
        }
    };

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    var capture = Capture{};
    try store.addProjectionListener(.{ .ptr = &capture, .vtable = &.{ .on_projection_signal = Capture.onProjection } });
    try store.addCommittedKeyListener(.{
        .ptr = &capture,
        .vtable = &.{
            .matches_key = Capture.matchesKey,
            .on_committed_key = Capture.onKey,
        },
    });

    const table = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_table = .{
        .table_id = 91,
        .name = "docs",
    } });
    defer std.testing.allocator.free(table);
    const invalid_split = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_split_transition = .{
        .transition_id = 901,
        .attempt_epoch = 1,
        .source_group_id = 91,
        .destination_group_id = group_ids.main_metadata_group_id,
        .phase = .prepare,
        .table_contract = test_transition_table_contract,
    } });
    defer std.testing.allocator.free(invalid_split);
    const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = invalid_split },
    });
    defer std.testing.allocator.free(entries);

    try std.testing.expectError(error.ReservedGroupId, store.applyCommittedBatch(41, 2, entries));
    try std.testing.expectEqual(@as(usize, 0), capture.projections);
    try std.testing.expectEqual(@as(usize, 0), capture.keys);
    try std.testing.expectEqual(@as(?AppliedMetadataBatch, null), try store.latestBatch(41));
    const tables = try store.listTables(std.testing.allocator, 41);
    defer store.freeTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 0), tables.len);
}

test "metadata raft apply store resolves stale store drain intent at apply time" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-drain-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 9 } });
    defer std.testing.allocator.free(draining_node_cmd);
    const draining_registration_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_store = .{ .store_id = 9, .node_id = 9, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(draining_registration_cmd);
    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .cancel_node_shutdown = .{ .node_id = 9 } });
    defer std.testing.allocator.free(active_node_cmd);
    const stale_draining_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_store = .{ .store_id = 9, .node_id = 9, .role = "data", .health_class = "healthy", .live = true, .drain_requested = true },
    });
    defer std.testing.allocator.free(stale_draining_store_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = draining_node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = draining_registration_cmd },
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = stale_draining_store_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 4,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 21);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 21);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(nodes[0].lifecycle));
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expect(!stores[0].drain_requested);
}

test "metadata raft apply store preserves node drain across store upsert" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-upsert-drain-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const registered_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_store = .{ .store_id = 12, .node_id = 12, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(registered_store_cmd);
    const draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 12 } });
    defer std.testing.allocator.free(draining_node_cmd);
    const stale_active_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{ .store_id = 12, .node_id = 12, .role = "data", .health_class = "healthy", .live = true, .drain_requested = false },
    });
    defer std.testing.allocator.free(stale_active_store_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = registered_store_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = draining_node_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = stale_active_store_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 24,
        .commit_index = 3,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 24);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 24);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(!metadata_table_manager.nodeLifecycleActive(nodes[0].lifecycle));
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expect(stores[0].drain_requested);
}

test "metadata raft apply store admits one split epoch atomically and retries idempotently" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-split-admission", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = testTransitionTableRecord(),
    });
    defer std.testing.allocator.free(table_cmd);
    const source_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_range = .{
        .group_id = 101,
        .range_id = test_transition_table_contract.source_identity.range_id,
        .table_id = test_transition_table_contract.table_id,
        .start_key = "doc:a",
        .end_key = "doc:z",
        .doc_identity_shard_id = test_transition_table_contract.source_identity.shard_id,
        .doc_identity_range_id = test_transition_table_contract.source_identity.range_id,
    } });
    defer std.testing.allocator.free(source_cmd);
    const first_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .admit_split_transition = .{
        .expected_source_epoch = 0,
        .record = .{
            .transition_id = 7001,
            .attempt_epoch = 1,
            .source_group_id = 101,
            .destination_group_id = 103,
            .phase = .prepare,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
            .table_contract = test_transition_table_contract,
        },
    } });
    defer std.testing.allocator.free(first_cmd);
    const competing_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .admit_split_transition = .{
        .expected_source_epoch = 0,
        .record = .{
            .transition_id = 7002,
            .attempt_epoch = 1,
            .source_group_id = 101,
            .destination_group_id = 104,
            .phase = .prepare,
            .split_key = "doc:n",
            .source_range_end = "doc:z",
            .table_contract = test_transition_table_contract,
        },
    } });
    defer std.testing.allocator.free(competing_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = source_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = first_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = competing_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = first_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 5,
        .entries_bytes = encoded_entries,
    });

    const ranges = try store.listRanges(std.testing.allocator, 21);
    defer store.freeRanges(std.testing.allocator, ranges);
    const transitions = try store.listSplitTransitions(std.testing.allocator, 21);
    defer store.freeSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 1), ranges[0].split_attempt_epoch);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    try std.testing.expectEqual(@as(u64, 7001), transitions[0].transition_id);
    try std.testing.expectEqual(@as(u64, 1), transitions[0].attempt_epoch);
}

test "metadata raft apply store atomically fences table replacement during a range transition" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-table-transition-fence", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const expected = metadata.TableRecord{
        .table_id = test_transition_table_contract.table_id,
        .name = test_transition_table_contract.table_name,
        .schema_json = test_transition_table_contract.schema_json,
        .indexes_json = test_transition_table_contract.indexes_json,
    };
    var replacement = expected;
    replacement.description = "replacement";
    replacement.schema_json = "{\"title\":{\"type\":\"text\"}}";

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_table = expected });
    defer std.testing.allocator.free(table_cmd);
    const source_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            101,
            test_transition_table_contract.source_identity,
            "doc:a",
            "doc:z",
        ),
    });
    defer std.testing.allocator.free(source_cmd);
    const split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .admit_split_transition = .{
            .expected_source_epoch = 0,
            .record = .{
                .transition_id = 7101,
                .attempt_epoch = 1,
                .source_group_id = 101,
                .destination_group_id = 102,
                .phase = .prepare,
                .split_key = "doc:m",
                .source_range_end = "doc:z",
                .table_contract = test_transition_table_contract,
            },
        },
    });
    defer std.testing.allocator.free(split_cmd);
    const replace_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .compare_and_replace_table = .{
            .expected = expected,
            .replacement = replacement,
        },
    });
    defer std.testing.allocator.free(replace_cmd);
    const active_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = source_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = split_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = replace_cmd },
    });
    defer std.testing.allocator.free(active_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 4,
        .entries_bytes = active_entries,
    });

    var table = (try store.getTable(std.testing.allocator, 21, expected.table_id)).?;
    defer metadata_table_manager.freeTable(std.testing.allocator, table);
    try std.testing.expectEqualStrings("", table.description);
    const active_fence = try store.getTableTransitionFence(21, expected.table_id);
    try std.testing.expectEqual(@as(u64, 1), active_fence.generation);
    try std.testing.expectEqual(@as(u32, 1), active_fence.active_count);

    const terminal_split = metadata.SplitTransitionRecord{
        .transition_id = 7101,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 102,
        .phase = .finalized,
        .split_key = "doc:m",
        .source_range_end = "doc:z",
        .table_contract = test_transition_table_contract,
    };
    const terminal_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_split_transition = terminal_split });
    defer std.testing.allocator.free(terminal_cmd);
    const terminal_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = terminal_cmd },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = replace_cmd },
    });
    defer std.testing.allocator.free(terminal_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 6,
        .entries_bytes = terminal_entries,
    });

    metadata_table_manager.freeTable(std.testing.allocator, table);
    table = (try store.getTable(std.testing.allocator, 21, expected.table_id)).?;
    try std.testing.expectEqualStrings("replacement", table.description);
    const terminal_fence = try store.getTableTransitionFence(21, expected.table_id);
    try std.testing.expectEqual(@as(u64, 2), terminal_fence.generation);
    try std.testing.expectEqual(@as(u32, 0), terminal_fence.active_count);

    const stale_admission_cmd = try encodeTransitionCommand(
        std.testing.allocator,
        .{ .admit_split_transition = .{
            .expected_source_epoch = 1,
            .record = .{
                .transition_id = 7102,
                .attempt_epoch = 2,
                .source_group_id = 101,
                .destination_group_id = 103,
                .phase = .prepare,
                .split_key = "doc:n",
                .source_range_end = "doc:z",
                .table_contract = test_transition_table_contract,
            },
        } },
    );
    defer std.testing.allocator.free(stale_admission_cmd);
    const stale_admission_entries =
        try raft_state_machine.encodeCommittedEntries(
            std.testing.allocator,
            &.{.{
                .term = 1,
                .index = 7,
                .entry_type = .normal,
                .data = stale_admission_cmd,
            }},
        );
    defer std.testing.allocator.free(stale_admission_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 7,
        .entries_bytes = stale_admission_entries,
    });

    const transitions = try store.listSplitTransitions(
        std.testing.allocator,
        21,
    );
    defer store.freeSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const ranges = try store.listRanges(std.testing.allocator, 21);
    defer store.freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 1), ranges[0].split_attempt_epoch);

    const donor_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            201,
            test_transition_table_contract.source_identity,
            "doc:m",
            "doc:z",
        ),
    });
    defer std.testing.allocator.free(donor_cmd);
    const receiver_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            202,
            test_transition_table_contract.target_identity,
            "doc:a",
            "doc:m",
        ),
    });
    defer std.testing.allocator.free(receiver_cmd);
    const stale_merge_cmd = try encodeTransitionCommand(
        std.testing.allocator,
        .{ .upsert_merge_transition = .{
            .transition_id = 7201,
            .donor_group_id = 201,
            .receiver_group_id = 202,
            .phase = .prepare,
            .table_contract = test_transition_table_contract,
        } },
    );
    defer std.testing.allocator.free(stale_merge_cmd);
    const stale_merge_entries = try raft_state_machine.encodeCommittedEntries(
        std.testing.allocator,
        &.{
            .{ .term = 1, .index = 8, .entry_type = .normal, .data = donor_cmd },
            .{ .term = 1, .index = 9, .entry_type = .normal, .data = receiver_cmd },
            .{ .term = 1, .index = 10, .entry_type = .normal, .data = stale_merge_cmd },
        },
    );
    defer std.testing.allocator.free(stale_merge_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 10,
        .entries_bytes = stale_merge_entries,
    });
    const merges = try store.listMergeTransitions(std.testing.allocator, 21);
    defer store.freeMergeTransitions(std.testing.allocator, merges);
    try std.testing.expectEqual(@as(usize, 0), merges.len);
}

test "metadata raft apply store table removal requires the observed inactive transition generation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        std.testing.allocator,
        ".zig-cache/tmp/{s}/metadata-table-removal-transition-fence",
        .{tmp.sub_path},
    );
    defer std.testing.allocator.free(root);

    const table = metadata.TableRecord{
        .table_id = test_transition_table_contract.table_id,
        .name = test_transition_table_contract.table_name,
        .schema_json = test_transition_table_contract.schema_json,
        .indexes_json = test_transition_table_contract.indexes_json,
    };
    const table_cmd = try encodeTransitionCommand(
        std.testing.allocator,
        .{ .upsert_table = table },
    );
    defer std.testing.allocator.free(table_cmd);
    const source_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            101,
            test_transition_table_contract.source_identity,
            "doc:a",
            "doc:z",
        ),
    });
    defer std.testing.allocator.free(source_cmd);
    const split = metadata.SplitTransitionRecord{
        .transition_id = 7301,
        .attempt_epoch = 1,
        .source_group_id = 101,
        .destination_group_id = 102,
        .phase = .prepare,
        .split_key = "doc:m",
        .source_range_end = "doc:z",
        .table_contract = test_transition_table_contract,
    };
    const split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .admit_split_transition = .{
            .expected_source_epoch = 0,
            .record = split,
        },
    });
    defer std.testing.allocator.free(split_cmd);
    const generation_zero_remove = try encodeTransitionCommand(
        std.testing.allocator,
        .{ .remove_table = .{
            .table_id = table.table_id,
            .expected_transition_generation = 0,
        } },
    );
    defer std.testing.allocator.free(generation_zero_remove);

    const active_entries = try raft_state_machine.encodeCommittedEntries(
        std.testing.allocator,
        &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = source_cmd },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = split_cmd },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = generation_zero_remove },
        },
    );
    defer std.testing.allocator.free(active_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 22,
        .commit_index = 4,
        .entries_bytes = active_entries,
    });
    var current = (try store.getTable(
        std.testing.allocator,
        22,
        table.table_id,
    )).?;
    metadata_table_manager.freeTable(std.testing.allocator, current);

    var terminal_split = split;
    terminal_split.phase = .finalized;
    const terminal_cmd = try encodeTransitionCommand(
        std.testing.allocator,
        .{ .upsert_split_transition = terminal_split },
    );
    defer std.testing.allocator.free(terminal_cmd);
    const stale_remove = try encodeTransitionCommand(
        std.testing.allocator,
        .{ .remove_table = .{
            .table_id = table.table_id,
            .expected_transition_generation = 1,
        } },
    );
    defer std.testing.allocator.free(stale_remove);
    const current_remove = try encodeTransitionCommand(
        std.testing.allocator,
        .{ .remove_table = .{
            .table_id = table.table_id,
            .expected_transition_generation = 2,
        } },
    );
    defer std.testing.allocator.free(current_remove);
    const terminal_entries = try raft_state_machine.encodeCommittedEntries(
        std.testing.allocator,
        &.{
            .{ .term = 1, .index = 5, .entry_type = .normal, .data = terminal_cmd },
            .{ .term = 1, .index = 6, .entry_type = .normal, .data = stale_remove },
        },
    );
    defer std.testing.allocator.free(terminal_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 22,
        .commit_index = 6,
        .entries_bytes = terminal_entries,
    });
    current = (try store.getTable(
        std.testing.allocator,
        22,
        table.table_id,
    )).?;
    metadata_table_manager.freeTable(std.testing.allocator, current);

    const remove_entries = try raft_state_machine.encodeCommittedEntries(
        std.testing.allocator,
        &.{.{
            .term = 1,
            .index = 7,
            .entry_type = .normal,
            .data = current_remove,
        }},
    );
    defer std.testing.allocator.free(remove_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 22,
        .commit_index = 7,
        .entries_bytes = remove_entries,
    });
    try std.testing.expect((try store.getTable(
        std.testing.allocator,
        22,
        table.table_id,
    )) == null);
}

test "metadata raft apply store ignores stale drained first store registration after cancellation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-first-drain-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 10 } });
    defer std.testing.allocator.free(draining_node_cmd);
    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .cancel_node_shutdown = .{ .node_id = 10 } });
    defer std.testing.allocator.free(active_node_cmd);
    const stale_draining_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_store = .{ .store_id = 10, .node_id = 10, .role = "data", .health_class = "healthy", .live = true, .drain_requested = true },
    });
    defer std.testing.allocator.free(stale_draining_store_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = draining_node_cmd },
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = stale_draining_store_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 22,
        .commit_index = 3,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 22);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 22);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(nodes[0].lifecycle));
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expect(!stores[0].drain_requested);
}

test "metadata raft apply store fences stale placement plans across drain cancellation and finalization" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const source_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-draining-placement-fence-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_root);
    const restored_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-draining-placement-fence-restored", .{tmp.sub_path});
    defer std.testing.allocator.free(restored_root);

    const register_node = try encodeTransitionCommand(std.testing.allocator, .{ .register_node = .{
        .node_id = 12,
        .role = "data",
        .lifecycle = metadata_table_manager.node_lifecycle_active,
    } });
    defer std.testing.allocator.free(register_node);
    const register_store = try encodeTransitionCommand(std.testing.allocator, .{ .register_store = .{
        .store_id = 12,
        .node_id = 12,
        .role = "data",
        .health_class = "healthy",
        .live = true,
    } });
    defer std.testing.allocator.free(register_store);
    const create_serving = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_replica_intent = .{
        .expected_metadata_version = null,
        .expected_version_fence = 0,
        .expected_target_drain_requested = false,
        .replacement = .{
            .record = .{ .group_id = 1201, .replica_id = 1, .local_node_id = 12 },
            .store_id = 12,
            .peer_node_ids = &.{12},
        },
    } });
    defer std.testing.allocator.free(create_serving);
    const recreate_serving_v3 = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_replica_intent = .{
        .expected_metadata_version = null,
        .expected_version_fence = 3,
        .expected_target_drain_requested = false,
        .replacement = .{
            .record = .{ .group_id = 1201, .replica_id = 1, .local_node_id = 12 },
            .store_id = 12,
            .peer_node_ids = &.{12},
        },
    } });
    defer std.testing.allocator.free(recreate_serving_v3);
    const request_shutdown = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 12 } });
    defer std.testing.allocator.free(request_shutdown);
    const cancel_shutdown = try encodeTransitionCommand(std.testing.allocator, .{ .cancel_node_shutdown = .{ .node_id = 12 } });
    defer std.testing.allocator.free(cancel_shutdown);
    const finalize_shutdown = try encodeTransitionCommand(std.testing.allocator, .{ .finalize_node_shutdown = .{ .node_id = 12 } });
    defer std.testing.allocator.free(finalize_shutdown);
    const stale_serving_v1 = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_replica_intent = .{
        .expected_metadata_version = 1,
        .expected_version_fence = 1,
        .expected_target_drain_requested = false,
        .replacement = .{
            .record = .{ .group_id = 1201, .replica_id = 1, .local_node_id = 12, .metadata_version = 1 },
            .store_id = 12,
            .peer_node_ids = &.{ 12, 14 },
        },
    } });
    defer std.testing.allocator.free(stale_serving_v1);
    const draining_v1 = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_replica_intent = .{
        .expected_metadata_version = 1,
        .expected_version_fence = 1,
        .expected_target_drain_requested = true,
        .replacement = .{
            .record = .{ .group_id = 1201, .replica_id = 1, .local_node_id = 12, .metadata_version = 1 },
            .store_id = 12,
            .peer_node_ids = &.{ 12, 13 },
            .serving_state = .draining,
        },
    } });
    defer std.testing.allocator.free(draining_v1);
    const remove_v2 = try encodeTransitionCommand(std.testing.allocator, .{ .remove_replica_intent = .{
        .group_id = 1201,
        .local_node_id = 12,
        .expected_metadata_version = 2,
    } });
    defer std.testing.allocator.free(remove_v2);
    const draining_v4 = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_replica_intent = .{
        .expected_metadata_version = 4,
        .expected_version_fence = 4,
        .expected_target_drain_requested = true,
        .replacement = .{
            .record = .{ .group_id = 1201, .replica_id = 1, .local_node_id = 12, .metadata_version = 4 },
            .store_id = 12,
            .peer_node_ids = &.{ 12, 13 },
            .serving_state = .draining,
        },
    } });
    defer std.testing.allocator.free(draining_v4);
    const remove_v5 = try encodeTransitionCommand(std.testing.allocator, .{ .remove_replica_intent = .{
        .group_id = 1201,
        .local_node_id = 12,
        .expected_metadata_version = 5,
    } });
    defer std.testing.allocator.free(remove_v5);

    const drain_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = register_node },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = register_store },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = create_serving },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = request_shutdown },
        // A pre-shutdown serving plan cannot overwrite the drain request.
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = stale_serving_v1 },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = draining_v1 },
        // The same plan cannot overwrite the version it just advanced.
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = draining_v1 },
    });
    defer std.testing.allocator.free(drain_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = source_root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 7,
        .entries_bytes = drain_entries,
    });
    {
        const intents = try store.listPlacementIntents(std.testing.allocator, 21);
        defer store.freePlacementIntents(std.testing.allocator, intents);
        try std.testing.expectEqual(@as(usize, 1), intents.len);
        try std.testing.expectEqual(@as(u64, 2), intents[0].record.metadata_version);
        try std.testing.expectEqual(raft_reconciler.PlacementServingState.draining, intents[0].serving_state);
        try std.testing.expectEqualSlices(u64, &.{ 12, 13 }, intents[0].peer_node_ids);
    }

    const pre_snapshot_cancel_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 8, .entry_type = .normal, .data = remove_v2 },
        .{ .term = 2, .index = 9, .entry_type = .normal, .data = cancel_shutdown },
        // The cancelled drain rejects its old expected drain state.
        .{ .term = 2, .index = 10, .entry_type = .normal, .data = draining_v1 },
    });
    defer std.testing.allocator.free(pre_snapshot_cancel_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 10,
        .entries_bytes = pre_snapshot_cancel_entries,
    });
    const snapshot = try store.snapshotBuilder().buildSnapshot(std.testing.allocator, 21);
    defer std.testing.allocator.free(snapshot);

    var restored = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = restored_root });
    defer restored.deinit();
    try std.testing.expect(try restored.snapshotBuilder().installSnapshot(std.testing.allocator, 21, 10, snapshot));
    const post_snapshot_cancel_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        // A stale absence plan is fenced after the intervening create/remove.
        .{ .term = 2, .index = 11, .entry_type = .normal, .data = create_serving },
        // A freshly observed absence can create the next incarnation.
        .{ .term = 2, .index = 12, .entry_type = .normal, .data = recreate_serving_v3 },
        // An old removal cannot delete the new placement incarnation.
        .{ .term = 2, .index = 13, .entry_type = .normal, .data = remove_v2 },
    });
    defer std.testing.allocator.free(post_snapshot_cancel_entries);
    try restored.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 13,
        .entries_bytes = post_snapshot_cancel_entries,
    });
    {
        const intents = try restored.listPlacementIntents(std.testing.allocator, 21);
        defer restored.freePlacementIntents(std.testing.allocator, intents);
        try std.testing.expectEqual(@as(usize, 1), intents.len);
        try std.testing.expectEqual(@as(u64, 4), intents[0].record.metadata_version);
        try std.testing.expectEqual(raft_reconciler.PlacementServingState.serving, intents[0].serving_state);
        const fences = try restored.listPlacementVersionFences(std.testing.allocator, 21);
        defer std.testing.allocator.free(fences);
        try std.testing.expectEqual(@as(usize, 1), fences.len);
        try std.testing.expectEqual(@as(u64, 1201), fences[0].group_id);
        try std.testing.expectEqual(@as(u64, 12), fences[0].local_node_id);
        try std.testing.expectEqual(@as(u64, 4), fences[0].version);
    }

    const finalize_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 3, .index = 14, .entry_type = .normal, .data = request_shutdown },
        .{ .term = 3, .index = 15, .entry_type = .normal, .data = draining_v4 },
        .{ .term = 3, .index = 16, .entry_type = .normal, .data = remove_v5 },
        .{ .term = 3, .index = 17, .entry_type = .normal, .data = finalize_shutdown },
        // Missing finalized ownership rejects even an absence-matching create.
        .{ .term = 3, .index = 18, .entry_type = .normal, .data = create_serving },
    });
    defer std.testing.allocator.free(finalize_entries);
    try restored.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 18,
        .entries_bytes = finalize_entries,
    });
    const final_intents = try restored.listPlacementIntents(std.testing.allocator, 21);
    defer restored.freePlacementIntents(std.testing.allocator, final_intents);
    try std.testing.expectEqual(@as(usize, 0), final_intents.len);
}

test "metadata raft apply store ignores stale draining node registration after cancellation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-node-stale-drain-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 11 } });
    defer std.testing.allocator.free(draining_node_cmd);
    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .cancel_node_shutdown = .{ .node_id = 11 } });
    defer std.testing.allocator.free(active_node_cmd);
    const stale_draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_node = .{ .node_id = 11, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_draining },
    });
    defer std.testing.allocator.free(stale_draining_node_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = draining_node_cmd },
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = stale_draining_node_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 23,
        .commit_index = 3,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 23);
    defer store.freeNodes(std.testing.allocator, nodes);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(nodes[0].lifecycle));
}

test "metadata raft apply store waits for termination debt before finalizing node shutdown" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-node-finalize-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const target_statuses = [_]metadata.GroupStatusReport{.{
        .group_id = 41,
        .updated_at_millis = 10,
        .local_voter = true,
        .voter_count = 1,
    }};
    const foreign_runtime_statuses = [_]metadata.RuntimeGroupStatusReport{.{
        .group_id = 99,
        .node_id = 12,
        .store_id = 99,
    }};
    const target_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_node = .{ .node_id = 12, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_draining },
    });
    defer std.testing.allocator.free(target_node_cmd);
    const other_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_node = .{ .node_id = 13, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
    });
    defer std.testing.allocator.free(other_node_cmd);
    const target_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{
            .store_id = 12,
            .node_id = 12,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .drain_requested = true,
            .group_statuses = @constCast(target_statuses[0..]),
        },
    });
    defer std.testing.allocator.free(target_store_cmd);
    const other_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{ .store_id = 13, .node_id = 13, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(other_store_cmd);
    const placement_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .upsert_replica_intent = .{
        .expected_metadata_version = null,
        .expected_version_fence = 0,
        .expected_target_drain_requested = true,
        .replacement = .{
            .record = .{ .group_id = 41, .replica_id = 1, .local_node_id = 12 },
            .store_id = 12,
            .peer_node_ids = &.{12},
            .serving_state = .draining,
        },
    } });
    defer std.testing.allocator.free(placement_cmd);
    const remove_placement_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .remove_replica_intent = .{
        .group_id = 41,
        .local_node_id = 12,
        .expected_metadata_version = 1,
    } });
    defer std.testing.allocator.free(remove_placement_cmd);
    const clear_target_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{
            .store_id = 12,
            .node_id = 12,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .drain_requested = true,
            .runtime_statuses = @constCast(foreign_runtime_statuses[0..]),
        },
    });
    defer std.testing.allocator.free(clear_target_store_cmd);
    const finalize_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .finalize_node_shutdown = .{ .node_id = 12 } });
    defer std.testing.allocator.free(finalize_cmd);
    const retry_shutdown_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 12 } });
    defer std.testing.allocator.free(retry_shutdown_cmd);

    const initial_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = target_node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = other_node_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = target_store_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = other_store_cmd },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = placement_cmd },
        .{ .term = 2, .index = 6, .entry_type = .normal, .data = finalize_cmd },
        // Retrying the original request after finalization was accepted must
        // not erase the durable completion intent.
        .{ .term = 2, .index = 7, .entry_type = .normal, .data = retry_shutdown_cmd },
    });
    defer std.testing.allocator.free(initial_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 24,
        .commit_index = 7,
        .entries_bytes = initial_entries,
    });
    {
        const nodes = try store.listNodes(std.testing.allocator, 24);
        defer store.freeNodes(std.testing.allocator, nodes);
        const stores = try store.listStores(std.testing.allocator, 24);
        defer store.freeStores(std.testing.allocator, stores);
        try std.testing.expectEqual(@as(usize, 2), nodes.len);
        try std.testing.expectEqual(@as(usize, 2), stores.len);
        const target = if (nodes[0].node_id == 12) nodes[0] else nodes[1];
        try std.testing.expect(metadata_table_manager.nodeLifecycleFinalizing(target.lifecycle));
    }

    // Removing the durable placement still leaves the store's last observed
    // runtime/membership debt. The original accepted finalization intent stays
    // durable; no second finalize command is needed.
    const placement_removed_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 8, .entry_type = .normal, .data = remove_placement_cmd },
    });
    defer std.testing.allocator.free(placement_removed_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 24,
        .commit_index = 8,
        .entries_bytes = placement_removed_entries,
    });
    {
        const nodes = try store.listNodes(std.testing.allocator, 24);
        defer store.freeNodes(std.testing.allocator, nodes);
        try std.testing.expectEqual(@as(usize, 2), nodes.len);
        const target = if (nodes[0].node_id == 12) nodes[0] else nodes[1];
        try std.testing.expect(metadata_table_manager.nodeLifecycleFinalizing(target.lifecycle));
    }

    // Clearing the final store observation transactionally completes the
    // pending finalization without mutation retries or a polling scan.
    const drained_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 9, .entry_type = .normal, .data = clear_target_store_cmd },
    });
    defer std.testing.allocator.free(drained_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 24,
        .commit_index = 9,
        .entries_bytes = drained_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 24);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 24);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expectEqual(@as(u64, 13), nodes[0].node_id);
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expectEqual(@as(u64, 13), stores[0].store_id);
}

test "metadata raft apply store commits active node finalization as a no-op" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-node-finalize-active-reject", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_node = .{ .node_id = 14, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
    });
    defer std.testing.allocator.free(active_node_cmd);
    const active_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{ .store_id = 14, .node_id = 14, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(active_store_cmd);
    const finalize_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .finalize_node_shutdown = .{ .node_id = 14 } });
    defer std.testing.allocator.free(finalize_cmd);

    const initial_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = active_store_cmd },
    });
    defer std.testing.allocator.free(initial_entries);
    const finalize_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = finalize_cmd },
    });
    defer std.testing.allocator.free(finalize_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 25,
        .commit_index = 2,
        .entries_bytes = initial_entries,
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 25,
        .commit_index = 3,
        .entries_bytes = finalize_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 25);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 25);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expectEqual(@as(u64, 14), nodes[0].node_id);
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expectEqual(@as(u64, 14), stores[0].store_id);
}

test "metadata raft apply store commits active store-only finalization as a no-op" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-only-finalize-active-reject", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const active_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{ .store_id = 15, .node_id = 15, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(active_store_cmd);
    const finalize_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .finalize_node_shutdown = .{ .node_id = 15 } });
    defer std.testing.allocator.free(finalize_cmd);

    const initial_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = active_store_cmd },
    });
    defer std.testing.allocator.free(initial_entries);
    const finalize_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = finalize_cmd },
    });
    defer std.testing.allocator.free(finalize_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 26,
        .commit_index = 1,
        .entries_bytes = initial_entries,
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 26,
        .commit_index = 2,
        .entries_bytes = finalize_entries,
    });

    const stores = try store.listStores(std.testing.allocator, 26);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expectEqual(@as(u64, 15), stores[0].store_id);
}

test "metadata raft apply store projects table and range records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-topology-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "docs",
            .description = "docs table",
            .schema_json = "{\"kind\":\"demo\"}",
            .indexes_json = "{\"default\":{}}",
            .replication_sources_json = "[\"seed\"]",
            .desired_replica_count = 5,
            .min_ranges = 2,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 4101,
            .range_id = 4101,
            .table_id = 41,
            .start_key = "doc:a",
            .end_key = "doc:z",
            .doc_identity_shard_id = 4001,
            .doc_identity_range_id = 9001,
            .restore_backup_id = "nightly",
            .restore_artifact_backup_id = "nightly-artifacts",
            .restore_location = "s3://backups/tables/docs",
            .restore_snapshot_path = "nightly/groups/4101.afb",
            .restore_connection = "production-backups",
            .restore_artifact_size_bytes = 4096,
            .restore_artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
    });
    defer std.testing.allocator.free(range_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    const tables = try store.listTables(std.testing.allocator, 41);
    defer store.freeTables(std.testing.allocator, tables);
    const ranges = try store.listRanges(std.testing.allocator, 41);
    defer store.freeRanges(std.testing.allocator, ranges);

    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(u64, 41), tables[0].table_id);
    try std.testing.expectEqualStrings("docs", tables[0].name);
    try std.testing.expectEqualStrings("docs table", tables[0].description);
    try std.testing.expectEqualStrings("{\"kind\":\"demo\"}", tables[0].schema_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", tables[0].indexes_json);
    try std.testing.expectEqualStrings("[\"seed\"]", tables[0].replication_sources_json);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 4101), ranges[0].group_id);
    try std.testing.expectEqual(@as(u64, 4001), ranges[0].doc_identity_shard_id);
    try std.testing.expectEqual(@as(u64, 9001), ranges[0].doc_identity_range_id);
    try std.testing.expectEqualStrings("doc:a", ranges[0].start_key);
    try std.testing.expectEqualStrings("nightly", ranges[0].restore_backup_id);
    try std.testing.expectEqualStrings("nightly-artifacts", ranges[0].restore_artifact_backup_id);
    try std.testing.expectEqualStrings("s3://backups/tables/docs", ranges[0].restore_location);
    try std.testing.expectEqualStrings("nightly/groups/4101.afb", ranges[0].restore_snapshot_path);
    try std.testing.expectEqualStrings("production-backups", ranges[0].restore_connection);
    try std.testing.expectEqual(@as(u64, 4096), ranges[0].restore_artifact_size_bytes);
    try std.testing.expectEqualStrings(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ranges[0].restore_artifact_sha256,
    );
}

test "metadata raft apply store completes only the matching restore intent and preserves topology" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-restore-cas-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    const metadata_group_id: u64 = 41;
    const data_group_id: u64 = 4101;
    const old_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const new_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{ .table_id = 41, .name = "docs" },
    });
    defer std.testing.allocator.free(table_cmd);
    const old_range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = data_group_id,
            .range_id = 4101,
            .table_id = 41,
            .start_key = "a",
            .end_key = "m",
            .restore_backup_id = "old",
            .restore_location = "s3://backups/old",
            .restore_snapshot_path = "old/groups/4101.afb",
            .restore_connection = "backup-store",
            .restore_artifact_size_bytes = 100,
            .restore_artifact_sha256 = old_hash,
        },
    });
    defer std.testing.allocator.free(old_range_cmd);
    const initial_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = old_range_cmd },
    });
    defer std.testing.allocator.free(initial_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = metadata_group_id,
        .commit_index = 2,
        .entries_bytes = initial_entries,
    });

    const replacement_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = data_group_id,
            .range_id = 4201,
            .table_id = 41,
            .start_key = "b",
            .end_key = "z",
            .doc_identity_shard_id = 77,
            .doc_identity_range_id = 88,
            .split_attempt_epoch = 9,
            .restore_backup_id = "new",
            .restore_artifact_backup_id = "new-artifact-namespace",
            .restore_location = "s3://backups/new",
            .restore_snapshot_path = "new/groups/4101.afb",
            .restore_connection = "backup-store",
            .restore_artifact_size_bytes = 200,
            .restore_artifact_sha256 = new_hash,
        },
    });
    defer std.testing.allocator.free(replacement_cmd);
    const stale_completion_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .complete_restore_range = .{
            .group_id = data_group_id,
            .table_id = 41,
            .backup_id = "old",
            .artifact_backup_id = "old",
            .location = "s3://backups/old",
            .snapshot_path = "old/groups/4101.afb",
            .connection = "backup-store",
            .artifact_size_bytes = 100,
            .artifact_sha256 = old_hash,
        },
    });
    defer std.testing.allocator.free(stale_completion_cmd);
    const replacement_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = replacement_cmd },
        .{ .term = 2, .index = 4, .entry_type = .normal, .data = stale_completion_cmd },
    });
    defer std.testing.allocator.free(replacement_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = metadata_group_id,
        .commit_index = 4,
        .entries_bytes = replacement_entries,
    });

    {
        const ranges = try store.listRanges(std.testing.allocator, metadata_group_id);
        defer store.freeRanges(std.testing.allocator, ranges);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqualStrings("new", ranges[0].restore_backup_id);
        try std.testing.expectEqualStrings("new-artifact-namespace", ranges[0].restore_artifact_backup_id);
        try std.testing.expectEqualStrings(new_hash, ranges[0].restore_artifact_sha256);
    }

    const matching_completion_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .complete_restore_range = .{
            .group_id = data_group_id,
            .table_id = 41,
            .backup_id = "new",
            .artifact_backup_id = "new-artifact-namespace",
            .location = "s3://backups/new",
            .snapshot_path = "new/groups/4101.afb",
            .connection = "backup-store",
            .artifact_size_bytes = 200,
            .artifact_sha256 = new_hash,
        },
    });
    defer std.testing.allocator.free(matching_completion_cmd);
    const completion_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 3, .index = 5, .entry_type = .normal, .data = matching_completion_cmd },
    });
    defer std.testing.allocator.free(completion_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = metadata_group_id,
        .commit_index = 5,
        .entries_bytes = completion_entries,
    });

    const ranges = try store.listRanges(std.testing.allocator, metadata_group_id);
    defer store.freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 4201), ranges[0].range_id);
    try std.testing.expectEqualStrings("b", ranges[0].start_key);
    try std.testing.expectEqualStrings("z", ranges[0].end_key.?);
    try std.testing.expectEqual(@as(u64, 77), ranges[0].doc_identity_shard_id);
    try std.testing.expectEqual(@as(u64, 88), ranges[0].doc_identity_range_id);
    try std.testing.expectEqual(@as(u64, 9), ranges[0].split_attempt_epoch);
    try std.testing.expectEqualStrings("", ranges[0].restore_backup_id);
    try std.testing.expectEqualStrings("", ranges[0].restore_location);
    try std.testing.expectEqualStrings("", ranges[0].restore_snapshot_path);
    try std.testing.expectEqualStrings("", ranges[0].restore_connection);
    try std.testing.expectEqual(@as(u64, 0), ranges[0].restore_artifact_size_bytes);
    try std.testing.expectEqualStrings("", ranges[0].restore_artifact_sha256);
    try std.testing.expect(metadata_table_manager.rangeRestoreCompletionMatches(
        ranges[0],
        "new",
        "new-artifact-namespace",
        "s3://backups/new",
    ));
}

test "metadata raft apply store rejects reserved data group ids in transition records" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-invalid-data-group-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = group_ids.main_metadata_group_id,
            .table_id = 41,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const range_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(range_entries);
    try std.testing.expectError(error.ReservedGroupId, store.snapshotBuilder().applyBatch(.{
        .group_id = group_ids.main_metadata_group_id,
        .commit_index = 1,
        .entries_bytes = range_entries,
    }));

    const split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 501,
            .attempt_epoch = 1,
            .source_group_id = 21,
            .destination_group_id = group_ids.main_metadata_group_id,
            .phase = .prepare,
            .table_contract = test_transition_table_contract,
        },
    });
    defer std.testing.allocator.free(split_cmd);
    const split_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = split_cmd },
    });
    defer std.testing.allocator.free(split_entries);
    try std.testing.expectError(error.ReservedGroupId, store.snapshotBuilder().applyBatch(.{
        .group_id = group_ids.main_metadata_group_id,
        .commit_index = 2,
        .entries_bytes = split_entries,
    }));

    const merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_merge_transition = .{
            .transition_id = 601,
            .donor_group_id = group_ids.main_metadata_group_id,
            .receiver_group_id = 30,
            .phase = .prepare,
            .table_contract = test_transition_table_contract,
        },
    });
    defer std.testing.allocator.free(merge_cmd);
    const merge_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = merge_cmd },
    });
    defer std.testing.allocator.free(merge_entries);
    try std.testing.expectError(error.ReservedGroupId, store.snapshotBuilder().applyBatch(.{
        .group_id = group_ids.main_metadata_group_id,
        .commit_index = 3,
        .entries_bytes = merge_entries,
    }));
}

test "metadata raft apply store notifies projection listeners for committed table and range changes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-topology-listener-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const Capture = struct {
        table_signals: usize = 0,
        range_signals: usize = 0,
        last_table_id: u64 = 0,
        last_range_group_id: u64 = 0,

        fn onSignal(ptr: *anyopaque, signal: ProjectionSignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            switch (signal.kind) {
                .table => {
                    self.table_signals += 1;
                    self.last_table_id = signal.table_id;
                },
                .range => {
                    self.range_signals += 1;
                    self.last_table_id = signal.table_id;
                    self.last_range_group_id = signal.group_id;
                },
                else => {},
            }
        }
    };

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 77,
            .name = "docs",
            .schema_json = "{}",
            .indexes_json = "{}",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 1001,
            .table_id = 77,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    var capture = Capture{};
    try store.addProjectionListener(.{
        .ptr = &capture,
        .vtable = &.{
            .on_projection_signal = Capture.onSignal,
        },
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    try std.testing.expectEqual(@as(usize, 1), capture.table_signals);
    try std.testing.expectEqual(@as(usize, 1), capture.range_signals);
    try std.testing.expectEqual(@as(u64, 77), capture.last_table_id);
    try std.testing.expectEqual(@as(u64, 1001), capture.last_range_group_id);
}

test "metadata raft apply store notifies projection listeners for shuffle join lease changes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-shuffle-lease-listener-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const Capture = struct {
        shuffle_join_lease_signals: usize = 0,

        fn onSignal(ptr: *anyopaque, signal: ProjectionSignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (signal.kind == .shuffle_join_lease) self.shuffle_join_lease_signals += 1;
        }
    };

    const lease_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_shuffle_join_lease = .{
            .job_id = 77,
            .owner_group_id = 202,
            .expires_at_ms = 9_999,
        },
    });
    defer std.testing.allocator.free(lease_cmd);
    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_shuffle_join_lease = .{
            .job_id = 77,
        },
    });
    defer std.testing.allocator.free(remove_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = lease_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = remove_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    var capture = Capture{};
    try store.addProjectionListener(.{
        .ptr = &capture,
        .vtable = &.{
            .on_projection_signal = Capture.onSignal,
        },
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    try std.testing.expectEqual(@as(usize, 2), capture.shuffle_join_lease_signals);
}

test "metadata raft apply store preserves projected tables and ranges across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-projection-reopen-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 61,
            .name = "docs",
            .description = "docs table",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 6101,
            .table_id = 61,
            .start_key = "doc:a",
            .end_key = "doc:z",
        },
    });
    defer std.testing.allocator.free(range_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 61,
            .commit_index = 2,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        const tables = try store.listTables(std.testing.allocator, 61);
        defer store.freeTables(std.testing.allocator, tables);
        const ranges = try store.listRanges(std.testing.allocator, 61);
        defer store.freeRanges(std.testing.allocator, ranges);

        try std.testing.expectEqual(@as(usize, 1), tables.len);
        try std.testing.expectEqualStrings("docs", tables[0].name);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqual(@as(u64, 6101), ranges[0].group_id);
    }
}

test "metadata raft apply store notifies committed key listeners for matched metadata prefixes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-key-listener-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const Capture = struct {
        matched: usize = 0,
        saw_table: bool = false,
        saw_range: bool = false,

        fn matches(ptr: *anyopaque, signal: CommittedKeySignal) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self;
            var table_prefix_buf: [96]u8 = undefined;
            const table_prefix = tablePrefixForGroup(&table_prefix_buf, signal.metadata_group_id) catch return false;
            if (std.mem.startsWith(u8, signal.key, table_prefix)) return true;

            var range_prefix_buf: [96]u8 = undefined;
            const range_prefix = rangePrefixForGroup(&range_prefix_buf, signal.metadata_group_id) catch return false;
            return std.mem.startsWith(u8, signal.key, range_prefix);
        }

        fn onKey(ptr: *anyopaque, signal: CommittedKeySignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.matched += 1;

            var table_prefix_buf: [96]u8 = undefined;
            const table_prefix = tablePrefixForGroup(&table_prefix_buf, signal.metadata_group_id) catch return;
            if (std.mem.startsWith(u8, signal.key, table_prefix)) {
                self.saw_table = true;
                return;
            }

            var range_prefix_buf: [96]u8 = undefined;
            const range_prefix = rangePrefixForGroup(&range_prefix_buf, signal.metadata_group_id) catch return;
            if (std.mem.startsWith(u8, signal.key, range_prefix)) self.saw_range = true;
        }
    };

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 88,
            .name = "docs",
            .schema_json = "{}",
            .indexes_json = "{}",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 2001,
            .table_id = 88,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    var capture = Capture{};
    try store.addCommittedKeyListener(.{
        .ptr = &capture,
        .vtable = &.{
            .matches_key = Capture.matches,
            .on_committed_key = Capture.onKey,
        },
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    try std.testing.expectEqual(@as(usize, 2), capture.matched);
    try std.testing.expect(capture.saw_table);
    try std.testing.expect(capture.saw_range);
}

test "metadata.table record decoder accepts legacy table metadata encoding" {
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(std.testing.allocator);

    try appendInt(std.testing.allocator, &encoded, u64, 41);
    try appendInt(std.testing.allocator, &encoded, u16, 5);
    try appendInt(std.testing.allocator, &encoded, u32, 2);
    try appendInt(std.testing.allocator, &encoded, u32, 4);
    try encoded.appendSlice(std.testing.allocator, "docs");
    try appendInt(std.testing.allocator, &encoded, u32, 10);
    try encoded.appendSlice(std.testing.allocator, "docs table");
    try appendInt(std.testing.allocator, &encoded, u32, 15);
    try encoded.appendSlice(std.testing.allocator, "{\"kind\":\"demo\"}");
    try appendInt(std.testing.allocator, &encoded, u32, 14);
    try encoded.appendSlice(std.testing.allocator, "{\"default\":{}}");
    try appendInt(std.testing.allocator, &encoded, u32, 8);
    try encoded.appendSlice(std.testing.allocator, "[\"seed\"]");
    try appendInt(std.testing.allocator, &encoded, u32, 4);
    try encoded.appendSlice(std.testing.allocator, "data");

    const decoded = try decodeTableRecord(std.testing.allocator, encoded.items);
    defer metadata_table_manager.freeTable(std.testing.allocator, decoded);

    try std.testing.expectEqual(@as(u64, 41), decoded.table_id);
    try std.testing.expectEqualStrings("docs", decoded.name);
    try std.testing.expectEqualStrings("docs table", decoded.description);
    try std.testing.expectEqualStrings("{\"kind\":\"demo\"}", decoded.schema_json);
    try std.testing.expectEqualStrings("", decoded.read_schema_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", decoded.indexes_json);
    try std.testing.expectEqualStrings("[\"seed\"]", decoded.replication_sources_json);
    try std.testing.expectEqualStrings("data", decoded.placement_role);
}

test "metadata.table record decoder round-trips read schema metadata" {
    const encoded = try encodeTableRecord(std.testing.allocator, .{
        .table_id = 41,
        .name = "docs",
        .description = "docs table",
        .schema_json = "{\"version\":1}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"default\":{}}",
        .replication_sources_json = "[\"seed\"]",
        .placement_role = "data",
        .desired_replica_count = 5,
        .min_ranges = 2,
    });
    defer std.testing.allocator.free(encoded);

    const decoded = try decodeTableRecord(std.testing.allocator, encoded);
    defer metadata_table_manager.freeTable(std.testing.allocator, decoded);

    try std.testing.expectEqualStrings("{\"version\":1}", decoded.schema_json);
    try std.testing.expectEqualStrings("{\"version\":0}", decoded.read_schema_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", decoded.indexes_json);
}

test "metadata schema progress transition command round-trips" {
    const encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_progress = .{
            .table_id = 41,
            .node_id = 7,
            .schema_version = 3,
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = (try decodeTransitionCommand(std.testing.allocator, encoded)) orelse return error.InvalidMetadataTransitionEncoding;
    defer decoded.deinit(std.testing.allocator);

    switch (decoded) {
        .upsert_schema_progress => |record| {
            try std.testing.expectEqual(@as(u64, 41), record.table_id);
            try std.testing.expectEqual(@as(u64, 7), record.node_id);
            try std.testing.expectEqual(@as(u32, 3), record.schema_version);
        },
        else => return error.InvalidMetadataTransitionEncoding,
    }
}

test "metadata reallocation request transition command round-trips" {
    const protected_members = [_]u64{ 1, 2, 3 };
    const fingerprint = metadata.reallocation_request.membershipFingerprint(&protected_members);
    const encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_reallocation_request = .{
            .request_id = 17,
            .requested_at_ms = 42_000,
            .barrier_protocol_version = metadata.reallocation_request.barrier_protocol_version,
            .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
            .protected_metadata_member_count = protected_members.len,
            .protected_metadata_membership_fingerprint = fingerprint,
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = (try decodeTransitionCommand(std.testing.allocator, encoded)) orelse return error.InvalidMetadataTransitionEncoding;
    defer decoded.deinit(std.testing.allocator);

    switch (decoded) {
        .upsert_reallocation_request => |record| {
            try std.testing.expectEqual(@as(u128, 17), record.request_id);
            try std.testing.expectEqual(@as(u64, 42_000), record.requested_at_ms);
            try std.testing.expectEqual(metadata.reallocation_request.barrier_protocol_version, record.barrier_protocol_version);
            try std.testing.expectEqualStrings("0123456789abcdef0123456789abcdef", &record.metadata_incarnation.?);
            try std.testing.expectEqual(@as(u32, 3), record.protected_metadata_member_count);
            try std.testing.expectEqualSlices(u8, &fingerprint, &record.protected_metadata_membership_fingerprint);
        },
        else => return error.InvalidMetadataTransitionEncoding,
    }

    const remove_encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_reallocation_request = .{
            .expected_request_id = 17,
        },
    });
    defer std.testing.allocator.free(remove_encoded);

    var remove_decoded = (try decodeTransitionCommand(std.testing.allocator, remove_encoded)) orelse
        return error.InvalidMetadataTransitionEncoding;
    defer remove_decoded.deinit(std.testing.allocator);
    switch (remove_decoded) {
        .remove_reallocation_request => |record| {
            try std.testing.expectEqual(@as(u128, 17), record.expected_request_id);
        },
        else => return error.InvalidMetadataTransitionEncoding,
    }
}

test "metadata reallocation request decoder accepts the legacy unfenced record" {
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(std.testing.allocator);
    try appendInt(std.testing.allocator, &encoded, u128, 17);
    try appendInt(std.testing.allocator, &encoded, u64, 42_000);
    var pos: usize = 0;
    const record = try readReallocationRequestRecord(encoded.items, &pos);
    try std.testing.expectEqual(encoded.items.len, pos);
    try std.testing.expectEqual(@as(u16, 0), record.barrier_protocol_version);
    try std.testing.expect(record.metadata_incarnation == null);
}

test "metadata extension lifecycle transition command round-trips" {
    const command: TransitionCommand = .{
        .apply_extension_lifecycle = .{
            .upsert_tables = &.{.{
                .table_id = 7,
                .name = "memories",
                .indexes_json = "{\"memory_text\":{\"type\":\"full_text\"}}",
            }},
            .upsert_installed_extensions = &.{.{
                .name = "memoryaf",
                .package_name = "memoryaf",
                .package_version = "1.0.0",
                .package_digest = "sha256:abc",
                .scope = .{ .kind = .table, .table_name = "memories" },
                .status = .ready,
            }},
            .remove_installed_extensions = &.{"old_memoryaf"},
            .upsert_extension_members = &.{.{
                .extension_name = "memoryaf",
                .scope = .{ .kind = .table, .table_name = "memories" },
                .object_kind = .index,
                .object_name = "memory_text",
                .table_name = "memories",
                .owner_metadata_json = "{\"type\":\"full_text\"}",
            }},
            .remove_extension_members = &.{.{
                .extension_name = "memoryaf",
                .object_kind = .mcp_tool,
                .object_name = "old_recall",
            }},
            .upsert_extension_dependencies = &.{.{
                .extension_name = "memoryaf",
                .required_extension_name = "core",
                .package_name = "antfly_core",
            }},
            .remove_extension_dependencies = &.{.{
                .extension_name = "memoryaf",
                .required_extension_name = "old_core",
                .package_name = "old_core",
            }},
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .apply_extension_lifecycle);
    const delta = decoded.?.apply_extension_lifecycle;
    try std.testing.expectEqual(@as(usize, 1), delta.upsert_tables.len);
    try std.testing.expectEqualStrings("memories", delta.upsert_tables[0].name);
    try std.testing.expectEqual(@as(usize, 1), delta.upsert_installed_extensions.len);
    try std.testing.expectEqualStrings("memoryaf", delta.upsert_installed_extensions[0].name);
    try std.testing.expectEqual(@as(usize, 1), delta.remove_installed_extensions.len);
    try std.testing.expectEqualStrings("old_memoryaf", delta.remove_installed_extensions[0]);
    try std.testing.expectEqual(@as(usize, 1), delta.upsert_extension_members.len);
    try std.testing.expectEqual(.index, delta.upsert_extension_members[0].object_kind);
    try std.testing.expectEqual(@as(usize, 1), delta.remove_extension_members.len);
    try std.testing.expectEqualStrings("old_recall", delta.remove_extension_members[0].object_name);
    try std.testing.expectEqual(@as(usize, 1), delta.upsert_extension_dependencies.len);
    try std.testing.expectEqualStrings("antfly_core", delta.upsert_extension_dependencies[0].package_name);
    try std.testing.expectEqual(@as(usize, 1), delta.remove_extension_dependencies.len);
}

test "metadata raft apply store projects schema progress records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-schema-progress-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const progress_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_progress = .{
            .table_id = 41,
            .node_id = 7,
            .schema_version = 3,
        },
    });
    defer std.testing.allocator.free(progress_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = progress_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 4,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const progress = try store.listSchemaProgress(std.testing.allocator, 41);
        defer store.freeSchemaProgress(std.testing.allocator, progress);
        try std.testing.expectEqual(@as(usize, 1), progress.len);
        try std.testing.expectEqual(@as(u64, 41), progress[0].table_id);
        try std.testing.expectEqual(@as(u64, 7), progress[0].node_id);
        try std.testing.expectEqual(@as(u32, 3), progress[0].schema_version);
    }

    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_schema_progress = .{
            .table_id = 41,
            .node_id = 7,
        },
    });
    defer std.testing.allocator.free(remove_cmd);
    const encoded_remove = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 5, .entry_type = .normal, .data = remove_cmd },
    });
    defer std.testing.allocator.free(encoded_remove);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 5,
            .entries_bytes = encoded_remove,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const progress = try store.listSchemaProgress(std.testing.allocator, 41);
        defer store.freeSchemaProgress(std.testing.allocator, progress);
        try std.testing.expectEqual(@as(usize, 0), progress.len);
    }
}

test "metadata restore progress transition command round-trips" {
    const command: TransitionCommand = .{
        .upsert_restore_progress = .{
            .table_id = 41,
            .node_id = 7,
            .group_id = 4101,
            .backup_id = "snap1",
            .artifact_backup_id = "snap1-artifacts",
            .location = "s3://archive/snap1",
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .upsert_restore_progress);
    try std.testing.expectEqual(@as(u64, 41), decoded.?.upsert_restore_progress.table_id);
    try std.testing.expectEqual(@as(u64, 7), decoded.?.upsert_restore_progress.node_id);
    try std.testing.expectEqual(@as(u64, 4101), decoded.?.upsert_restore_progress.group_id);
    try std.testing.expectEqualStrings("snap1", decoded.?.upsert_restore_progress.backup_id);
    try std.testing.expectEqualStrings(
        "snap1-artifacts",
        decoded.?.upsert_restore_progress.artifact_backup_id,
    );
    try std.testing.expectEqualStrings("s3://archive/snap1", decoded.?.upsert_restore_progress.location);
    try std.testing.expectEqualStrings(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        decoded.?.upsert_restore_progress.artifact_sha256,
    );
}

test "metadata replication source status transition command round-trips" {
    const command: TransitionCommand = .{
        .upsert_replication_source_status = .{
            .table_id = 41,
            .source_ordinal = 0,
            .source_kind = "postgres",
            .external_table = "users",
            .cutover_mode = "exported_snapshot",
            .phase = "snapshot",
            .checkpoint = "lsn:0/16B6A50",
            .prepared_checkpoint = "lsn:0/16B6A50",
            .last_error = "",
            .failure_class = "terminal",
            .lag_records = 12,
            .lag_millis = 34,
            .consecutive_failures = 4,
            .last_source_commit_at_ms = 120,
            .last_success_at_ms = 123,
            .last_change_applied_at_ms = 124,
            .cutover_intent_id = 0x1122,
            .cutover_authority_id = 0x3344,
            .cutover_provider_identity = [_]u8{0x44} ** std.crypto.hash.sha2.Sha256.digest_length,
            .updated_at_ms = 555,
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .upsert_replication_source_status);
    try std.testing.expectEqual(@as(u64, 41), decoded.?.upsert_replication_source_status.table_id);
    try std.testing.expectEqual(@as(u32, 0), decoded.?.upsert_replication_source_status.source_ordinal);
    try std.testing.expectEqualStrings("postgres", decoded.?.upsert_replication_source_status.source_kind);
    try std.testing.expectEqualStrings("users", decoded.?.upsert_replication_source_status.external_table);
    try std.testing.expectEqualStrings("exported_snapshot", decoded.?.upsert_replication_source_status.cutover_mode);
    try std.testing.expectEqualStrings("snapshot", decoded.?.upsert_replication_source_status.phase);
    try std.testing.expectEqualStrings("lsn:0/16B6A50", decoded.?.upsert_replication_source_status.prepared_checkpoint);
    try std.testing.expectEqualStrings("terminal", decoded.?.upsert_replication_source_status.failure_class);
    try std.testing.expectEqual(@as(u64, 34), decoded.?.upsert_replication_source_status.lag_millis);
    try std.testing.expectEqual(@as(u64, 4), decoded.?.upsert_replication_source_status.consecutive_failures);
    try std.testing.expectEqual(@as(u64, 120), decoded.?.upsert_replication_source_status.last_source_commit_at_ms);
    try std.testing.expectEqual(@as(u64, 123), decoded.?.upsert_replication_source_status.last_success_at_ms);
    try std.testing.expectEqual(@as(u64, 124), decoded.?.upsert_replication_source_status.last_change_applied_at_ms);
    try std.testing.expectEqual(@as(u64, 0x1122), decoded.?.upsert_replication_source_status.cutover_intent_id);
    try std.testing.expectEqual(@as(u64, 0x3344), decoded.?.upsert_replication_source_status.cutover_authority_id);
    try std.testing.expectEqualSlices(
        u8,
        &([_]u8{0x44} ** std.crypto.hash.sha2.Sha256.digest_length),
        &decoded.?.upsert_replication_source_status.cutover_provider_identity,
    );
}

test "metadata shuffle join lease transition command round-trips" {
    const command: TransitionCommand = .{
        .upsert_shuffle_join_lease = .{
            .job_id = 77,
            .owner_group_id = 202,
            .expires_at_ms = 9_999,
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .upsert_shuffle_join_lease);
    try std.testing.expectEqual(@as(u64, 77), decoded.?.upsert_shuffle_join_lease.job_id);
    try std.testing.expectEqual(@as(u64, 202), decoded.?.upsert_shuffle_join_lease.owner_group_id);
    try std.testing.expectEqual(@as(u64, 9_999), decoded.?.upsert_shuffle_join_lease.expires_at_ms);
}

test "metadata raft apply store projects shuffle join lease records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-shuffle-lease-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const lease_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_shuffle_join_lease = .{
            .job_id = 77,
            .owner_group_id = 202,
            .expires_at_ms = 9_999,
        },
    });
    defer std.testing.allocator.free(lease_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = lease_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 4,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const lease = (try store.getShuffleJoinLease(41, 77)).?;
        try std.testing.expectEqual(@as(u64, 77), lease.job_id);
        try std.testing.expectEqual(@as(u64, 202), lease.owner_group_id);
        try std.testing.expectEqual(@as(u64, 9_999), lease.expires_at_ms);

        const leases = try store.listShuffleJoinLeases(std.testing.allocator, 41);
        defer store.freeShuffleJoinLeases(std.testing.allocator, leases);
        try std.testing.expectEqual(@as(usize, 1), leases.len);
    }
}

test "metadata raft apply store projects restore progress records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-restore-progress-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const progress_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_restore_progress = .{
            .table_id = 41,
            .node_id = 7,
            .group_id = 4101,
            .backup_id = "snap1",
            .artifact_backup_id = "snap1-artifacts",
        },
    });
    defer std.testing.allocator.free(progress_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = progress_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 3,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const progress = try store.listRestoreProgress(std.testing.allocator, 41);
        defer store.freeRestoreProgress(std.testing.allocator, progress);
        try std.testing.expectEqual(@as(usize, 1), progress.len);
        try std.testing.expectEqual(@as(u64, 41), progress[0].table_id);
        try std.testing.expectEqual(@as(u64, 7), progress[0].node_id);
        try std.testing.expectEqual(@as(u64, 4101), progress[0].group_id);
        try std.testing.expectEqualStrings("snap1", progress[0].backup_id);
        try std.testing.expectEqualStrings("snap1-artifacts", progress[0].artifact_backup_id);
    }
}

test "metadata raft apply store projects replication source status records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-replication-source-status-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const replication_sources_json =
        "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]";
    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "docs",
            .replication_sources_json = replication_sources_json,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const claim_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .claim_replication_source_cutover = .{
            .expected_replication_sources_json = replication_sources_json,
            .expected_authority_id = 0,
            .record = .{
                .table_id = 41,
                .source_ordinal = 0,
                .source_kind = "postgres",
                .external_table = "users",
                .cutover_mode = "exported_snapshot_pending",
                .slot_name = "docs_af_0000000000001122_0000000000003344",
                .publication_name = "docs_pub_af_0000000000001122_0000000000003344",
                .phase = "cutover_preparing",
                .cutover_intent_id = 0x1122,
                .cutover_authority_id = 0x3344,
                .cutover_provider_identity = [_]u8{0x44} ** std.crypto.hash.sha2.Sha256.digest_length,
                .cutover_config_fingerprint = [_]u8{0x33} ** std.crypto.hash.sha2.Sha256.digest_length,
            },
        },
    });
    defer std.testing.allocator.free(claim_cmd);
    const status_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_replication_source_status = .{
            .table_id = 41,
            .source_ordinal = 0,
            .source_kind = "postgres",
            .external_table = "users",
            .cutover_mode = "exported_snapshot",
            .slot_name = "docs_af_0000000000001122_0000000000003344",
            .publication_name = "docs_pub_af_0000000000001122_0000000000003344",
            .phase = "streaming",
            .checkpoint = "lsn:0/16B6B10",
            .prepared_checkpoint = "lsn:0/16B6A50",
            .last_error = "",
            .failure_class = "retryable",
            .lag_records = 3,
            .lag_millis = 21,
            .consecutive_failures = 2,
            .last_source_commit_at_ms = 333,
            .last_success_at_ms = 444,
            .last_change_applied_at_ms = 555,
            .cutover_intent_id = 0x1122,
            .cutover_authority_id = 0x3344,
            .cutover_provider_identity = [_]u8{0x44} ** std.crypto.hash.sha2.Sha256.digest_length,
            .cutover_config_fingerprint = [_]u8{0x33} ** std.crypto.hash.sha2.Sha256.digest_length,
            .updated_at_ms = 777,
        },
    });
    defer std.testing.allocator.free(status_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = claim_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = status_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 3,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const statuses = try store.listReplicationSourceStatuses(std.testing.allocator, 41);
        defer store.freeReplicationSourceStatuses(std.testing.allocator, statuses);
        try std.testing.expectEqual(@as(usize, 1), statuses.len);
        try std.testing.expectEqual(@as(u64, 41), statuses[0].table_id);
        try std.testing.expectEqual(@as(u32, 0), statuses[0].source_ordinal);
        try std.testing.expectEqualStrings("postgres", statuses[0].source_kind);
        try std.testing.expectEqualStrings("users", statuses[0].external_table);
        try std.testing.expectEqualStrings("streaming", statuses[0].phase);
        try std.testing.expectEqualStrings("lsn:0/16B6A50", statuses[0].prepared_checkpoint);
        try std.testing.expectEqualStrings("retryable", statuses[0].failure_class);
        try std.testing.expectEqual(@as(u64, 3), statuses[0].lag_records);
        try std.testing.expectEqual(@as(u64, 21), statuses[0].lag_millis);
        try std.testing.expectEqual(@as(u64, 2), statuses[0].consecutive_failures);
        try std.testing.expectEqual(@as(u64, 333), statuses[0].last_source_commit_at_ms);
        try std.testing.expectEqual(@as(u64, 444), statuses[0].last_success_at_ms);
        try std.testing.expectEqual(@as(u64, 555), statuses[0].last_change_applied_at_ms);
        try std.testing.expectEqual(@as(u64, 0x1122), statuses[0].cutover_intent_id);
        try std.testing.expectEqual(@as(u64, 0x3344), statuses[0].cutover_authority_id);
        try std.testing.expectEqualSlices(
            u8,
            &([_]u8{0x44} ** std.crypto.hash.sha2.Sha256.digest_length),
            &statuses[0].cutover_provider_identity,
        );
        try std.testing.expectEqualSlices(
            u8,
            &([_]u8{0x33} ** std.crypto.hash.sha2.Sha256.digest_length),
            &statuses[0].cutover_config_fingerprint,
        );

        const point_status = (try store.getReplicationSourceStatus(
            std.testing.allocator,
            41,
            41,
            0,
        )).?;
        defer metadata_table_manager.freeReplicationSourceStatus(std.testing.allocator, point_status);
        try std.testing.expectEqual(@as(u64, 0x1122), point_status.cutover_intent_id);
        try std.testing.expectEqual(@as(u64, 0x3344), point_status.cutover_authority_id);
        try std.testing.expectEqualSlices(
            u8,
            &([_]u8{0x44} ** std.crypto.hash.sha2.Sha256.digest_length),
            &point_status.cutover_provider_identity,
        );
    }
}

test "metadata raft apply store fences exact cutover authority and retirement" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/metadata-cutover-authority-cas",
        .{tmp.sub_path},
    );
    defer alloc.free(root);

    const group_id: u64 = 41;
    const replication_sources_json =
        "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]";
    const fingerprint =
        [_]u8{0x33} ** std.crypto.hash.sha2.Sha256.digest_length;
    const provider =
        [_]u8{0x44} ** std.crypto.hash.sha2.Sha256.digest_length;
    const authority_a = metadata.ReplicationSourceStatusRecord{
        .table_id = 41,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .cutover_mode = "exported_snapshot_pending",
        .slot_name = "docs_af_0000000000000011_00000000000000a1",
        .publication_name = "docs_pub_af_0000000000000011_00000000000000a1",
        .phase = "cutover_preparing",
        .cutover_intent_id = 0x11,
        .cutover_authority_id = 0xa1,
        .cutover_config_fingerprint = fingerprint,
        .cutover_provider_identity = provider,
    };
    var authority_b = authority_a;
    authority_b.slot_name =
        "docs_af_0000000000000011_00000000000000b2";
    authority_b.publication_name =
        "docs_pub_af_0000000000000011_00000000000000b2";
    authority_b.cutover_authority_id = 0xb2;
    authority_b.retired_cutover_authority_id =
        authority_a.cutover_authority_id;
    authority_b.retired_slot_name = authority_a.slot_name;
    authority_b.retired_publication_name = authority_a.publication_name;
    var authority_c = authority_b;
    authority_c.slot_name =
        "docs_af_0000000000000011_00000000000000c3";
    authority_c.publication_name =
        "docs_pub_af_0000000000000011_00000000000000c3";
    authority_c.cutover_authority_id = 0xc3;
    authority_c.retired_cutover_authority_id =
        authority_b.cutover_authority_id;
    authority_c.retired_slot_name = authority_b.slot_name;
    authority_c.retired_publication_name = authority_b.publication_name;

    const Apply = struct {
        fn command(
            inner_alloc: std.mem.Allocator,
            store: *RaftApplyStore,
            metadata_group_id: u64,
            index: u64,
            value: TransitionCommand,
        ) !void {
            const encoded = try encodeTransitionCommand(inner_alloc, value);
            defer inner_alloc.free(encoded);
            const entries = try raft_state_machine.encodeCommittedEntries(
                inner_alloc,
                &.{.{
                    .term = 1,
                    .index = index,
                    .entry_type = .normal,
                    .data = encoded,
                }},
            );
            defer inner_alloc.free(entries);
            try store.snapshotBuilder().applyBatch(.{
                .group_id = metadata_group_id,
                .commit_index = index,
                .entries_bytes = entries,
            });
        }
    };

    var store = try RaftApplyStore.init(alloc, .{ .root_dir = root });
    defer store.deinit();
    try Apply.command(alloc, &store, group_id, 1, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "docs",
            .replication_sources_json = replication_sources_json,
        },
    });
    try Apply.command(alloc, &store, group_id, 2, .{
        .claim_replication_source_cutover = .{
            .expected_replication_sources_json = replication_sources_json,
            .expected_authority_id = 0,
            .record = authority_a,
        },
    });
    try Apply.command(alloc, &store, group_id, 3, .{
        .claim_replication_source_cutover = .{
            .expected_replication_sources_json = replication_sources_json,
            .expected_authority_id = authority_a.cutover_authority_id,
            .record = authority_b,
        },
    });

    // A third authority cannot hide B's pending retirement obligation.
    try Apply.command(alloc, &store, group_id, 4, .{
        .claim_replication_source_cutover = .{
            .expected_replication_sources_json = replication_sources_json,
            .expected_authority_id = authority_b.cutover_authority_id,
            .record = authority_c,
        },
    });
    var stale_a = authority_a;
    stale_a.phase = "streaming";
    try Apply.command(alloc, &store, group_id, 5, .{
        .upsert_replication_source_status = stale_a,
    });
    var malformed_retirement_b = authority_b;
    malformed_retirement_b.retired_slot_name = "unowned_slot";
    try Apply.command(alloc, &store, group_id, 6, .{
        .upsert_replication_source_status = malformed_retirement_b,
    });
    {
        const current = (try store.getReplicationSourceStatus(
            alloc,
            group_id,
            41,
            0,
        )).?;
        defer metadata_table_manager.freeReplicationSourceStatus(
            alloc,
            current,
        );
        try std.testing.expectEqual(
            authority_b.cutover_authority_id,
            current.cutover_authority_id,
        );
        try std.testing.expectEqual(
            authority_a.cutover_authority_id,
            current.retired_cutover_authority_id,
        );
        try std.testing.expectEqualStrings(
            authority_a.slot_name,
            current.retired_slot_name,
        );
    }

    var failed_b = authority_b;
    failed_b.phase = "failed";
    failed_b.failure_class = "retryable";
    failed_b.last_error = "ExactCutoverCleanupPending";
    try Apply.command(alloc, &store, group_id, 7, .{
        .upsert_replication_source_status = failed_b,
    });
    try Apply.command(alloc, &store, group_id, 8, .{
        .claim_replication_source_cutover = .{
            .expected_replication_sources_json = replication_sources_json,
            .expected_authority_id = authority_b.cutover_authority_id,
            .record = authority_b,
        },
    });
    {
        const current = (try store.getReplicationSourceStatus(
            alloc,
            group_id,
            41,
            0,
        )).?;
        defer metadata_table_manager.freeReplicationSourceStatus(
            alloc,
            current,
        );
        try std.testing.expectEqualStrings("cutover_preparing", current.phase);
        try std.testing.expectEqual(
            authority_a.cutover_authority_id,
            current.retired_cutover_authority_id,
        );
    }

    var advanced_b = authority_b;
    advanced_b.phase = "snapshot";
    advanced_b.prepared_checkpoint = "lsn:1";
    try Apply.command(alloc, &store, group_id, 9, .{
        .upsert_replication_source_status = advanced_b,
    });
    try Apply.command(alloc, &store, group_id, 10, .{
        .claim_replication_source_cutover = .{
            .expected_replication_sources_json = replication_sources_json,
            .expected_authority_id = authority_b.cutover_authority_id,
            .record = authority_b,
        },
    });
    {
        const current = (try store.getReplicationSourceStatus(
            alloc,
            group_id,
            41,
            0,
        )).?;
        defer metadata_table_manager.freeReplicationSourceStatus(
            alloc,
            current,
        );
        try std.testing.expectEqualStrings("snapshot", current.phase);
        try std.testing.expectEqualStrings(
            "lsn:1",
            current.prepared_checkpoint,
        );
    }

    var retired_b = authority_b;
    retired_b.retired_cutover_authority_id = 0;
    retired_b.retired_slot_name = "";
    retired_b.retired_publication_name = "";
    retired_b.phase = "snapshot";
    retired_b.prepared_checkpoint = "lsn:1";
    // Ordinary progress publication cannot erase a provider cleanup
    // obligation, even from the current authority.
    try Apply.command(alloc, &store, group_id, 11, .{
        .upsert_replication_source_status = retired_b,
    });
    {
        const current = (try store.getReplicationSourceStatus(
            alloc,
            group_id,
            41,
            0,
        )).?;
        defer metadata_table_manager.freeReplicationSourceStatus(
            alloc,
            current,
        );
        try std.testing.expectEqual(
            authority_a.cutover_authority_id,
            current.retired_cutover_authority_id,
        );
    }
    try Apply.command(alloc, &store, group_id, 12, .{
        .complete_replication_source_retirement = advanced_b,
    });
    try Apply.command(alloc, &store, group_id, 13, .{
        .claim_replication_source_cutover = .{
            .expected_replication_sources_json = "[]",
            .expected_authority_id = authority_b.cutover_authority_id,
            .record = authority_c,
        },
    });
    {
        const current = (try store.getReplicationSourceStatus(
            alloc,
            group_id,
            41,
            0,
        )).?;
        defer metadata_table_manager.freeReplicationSourceStatus(
            alloc,
            current,
        );
        try std.testing.expectEqual(
            authority_b.cutover_authority_id,
            current.cutover_authority_id,
        );
        try std.testing.expectEqual(
            @as(u64, 0),
            current.retired_cutover_authority_id,
        );
        try std.testing.expectEqualStrings("snapshot", current.phase);
    }

    try Apply.command(alloc, &store, group_id, 14, .{
        .claim_replication_source_cutover = .{
            .expected_replication_sources_json = replication_sources_json,
            .expected_authority_id = authority_b.cutover_authority_id,
            .record = authority_c,
        },
    });
    const current = (try store.getReplicationSourceStatus(
        alloc,
        group_id,
        41,
        0,
    )).?;
    defer metadata_table_manager.freeReplicationSourceStatus(alloc, current);
    try std.testing.expectEqual(
        authority_c.cutover_authority_id,
        current.cutover_authority_id,
    );
    try std.testing.expectEqual(
        authority_b.cutover_authority_id,
        current.retired_cutover_authority_id,
    );
}

test "metadata raft apply store transition codec preserves exact raft voter identity" {
    const fingerprint = metadata_table_manager.voterSetFingerprint(&.{ 101, 102, 104 }, null);
    var group_statuses = [_]metadata.GroupStatusReport{.{
        .group_id = 5101,
        .raft_applied_index = 91,
        .raft_term = 12,
        .raft_membership_index = 87,
        .disk_bytes = 4096,
        .disk_bytes_known = true,
        .observed_reallocation_request_id = 0x123456789abcdef00123456789abcdef,
        .local_leader = true,
        .local_voter = true,
        .voter_count = 3,
        .voter_set_known = true,
        .voter_set_fingerprint = fingerprint,
    }};
    const encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{
            .store_id = 101,
            .node_id = 101,
            .role = "data",
            .group_statuses = group_statuses[0..],
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = (try decodeTransitionCommand(std.testing.allocator, encoded)) orelse
        return error.InvalidMetadataTransitionEncoding;
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expect(decoded == .upsert_store);
    const statuses = decoded.upsert_store.group_statuses;
    try std.testing.expectEqual(@as(usize, 1), statuses.len);
    try std.testing.expect(statuses[0].voter_set_known);
    try std.testing.expect(statuses[0].disk_bytes_known);
    try std.testing.expectEqual(@as(u64, 4096), statuses[0].disk_bytes);
    try std.testing.expectEqual(@as(u64, 91), statuses[0].raft_applied_index);
    try std.testing.expectEqual(@as(u64, 12), statuses[0].raft_term);
    try std.testing.expectEqual(@as(u64, 87), statuses[0].raft_membership_index);
    try std.testing.expectEqual(
        @as(u128, 0x123456789abcdef00123456789abcdef),
        statuses[0].observed_reallocation_request_id,
    );
    try std.testing.expectEqual(@as(u16, 3), statuses[0].voter_count);
    try std.testing.expectEqualSlices(u8, &fingerprint, &statuses[0].voter_set_fingerprint);
}

test "metadata raft apply store transition codec preserves duplicate group observations by position" {
    var group_statuses = [_]metadata.GroupStatusReport{
        .{
            .group_id = 5101,
            .observed_reallocation_request_id = 0x1111,
        },
        .{
            .group_id = 5101,
            .observed_reallocation_request_id = 0x2222,
        },
    };
    const encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{
            .store_id = 101,
            .node_id = 101,
            .role = "data",
            .group_statuses = group_statuses[0..],
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = (try decodeTransitionCommand(std.testing.allocator, encoded)) orelse
        return error.InvalidMetadataTransitionEncoding;
    defer decoded.deinit(std.testing.allocator);
    const statuses = decoded.upsert_store.group_statuses;
    try std.testing.expectEqual(@as(usize, 2), statuses.len);
    try std.testing.expectEqual(@as(u64, 5101), statuses[0].group_id);
    try std.testing.expectEqual(@as(u64, 5101), statuses[1].group_id);
    try std.testing.expectEqual(@as(u128, 0x1111), statuses[0].observed_reallocation_request_id);
    try std.testing.expectEqual(@as(u128, 0x2222), statuses[1].observed_reallocation_request_id);
}

test "metadata raft apply store projects placement intents from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-placement-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .register_node = .{
        .node_id = 7,
        .role = "data",
        .lifecycle = metadata_table_manager.node_lifecycle_active,
    } });
    defer std.testing.allocator.free(node_cmd);
    const store_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .register_store = .{
        .store_id = 44,
        .node_id = 7,
        .role = "data",
        .health_class = "healthy",
        .live = true,
    } });
    defer std.testing.allocator.free(store_cmd);
    const intent_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_replica_intent = .{
            .expected_metadata_version = null,
            .expected_version_fence = 0,
            .expected_target_drain_requested = false,
            .replacement = .{
                .record = .{
                    .group_id = 5101,
                    .replica_id = 2,
                    .local_node_id = 7,
                    .bootstrap_mode = .fetch_snapshot,
                    .snapshot_bootstrap = .{
                        .from_node_id = 3,
                        .term = 8,
                        .snapshot_id = "snap-5101",
                        .uri = "http://127.0.0.1:7777/raft/v1/snapshot/fetch/snap-5101",
                    },
                },
                .store_id = 44,
                .peer_node_ids = &.{ 7, 8, 9 },
                .serving_state = .retiring,
            },
        },
    });
    defer std.testing.allocator.free(intent_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = store_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = intent_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 51,
            .commit_index = 3,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const intents = try store.listPlacementIntents(std.testing.allocator, 51);
        defer store.freePlacementIntents(std.testing.allocator, intents);
        try std.testing.expectEqual(@as(usize, 1), intents.len);
        try std.testing.expectEqual(@as(u64, 5101), intents[0].record.group_id);
        try std.testing.expectEqual(@as(u64, 7), intents[0].record.local_node_id);
        try std.testing.expectEqual(@as(u64, 44), intents[0].store_id);
        try std.testing.expectEqual(@as(usize, 3), intents[0].peer_node_ids.len);
        try std.testing.expectEqual(raft_reconciler.PlacementServingState.retiring, intents[0].serving_state);
        try std.testing.expectEqual(raft_catalog.ReplicaBootstrapMode.fetch_snapshot, intents[0].record.bootstrap_mode);
        try std.testing.expect(intents[0].record.snapshot_bootstrap != null);
        try std.testing.expectEqual(@as(u64, 3), intents[0].record.snapshot_bootstrap.?.from_node_id);
        try std.testing.expectEqual(@as(u64, 8), intents[0].record.snapshot_bootstrap.?.term);
        try std.testing.expectEqualStrings("snap-5101", intents[0].record.snapshot_bootstrap.?.snapshot_id);
    }
}

test "metadata raft apply store projects backup restore bootstrap source in placement intents" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-placement-backup-source-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .register_node = .{
        .node_id = 7,
        .role = "data",
        .lifecycle = metadata_table_manager.node_lifecycle_active,
    } });
    defer std.testing.allocator.free(node_cmd);
    const store_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .register_store = .{
        .store_id = 45,
        .node_id = 7,
        .role = "data",
        .health_class = "healthy",
        .live = true,
    } });
    defer std.testing.allocator.free(store_cmd);
    const intent_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_replica_intent = .{
            .expected_metadata_version = null,
            .expected_version_fence = 0,
            .expected_target_drain_requested = false,
            .replacement = .{
                .record = .{
                    .group_id = 5201,
                    .replica_id = 2,
                    .local_node_id = 7,
                    .bootstrap_mode = .fetch_snapshot,
                    .backup_restore_bootstrap = .{
                        .backup_id = "snap-5201",
                        .artifact_backup_id = "snap-5201",
                        .location = "file:///tmp/backups",
                        .snapshot_path = "snap-5201/groups/5201",
                        .connection = "backup-store",
                        .artifact_size_bytes = 4096,
                        .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    },
                },
                .store_id = 45,
                .peer_node_ids = &.{ 7, 8, 9 },
            },
        },
    });
    defer std.testing.allocator.free(intent_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = store_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = intent_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 52,
            .commit_index = 3,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const intents = try store.listPlacementIntents(std.testing.allocator, 52);
        defer store.freePlacementIntents(std.testing.allocator, intents);
        try std.testing.expectEqual(@as(usize, 1), intents.len);
        try std.testing.expect(intents[0].record.backup_restore_bootstrap != null);
        try std.testing.expectEqualStrings("snap-5201", intents[0].record.backup_restore_bootstrap.?.backup_id);
        try std.testing.expectEqualStrings("file:///tmp/backups", intents[0].record.backup_restore_bootstrap.?.location);
        try std.testing.expectEqualStrings("snap-5201/groups/5201", intents[0].record.backup_restore_bootstrap.?.snapshot_path);
        try std.testing.expectEqualStrings("backup-store", intents[0].record.backup_restore_bootstrap.?.connection);
        try std.testing.expectEqual(@as(u64, 4096), intents[0].record.backup_restore_bootstrap.?.artifact_size_bytes);
        try std.testing.expectEqualStrings(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            intents[0].record.backup_restore_bootstrap.?.artifact_sha256,
        );
    }
}

test "metadata state machine projects transitions through metadata apply store" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sm-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const SinkRecorder = struct {
        last_index: u64 = 0,

        fn sink(self: *@This()) raft_state_machine.AppliedIndexSink {
            return .{
                .ptr = self,
                .vtable = &.{
                    .set_applied_index = setAppliedIndex,
                },
            };
        }

        fn setAppliedIndex(ptr: *anyopaque, _: u64, index: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.last_index = index;
        }
    };

    var sink = SinkRecorder{};
    var sm = raft_state_machine.MetadataStateMachine{
        .alloc = std.testing.allocator,
        .applied_sink = sink.sink(),
        .snapshot_builder = store.snapshotBuilder(),
    };

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = testTransitionTableRecord(),
    });
    defer std.testing.allocator.free(table_cmd);
    const source_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = testTransitionRangeRecord(
            41,
            test_transition_table_contract.source_identity,
            "a",
            "z",
        ),
    });
    defer std.testing.allocator.free(source_cmd);
    const setup_entries = try raft_state_machine.encodeCommittedEntries(
        std.testing.allocator,
        &.{
            .{ .term = 3, .index = 10, .entry_type = .normal, .data = table_cmd },
            .{ .term = 3, .index = 11, .entry_type = .normal, .data = source_cmd },
        },
    );
    defer std.testing.allocator.free(setup_entries);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 11,
        .entries_bytes = setup_entries,
    });

    const cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .admit_split_transition = .{
            .expected_source_epoch = 0,
            .record = .{
                .transition_id = 701,
                .attempt_epoch = 1,
                .source_group_id = 41,
                .destination_group_id = 42,
                .phase = .prepare,
                .split_key = "m",
                .source_range_end = "z",
                .table_contract = test_transition_table_contract,
            },
        },
    });
    defer std.testing.allocator.free(cmd);

    try sm.stateMachine().applyReady(41, null, &.{
        .{ .term = 3, .index = 12, .entry_type = .normal, .data = cmd },
    }, &.{});

    const splits = try store.listSplitTransitions(std.testing.allocator, 41);
    defer store.freeSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 1), splits.len);
    try std.testing.expectEqual(@as(u64, 701), splits[0].transition_id);
    try std.testing.expectEqual(@as(u64, 12), sink.last_index);
}

test "metadata raft apply store runtime status codec preserves document identity telemetry" {
    const alloc = std.testing.allocator;

    var runtime_statuses = [_]metadata.RuntimeGroupStatusReport{.{
        .table_id = 1,
        .table_name = "docs",
        .group_id = 10,
        .store_id = 20,
        .node_id = 30,
        .source = "background_refresh",
        .freshness = "fresh",
        .disk_bytes = 4096,
        .disk_bytes_known = true,
        .enrichment = .{
            .projection_checkpoint_status = "rebuilding",
            .projection_checkpoint_config_hash = std.math.maxInt(u64) - 7,
            .processed_requests = 17,
            .consecutive_retry_count = 4,
            .next_retry_at_ms = 1_753_392_345_999,
            .embed_batches_completed = 3,
            .last_embed_batch_completed_ms = 1_753_392_345_678,
            .worker_started = true,
            .stalled = true,
        },
        .doc_identity = .{
            .namespace_table_id = 1,
            .namespace_shard_id = 10,
            .namespace_range_id = 1001,
            .next_ordinal = 44,
            .allocated_ordinals = 43,
            .ordinal_capacity_remaining = 123,
            .rebuild_required = true,
            .state_rows = 42,
            .live_ordinals = 40,
            .tombstone_ordinals = 2,
            .min_created_generation = 11,
            .max_created_generation = 17,
            .min_deleted_generation = 15,
            .max_deleted_generation = 18,
            .scanned_primary_docs = 41,
            .primary_docs_missing_ordinals = 1,
            .primary_docs_with_tombstone_ordinals = 1,
            .complete = true,
        },
        .doc_set_planning = .{
            .resolved_set_count = 9,
            .ordinal_list_count = 8,
            .ordinal_list_docs = 7,
            .missing_ordinal_coverage_count = 6,
            .stale_identity_generation_rejection_count = 5,
        },
        .indexes = @constCast((&[_]metadata.RuntimeIndexStatusReport{.{
            .name = "visual_idx",
            .kind = "dense_vector",
            .load_error = "InvalidRebuildState",
            .coverage_produced_count = 31,
            .coverage_skipped_count = 7,
            .coverage_terminal_failed_count = 2,
            .coverage_generation = 0x5678,
            .coverage_config_hash = 0x1234,
            .coverage_identity_ready = true,
            .coverage_summary_ready = true,
        }})[0..]),
    }};

    const encoded = try encodeStoreRecord(alloc, .{
        .store_id = 20,
        .node_id = 30,
        .runtime_statuses = runtime_statuses[0..],
    });
    defer alloc.free(encoded);

    const decoded = try decodeStoreRecord(alloc, encoded);
    defer metadata_table_manager.freeStore(alloc, decoded);

    try std.testing.expectEqual(@as(usize, 1), decoded.runtime_statuses.len);
    const status = decoded.runtime_statuses[0];
    try std.testing.expectEqual(@as(u64, 4096), status.disk_bytes);
    try std.testing.expect(status.disk_bytes_known);
    try std.testing.expect(status.enrichment.worker_started);
    try std.testing.expect(status.enrichment.stalled);
    try std.testing.expectEqual(@as(u64, 17), status.enrichment.processed_requests);
    try std.testing.expectEqual(@as(u32, 4), status.enrichment.consecutive_retry_count);
    try std.testing.expectEqual(@as(u64, 1_753_392_345_999), status.enrichment.next_retry_at_ms);
    try std.testing.expectEqual(@as(u64, 3), status.enrichment.embed_batches_completed);
    try std.testing.expectEqual(@as(u64, 1_753_392_345_678), status.enrichment.last_embed_batch_completed_ms);
    try std.testing.expectEqual(std.math.maxInt(u64) - 7, status.enrichment.projection_checkpoint_config_hash);
    try std.testing.expectEqualStrings("rebuilding", status.enrichment.projection_checkpoint_status);
    try std.testing.expectEqual(@as(u64, 1), status.doc_identity.namespace_table_id);
    try std.testing.expectEqual(@as(u64, 10), status.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(@as(u64, 1001), status.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u32, 44), status.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 43), status.doc_identity.allocated_ordinals);
    try std.testing.expect(status.doc_identity.rebuild_required);
    try std.testing.expect(status.doc_identity.complete);
    try std.testing.expectEqual(@as(u64, 9), status.doc_set_planning.resolved_set_count);
    try std.testing.expectEqual(@as(u64, 8), status.doc_set_planning.ordinal_list_count);
    try std.testing.expectEqual(@as(u64, 7), status.doc_set_planning.ordinal_list_docs);
    try std.testing.expectEqual(@as(u64, 6), status.doc_set_planning.missing_ordinal_coverage_count);
    try std.testing.expectEqual(@as(u64, 5), status.doc_set_planning.stale_identity_generation_rejection_count);
    try std.testing.expectEqual(@as(usize, 1), status.indexes.len);
    try std.testing.expectEqualStrings("InvalidRebuildState", status.indexes[0].load_error.?);
    try std.testing.expectEqual(@as(u64, 31), status.indexes[0].coverage_produced_count);
    try std.testing.expectEqual(@as(u64, 7), status.indexes[0].coverage_skipped_count);
    try std.testing.expectEqual(@as(u64, 2), status.indexes[0].coverage_terminal_failed_count);
    try std.testing.expectEqual(@as(u64, 0x5678), status.indexes[0].coverage_generation);
    try std.testing.expectEqual(@as(u64, 0x1234), status.indexes[0].coverage_config_hash);
    try std.testing.expect(status.indexes[0].coverage_identity_ready);
    try std.testing.expect(status.indexes[0].coverage_summary_ready);
}

test "metadata runtime index status decoder accepts version ten records" {
    const alloc = std.testing.allocator;
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(alloc);

    try appendRuntimeIndexStatusRecord(alloc, &encoded, .{
        .name = "legacy_idx",
        .kind = "dense_vector",
        .doc_count = 17,
        .replay_applied_sequence = 9,
        .replay_target_sequence = 11,
        .replay_catch_up_required = true,
    });

    // Version 10 ended immediately after replay_catch_up_required. The final
    // absent optional-string tag is the only field appended by version 11.
    try std.testing.expectEqual(@as(u8, 0), encoded.items[encoded.items.len - 1]);
    encoded.items.len -= 1;

    var pos: usize = 0;
    const decoded = try readRuntimeIndexStatusRecord(alloc, encoded.items, &pos, 10);
    defer metadata_table_manager.freeRuntimeIndexStatusReport(alloc, decoded);

    try std.testing.expectEqual(encoded.items.len, pos);
    try std.testing.expectEqualStrings("legacy_idx", decoded.name);
    try std.testing.expectEqual(@as(u64, 17), decoded.doc_count);
    try std.testing.expectEqual(@as(u64, 9), decoded.replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 11), decoded.replay_target_sequence);
    try std.testing.expect(decoded.replay_catch_up_required);
    try std.testing.expectEqual(@as(?[]const u8, null), decoded.load_error);
}

test "metadata raft apply store group status decoder accepts version one records" {
    const alloc = std.testing.allocator;
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(alloc);

    const fingerprint = metadata_table_manager.voterSetFingerprint(&.{ 3, 5, 8 }, null);
    try appendInt(alloc, &encoded, u16, 1);
    try appendInt(alloc, &encoded, u64, 41);
    try appendInt(alloc, &encoded, u64, 17);
    try appendInt(alloc, &encoded, u64, 4096);
    try encoded.append(alloc, 0);
    try appendInt(alloc, &encoded, u32, 0);
    try appendInt(alloc, &encoded, u64, 1234);
    try encoded.append(alloc, 1);
    try appendInt(alloc, &encoded, u64, 1200);
    try encoded.append(alloc, 0);
    try encoded.append(alloc, 1);
    try encoded.append(alloc, 1);
    try encoded.append(alloc, 0);
    try encoded.append(alloc, 0);
    try encoded.append(alloc, 1);
    try appendInt(alloc, &encoded, u16, 3);
    try encoded.append(alloc, 0);
    try encoded.append(alloc, 1);
    try encoded.appendSlice(alloc, &fingerprint);

    var pos: usize = 0;
    const decoded = try readGroupStatusRecord(alloc, encoded.items, &pos);
    try std.testing.expectEqual(encoded.items.len, pos);
    try std.testing.expectEqual(@as(u64, 41), decoded.group_id);
    try std.testing.expectEqual(@as(u64, 0), decoded.relocation_generation);
    try std.testing.expectEqual(@as(u64, 0), decoded.raft_applied_index);
    try std.testing.expectEqual(@as(u64, 17), decoded.doc_count);
    try std.testing.expectEqual(@as(u64, 4096), decoded.disk_bytes);
    try std.testing.expect(decoded.local_leader);
    try std.testing.expect(decoded.local_voter);
    try std.testing.expect(decoded.replay_required);
    try std.testing.expect(decoded.replay_caught_up);
    try std.testing.expect(decoded.voter_set_known);
    try std.testing.expectEqualSlices(u8, &fingerprint, &decoded.voter_set_fingerprint);
}

test "metadata apply store replay is idempotent when applied watermark lags WAL state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-apply-replay-idempotent", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try raft_storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 202, 1);
    defer layout.deinit(std.testing.allocator);

    const cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = testTransitionTableRecord(),
    });
    defer std.testing.allocator.free(cmd);

    {
        var wal_state = try wal_replica_state_mod.WalReplicaState.init(std.testing.allocator, layout, .{});
        defer wal_state.deinit();

        const entries = try std.testing.allocator.dupe(raft_engine.core.Entry, &[_]raft_engine.core.Entry{
            .{ .term = 5, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);

        try wal_state.groupStorage().persistReady(202, .{
            .hard_state = .{ .current_term = 5, .voted_for = 1, .commit_index = 1 },
            .entries = entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        var sm = raft_state_machine.MetadataStateMachine{
            .alloc = std.testing.allocator,
            .applied_sink = raft_state_machine.noopAppliedIndexSink(),
            .snapshot_builder = store.snapshotBuilder(),
        };
        try sm.stateMachine().applyReady(202, null, &.{
            .{ .term = 5, .index = 1, .entry_type = .normal, .data = cmd },
        }, &.{});
    }

    {
        var wal_state = try wal_replica_state_mod.WalReplicaState.init(std.testing.allocator, layout, .{});
        defer wal_state.deinit();
        try std.testing.expectEqual(@as(u64, 0), wal_state.appliedIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 202,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = wal_state.appliedIndex(),
        }, wal_state.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);

        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        var sm = raft_state_machine.MetadataStateMachine{
            .alloc = std.testing.allocator,
            .applied_sink = raft_state_machine.noopAppliedIndexSink(),
            .snapshot_builder = store.snapshotBuilder(),
        };
        try sm.stateMachine().applyReady(202, rd.snapshot, rd.committed_entries, &.{});

        const batch = (try store.latestBatch(202)) orelse return error.MissingMetadataBatch;
        try std.testing.expectEqual(@as(u64, 1), batch.commit_index);

        const tables = try store.listTables(std.testing.allocator, 202);
        defer store.freeTables(std.testing.allocator, tables);
        try std.testing.expectEqual(@as(usize, 1), tables.len);
        try std.testing.expectEqual(
            test_transition_table_contract.table_id,
            tables[0].table_id,
        );
        try std.testing.expectEqualStrings(
            test_transition_table_contract.table_name,
            tables[0].name,
        );
    }
}
const restore_job_logical_prefix = "\x00\x00__api_restore_jobs__:";
const max_restore_job_logical_key_bytes: usize = 128;
const max_restore_job_value_bytes: usize = 64 * 1024;
