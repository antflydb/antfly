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

//! DB integration with hot standby coordination. Engine-only tests stay with DB.
const engine = @import("antfly_local_sources").storage_db_db;
const default_test_wait_attempts = engine.test_support.default_test_wait_attempts;
const ConcurrentWriteProbe = engine.test_support.ConcurrentWriteProbe;
const DB = engine.DB;
const DocIdentityNamespace = engine.DocIdentityNamespace;
const DurableReplicationOutboxKind = durable_outbox.Kind;
const EnrichmentAppendContext = engine.test_support.EnrichmentAppendContext;
const GateDenseEmbedder = engine.test_support.GateDenseEmbedder;
const GraphPrimaryPublicationTest = engine.test_support.GraphPrimaryPublicationTest;
const GraphTtlSha256 = @import("antfly_hash").Sha256;
const Io = std.Io;
const MutationBarrier = engine.MutationBarrier;
const OpenOptions = engine.OpenOptions;
const ReplicationAsyncEffectMirror = engine.ReplicationAsyncEffectMirror;
const ResolutionHandoffPublishHook = engine.test_support.ResolutionHandoffPublishHook;
const TestDirectory = @import("antfly_local_sources").common_test_directory.TestDirectory;
const TtlCleanupContext = engine.test_support.TtlCleanupContext;
const appendDerivedBatchRecord = engine.test_support.appendDerivedBatchRecord;
const appendDerivedBatchRecordContext = engine.test_support.appendDerivedBatchRecordContext;
const appendResolutionRecord = engine.test_support.appendResolutionRecord;
const appendResolutionRecordWithHook = engine.test_support.appendResolutionRecordWithHook;
const applyDerivedBatchToIndexAsync = engine.test_support.applyDerivedBatchToIndexAsync;
const builtin = @import("builtin");
const change_journal_mod = @import("antfly_local_sources").storage_db_derived_change_journal;
const cleanupTempDir = engine.test_support.cleanupTempDir;
const doc_identity = @import("antfly_local_sources").storage_db_doc_identity;
const docstore_mod = @import("antfly_local_sources").storage_docstore;
const durableReplicationOutboxKeyAlloc = durable_outbox.durableReplicationOutboxKeyAlloc;
const durableReplicationOutboxKindFromKey = durable_outbox.durableReplicationOutboxKindFromKey;
const encodeDurableReplicationOutboxAlloc = durable_outbox.encodeDurableReplicationOutboxAlloc;
const enrichment_artifact_codec = @import("antfly_local_sources").storage_db_enrichment_artifact_codec;
const executeDeleteBatchContext = engine.test_support.executeDeleteBatchContext;
const expireDirectGraphTtlCandidateContext = engine.test_support.expireDirectGraphTtlCandidateContext;
const expireGraphTtlCandidateContext = engine.test_support.expireGraphTtlCandidateContext;
const graph_edge_contender = @import("antfly_local_sources").storage_db_graph_edge_contender;
const graph_edge_ttl_expiration = @import("antfly_local_sources").storage_db_graph_edge_ttl_expiration;
const graph_edge_ttl_tombstone = @import("antfly_local_sources").storage_db_graph_edge_ttl_tombstone;
const graph_metric_rerank = @import("antfly_local_sources").graph_metric_rerank;
const graph_mod = @import("antfly_local_sources").graph_graph;
const internal_keys = @import("antfly_local_sources").storage_internal_keys;
const lockAtomic = engine.test_support.lockAtomic;
const monotonicTimeNs = engine.test_support.monotonicTimeNs;
const platform_clock = @import("antfly_platform").clock;
const portable_backup = @import("antfly_local_sources").storage_portable_backup;
const publishResolutionHandoffContext = engine.test_support.publishResolutionHandoffContext;
const relational_columns = @import("antfly_local_sources").storage_db_relational_columns;
const replay_stream_mod = @import("antfly_local_sources").storage_db_derived_replay_stream;
const replication_batch_outbox_key = durable_outbox.replication_batch_outbox_key;
const replication_effects_mod = @import("antfly_local_sources").storage_db_replication_effects;
const replication_ingress = @import("antfly_local_sources").storage_db_replication_ingress;
const replication_outbox_v2_prefix = durable_outbox.replication_outbox_v2_prefix;
const replication_record_mod = @import("antfly_local_sources").storage_db_replication_record;
const resolution_handoff = @import("antfly_local_sources").storage_db_resolution_handoff;
const resolution_runtime_mod = @import("antfly_local_sources").storage_db_resolution_runtime;
const row_policy_authority_mod = @import("antfly_local_sources").usermgr_row_policy_authority;
const row_policy_bundle_mod = @import("antfly_local_sources").storage_db_row_policy_bundle;
const schema_mod = @import("antfly_local_sources").storage_schema;
const sleepNs = engine.test_support.sleepNs;
const std = @import("std");
const table_catalog_mod = @import("antfly_local_sources").storage_db_table_catalog;
const transactions_mod = @import("antfly_local_sources").storage_transactions;
const types = @import("antfly_local_sources").storage_db_types;
const waitForAtomicFlag = engine.test_support.waitForAtomicFlag;
const durable_outbox = @import("antfly_local_sources").storage_db_durable_outbox;

const hot_standby_write_gate_adapter = @import("write_gate.zig");

const hot_standby_sync_wait = @import("sync_wait.zig");

const hot_standby_publisher_adapter = @import("db_commit.zig");

const hot_standby_commit_gate_mod = @import("commit_gate.zig");

const hot_standby_fencing_mod = @import("fencing.zig");

const hot_standby_primary_mod = @import("primary.zig");

const hot_standby_public_gate_state_mod = @import("public_gate_state.zig");

const hot_standby_session_mod = @import("session.zig");

const hot_standby_standby_mod = @import("standby.zig");

const hot_standby_write_gate_mod = @import("write_gate.zig");

const HotStandbyPrimaryProgressSyncWait = hot_standby_sync_wait.HotStandbyPrimaryProgressSyncWait;

const HotStandbySessionSyncWait = hot_standby_sync_wait.HotStandbySessionSyncWait;

test "storage.hot_standby resolution handoff fence rejects completion after durable HA replay" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.heap.c_allocator;
    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 271,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();

    var public_gate = hot_standby_public_gate_state_mod.State{};
    public_gate.configurePrimary(&primary, false);
    var transition_mutex: std.atomic.Mutex = .unlocked;
    var barrier: MutationBarrier = .{};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .replication_async_effect_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .mutation_barrier = &barrier,
            .transition_mutex = &transition_mutex,
        }),
        .replication_write_gate = .{ .shared = .{ .state = public_gate.storageWriteState() } },
        .start_index_workers = false,
    });
    defer db.close();

    const PauseBeforePublish = struct {
        io: std.Io,
        entered: std.atomic.Value(bool) = .init(false),
        reached: std.Io.Event = .unset,
        release: std.Io.Event = .unset,

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            self.reached.set(self.io);
            self.release.waitUncancelable(self.io);
        }
    };
    const WriteProbe = struct {
        ctx: *EnrichmentAppendContext,
        resolution_key: []const u8,
        pause: *PauseBeforePublish,
        result: std.atomic.Value(u8) = .init(0),

        fn run(self: *@This()) void {
            // Also wake the test if the writer fails before entering the hook,
            // so a real regression reports its assertion instead of hanging.
            defer self.pause.reached.set(self.pause.io);
            const writes = [_]resolution_runtime_mod.ArtifactWrite{.{
                .key = self.resolution_key,
                .value = "{\"entities\":[\"durable\"]}",
            }};
            _ = appendResolutionRecordWithHook(self.ctx, .{
                .batch = .{ .changed_artifact_keys = &.{self.resolution_key} },
                .artifact_writes = &writes,
                .publish_resolution_handoff = true,
            }, ResolutionHandoffPublishHook{
                .ptr = self.pause,
                .run_fn = PauseBeforePublish.run,
            }) catch |err| {
                self.result.store(if (err == error.HAFencedPrimary) 1 else 2, .release);
                return;
            };
            self.result.store(3, .release);
        }
    };

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:fenced", "resolution_v1");
    defer alloc.free(resolution_key);
    const marker_key = try resolution_handoff.keyAlloc(alloc, resolution_key);
    defer alloc.free(marker_key);
    var pause = PauseBeforePublish{ .io = std.testing.io };
    var probe = WriteProbe{
        .ctx = db.resolution_append_context.?,
        .resolution_key = resolution_key,
        .pause = &pause,
    };
    var thread = try std.testing.io.concurrent(WriteProbe.run, .{&probe});
    var thread_joined = false;
    errdefer {
        pause.release.set(pause.io);
        if (!thread_joined) thread.await(std.testing.io);
    }

    pause.reached.waitUncancelable(pause.io);
    try std.testing.expect(pause.entered.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), db.core.nextDerivedSequence());
    const artifact = try db.core.store.get(alloc, resolution_key);
    defer alloc.free(artifact);
    try std.testing.expectEqualStrings("{\"entities\":[\"durable\"]}", artifact);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, marker_key));
    // Remote durability no longer retains the mutation lease. Capture can
    // select the exact local-commit/HA-tail boundary before the unacknowledged
    // handoff marker is published.
    var capture_before_fence = barrier.tryAcquireExclusive() orelse return error.TestExpectedEqual;
    capture_before_fence.release();

    lockAtomic(&transition_mutex);
    public_gate.publishPrimaryFence(true);
    pause.release.set(pause.io);
    transition_mutex.unlock();
    thread.await(std.testing.io);
    thread_joined = true;

    try std.testing.expectEqual(@as(u8, 1), probe.result.load(.acquire));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, marker_key));
    var capture = barrier.tryAcquireExclusive() orelse return error.TestExpectedEqual;
    capture.release();
}

test "row-policy Raft apply persists fail-closed intent and finalizes after restart" {
    const alloc = std.testing.allocator;
    var test_tmp = try TestDirectory.init("row-policy-pending");
    defer test_tmp.cleanup();
    const path = std.mem.span(test_tmp.path().ptr);
    const namespace: DocIdentityNamespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 };
    const options: OpenOptions = .{ .identity_namespace = namespace, .start_index_workers = false, .start_optional_runtimes = false };
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    var bundle_bytes: []u8 = undefined;
    var request: @import("antfly_local_sources").system_catalog_policies.InstallRequest = undefined;
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, schema_json);
        const schema = db.core.schema.?;
        const schema_bytes = try schema_mod.serializeSchema(alloc, schema);
        defer alloc.free(schema_bytes);
        var schema_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(schema_bytes, &schema_digest, .{});
        const policy: @import("antfly_local_sources").system_catalog_policies.Record = .{
            .id = 1,
            .generation = 1,
            .table_id = namespace.table_id,
            .schema_version = schema.version,
            .schema_digest = schema_digest,
            .name = "visible",
            .commands = .{ .select = true },
            .roles = &.{"PUBLIC"},
            .using = .{ .instructions = &.{
                .{ .type = .{ .kind = .boolean }, .operation = .{ .literal = .{ .bool = true } } },
            }, .root = 0 },
        };
        bundle_bytes = try std.json.Stringify.valueAlloc(alloc, @import("antfly_local_sources").system_catalog_policies.InstallSnapshot{
            .table_id = namespace.table_id,
            .schema_version = schema.version,
            .schema_digest = schema_digest,
            .policy_generation = 1,
            .catalog_epoch = 2,
            .phase = .pending_install,
            .records = &.{policy},
            .settings = &.{},
        }, .{});
        const range = db.core.byteRange();
        request = .{
            .table_id = namespace.table_id,
            .expected_generation = 1,
            .expected_catalog_epoch = 2,
            .expected_phase = .pending_install,
            .owner_group_id = 17,
            .expected_descriptor_digest = try (@import("antfly_local_sources").system_catalog_policies.OwnerDescriptor{
                .table_id = namespace.table_id,
                .group_id = 17,
                .shard_id = namespace.shard_id,
                .range_id = namespace.range_id,
                .schema_version = schema.version,
                .schema_digest = schema_digest,
                .range_start = range.start,
                .range_end = range.end,
            }).digest(),
        };
        var old_reader = try db.local_execution.row_policy_gate.enterRaw();
        const delayed_scan = try db.openRelationalReadSession(alloc, "", "", .{ .relational_query = .{ .fields = &.{"id"} } });
        try std.testing.expect((try db.applyReplicatedRowPolicyPublication(bundle_bytes, request, .{ .term = 3, .index = 11 })) == null);
        try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.preparing, db.core.table_catalog.row_policy_phase);
        try std.testing.expectError(error.RowPolicyAuthenticationRequired, db.local_execution.row_policy_gate.enterRaw());
        try std.testing.expectError(error.RowPolicyCatalogChanged, delayed_scan.nextTypedPage(alloc, null, .{}));
        try std.testing.expectError(error.RowPolicyReadersActive, db.loadRowPolicyReceipt(1, .pending_install));
        delayed_scan.deinit();
        old_reader.release();
        // The applied marker is already durable; reopening must retain the
        // preparing barrier and complete only from the committed intent.
    }
    defer alloc.free(bundle_bytes);
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.preparing, db.local_execution.row_policy_gate.currentPhase());
        try std.testing.expectError(error.RowPolicyAuthenticationRequired, db.local_execution.row_policy_gate.enterRaw());
        const receipt = try db.loadRowPolicyReceipt(1, .pending_install);
        try std.testing.expectEqual(@as(u64, 3), receipt.applied_term);
        try std.testing.expectEqual(@as(u64, 11), receipt.applied_index);
        try std.testing.expectEqualDeep(receipt, (try db.applyReplicatedRowPolicyPublication(bundle_bytes, request, .{ .term = 3, .index = 11 })).?);
        var parsed = try std.json.parseFromSlice(@import("antfly_local_sources").system_catalog_policies.InstallSnapshot, alloc, bundle_bytes, .{});
        defer parsed.deinit();
        parsed.value.phase = .serving_install;
        const serving_bytes = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
        defer alloc.free(serving_bytes);
        request.expected_phase = .serving_install;
        try std.testing.expect((try db.applyReplicatedRowPolicyPublication(serving_bytes, request, .{ .term = 3, .index = 12 })) == null);
        try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.preparing, db.local_execution.row_policy_gate.currentPhase());
        const serving_receipt = try db.loadRowPolicyReceipt(1, .serving_install);
        try std.testing.expectEqual(@as(u64, 12), serving_receipt.applied_index);
        try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.active, db.local_execution.row_policy_gate.currentPhase());
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, row_policy_bundle_mod.pending_key));
    }
    {
        var reopened = try DB.open(alloc, path, options);
        defer reopened.close();
        try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.active, reopened.local_execution.row_policy_gate.currentPhase());
        try std.testing.expect(reopened.local_execution.row_policy_bundle != null);
        try std.testing.expectError(error.RowPolicyAuthenticationRequired, reopened.local_execution.row_policy_gate.enterRaw());
    }
    {
        // A serving policy must survive a primary reopen with the ordered
        // batch and metadata mirrors attached; raw callers remain denied.
        var log_tmp = try TestDirectory.init("row-policy-ha-log");
        defer log_tmp.cleanup();
        var slots_tmp = try TestDirectory.init("row-policy-ha-slots");
        defer slots_tmp.cleanup();
        var primary = try hot_standby_primary_mod.Primary.open(alloc, std.mem.span(log_tmp.path().ptr), std.mem.span(slots_tmp.path().ptr), .{
            .cluster_id = 1,
            .shard_id = namespace.shard_id,
            .table_id = namespace.table_id,
            .timeline_id = 1,
            .epoch = 1,
        }, .{});
        defer primary.close();
        var mirrored_options = options;
        mirrored_options.replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{});
        mirrored_options.replication_async_metadata_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{});
        var mirrored = try DB.open(alloc, path, mirrored_options);
        defer mirrored.close();
        try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.active, mirrored.local_execution.row_policy_gate.currentPhase());
        try std.testing.expectError(error.RowPolicyAuthenticationRequired, mirrored.local_execution.row_policy_gate.enterRaw());
    }
    var follower_tmp = try TestDirectory.init("row-policy-follower");
    defer follower_tmp.cleanup();
    var follower = try DB.open(alloc, std.mem.span(follower_tmp.path().ptr), options);
    defer follower.close();
    try follower.setSchemaJson(alloc, schema_json);
    var pending_request = request;
    pending_request.expected_phase = .pending_install;
    var follower_reader = try follower.local_execution.row_policy_gate.enterRaw();
    try std.testing.expect((try follower.applyReplicatedRowPolicyPublication(bundle_bytes, pending_request, .{ .term = 3, .index = 11 })) == null);
    try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.preparing, follower.local_execution.row_policy_gate.currentPhase());
    var follower_bundle = try std.json.parseFromSlice(@import("antfly_local_sources").system_catalog_policies.InstallSnapshot, alloc, bundle_bytes, .{});
    defer follower_bundle.deinit();
    follower_bundle.value.phase = .serving_install;
    const follower_serving_bytes = try std.json.Stringify.valueAlloc(alloc, follower_bundle.value, .{});
    defer alloc.free(follower_serving_bytes);
    pending_request.expected_phase = .serving_install;
    try std.testing.expect((try follower.applyReplicatedRowPolicyPublication(follower_serving_bytes, pending_request, .{ .term = 3, .index = 12 })) == null);
    // No receipt probe ran for pending_install on this follower. Catch-up
    // must advance the committed fail-closed intent rather than stall Raft.
    try std.testing.expectError(error.NotFound, follower.loadRowPolicyReceipt(1, .pending_install));
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, follower.get(alloc, "row:unseen"));
    try std.testing.expectError(error.RowPolicyReadersActive, follower.loadRowPolicyReceipt(1, .serving_install));
    follower_reader.release();
    const follower_receipt = try follower.loadRowPolicyReceipt(1, .serving_install);
    try std.testing.expectEqual(@as(u64, 12), follower_receipt.applied_index);
    try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.active, follower.local_execution.row_policy_gate.currentPhase());

    var next_bundle = try std.json.parseFromSlice(@import("antfly_local_sources").system_catalog_policies.InstallSnapshot, alloc, bundle_bytes, .{});
    defer next_bundle.deinit();
    next_bundle.value.policy_generation = 2;
    next_bundle.value.catalog_epoch = 3;
    next_bundle.value.phase = .pending_disable;
    const candidate_bytes = try std.json.Stringify.valueAlloc(alloc, next_bundle.value, .{});
    defer alloc.free(candidate_bytes);
    var candidate_request = request;
    candidate_request.expected_generation = 2;
    candidate_request.expected_catalog_epoch = 3;
    candidate_request.expected_phase = .pending_disable;
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        const candidate_receipt = (try db.applyReplicatedRowPolicyPublication(candidate_bytes, candidate_request, .{ .term = 3, .index = 13 })).?;
        try std.testing.expectEqual(@as(u64, 13), candidate_receipt.applied_index);
        try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.active, db.local_execution.row_policy_gate.currentPhase());
        try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_policy_generation);
        try std.testing.expect(db.local_execution.row_policy_bundle != null);
    }
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        // A staged candidate does not replace the serving policy on restart.
        try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.active, db.local_execution.row_policy_gate.currentPhase());
        try std.testing.expectEqual(@as(u64, 1), db.local_execution.row_policy_bundle.?.parsed.value.policy_generation);
        const old_principal: row_policy_authority_mod.Payload = .{
            .principal = "alice",
            .roles = &.{},
            .auth_revision = 1,
            .table_id = namespace.table_id,
            .table = "table:7",
            .database = "main",
            .policy_generation = 1,
            .catalog_epoch = 2,
            .access = .read,
            .expires = 130,
        };
        var old_reader = try db.local_execution.row_policy_gate.enterVerifiedPrincipal(&old_principal, 100);
        try old_reader.checkAt(100);
        next_bundle.value.phase = .serving_disable;
        const serving_bytes = try std.json.Stringify.valueAlloc(alloc, next_bundle.value, .{});
        defer alloc.free(serving_bytes);
        candidate_request.expected_phase = .serving_disable;
        try std.testing.expect((try db.applyReplicatedRowPolicyPublication(serving_bytes, candidate_request, .{ .term = 3, .index = 14 })) == null);
        try std.testing.expectError(error.RowPolicyCatalogChanged, old_reader.checkAt(100));
        try std.testing.expectError(error.RowPolicyReadersActive, db.loadRowPolicyReceipt(2, .serving_disable));
        old_reader.release();
        _ = try db.loadRowPolicyReceipt(2, .serving_disable);
        try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.preparing, db.local_execution.row_policy_gate.currentPhase());
        next_bundle.value.phase = .disabled;
        const disabled_bytes = try std.json.Stringify.valueAlloc(alloc, next_bundle.value, .{});
        defer alloc.free(disabled_bytes);
        candidate_request.expected_phase = .disabled;
        try std.testing.expect((try db.applyReplicatedRowPolicyPublication(disabled_bytes, candidate_request, .{ .term = 3, .index = 15 })) == null);
        _ = try db.loadRowPolicyReceipt(2, .disabled);
        try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.disabled, db.local_execution.row_policy_gate.currentPhase());
        var raw = try db.local_execution.row_policy_gate.enterRawRead();
        raw.release();
    }
}

test "storage.hot_standby graph retirement seal replays exact Raft receipt and rejects missing marker" {
    const alloc = std.testing.allocator;
    const replication_effects = @import("effects.zig");
    const seal = @import("antfly_local_sources").storage_db_graph_retirement_seal;
    var path_tmp = try TestDirectory.init("graph-retirement-ha-replay");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer cleanupTempDir(path);
    const namespace: doc_identity.Namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 };
    var db = try DB.open(alloc, std.mem.span(path), .{ .identity_namespace = namespace, .start_index_workers = false, .start_optional_runtimes = false });
    var db_open = true;
    defer if (db_open) db.close();
    try db.setSchemaJson(alloc,
        \\{"version":0,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try db.addIndex(.{ .name = "links", .kind = .graph, .config_json = "{}", .coverage_generation = 1 });
    const graph_digest = (try db.core.index_manager.graphRetirementConfigDigest(alloc)) orelse return error.TestUnexpectedResult;
    const catalog = try db.core.store.get(alloc, @import("antfly_local_sources").storage_db_relational_integrity_catalog.key);
    defer alloc.free(catalog);
    var catalog_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(catalog, &catalog_digest, .{});
    const scope: seal.Scope = .{
        .fence = .{ .role = .rewrite_source, .transition_id = 7, .attempt = 1, .admission_epoch = 1, .peer_group_id = 401, .owner_group_id = 301, .namespace = namespace, .catalog_digest = catalog_digest },
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .target_table_id = 10,
        .graph_config_digest = graph_digest,
    };
    const begin_req: types.BatchRequest = .{ .relational_topology = .{ .action = .begin, .fence = scope.fence, .graph_retirement = scope } };
    const begin_payload = try replication_effects.encodeBatchMutationRequestAlloc(alloc, begin_req);
    defer alloc.free(begin_payload);
    var record: replication_record_mod.RecordView = .{
        .kind = .batch_mutation,
        .payload_codec = .json,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .table_id = namespace.table_id,
        .shard_id = namespace.shard_id,
        .payload = begin_payload,
    };
    try replication_ingress.applyRecord(&db, record);
    try std.testing.expect(!db.core.index_manager.graphRetirementAdmissionOpen());
    try std.testing.expectError(error.IntegrityTopologyBusy, db.findKShortestPaths(alloc, "links", "a", "b", 2, &.{}, .out, .min_hops, 4, null, null));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.matchPattern(alloc, "links", &.{"a"}, &.{}, 1, &.{}));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.search(alloc, .{
        .graph_metric_queries = &.{.{ .name = "metric", .query = .{ .index_name = "links", .metric_name = "rank" } }},
    }));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.search(alloc, .{
        .graph_metric_rerank = .{ .index_name = "links", .metric_name = "rank" },
    }));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.batch(.{
        .graph_writes = &.{.{ .index_name = "links", .source = "a", .target = "b", .edge_type = "related", .weight = 1.0 }},
    }));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.runGraphMetricMaintenanceForIdle());
    // The begin intent alone must rehydrate the closed gate before optional
    // graph runtimes start. No seal receipt exists yet, so restart cannot
    // accidentally treat the owner as either unguarded or completed.
    db.close();
    db_open = false;
    db = try DB.open(alloc, std.mem.span(path), .{ .identity_namespace = namespace, .start_index_workers = false, .start_optional_runtimes = false });
    db_open = true;
    try std.testing.expect(!db.core.index_manager.graphRetirementAdmissionOpen());
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const status = try seal.status(&read);
        try std.testing.expect(status.intent.?.eql(scope));
        try std.testing.expect(status.receipt == null);
    }
    try std.testing.expectError(error.IntegrityTopologyBusy, db.search(alloc, .{
        .graph_metric_rerank = .{ .index_name = "links", .metric_name = "rank" },
    }));
    const seal_req: types.BatchRequest = .{ .relational_topology = .{ .action = .seal_graph_retirement, .fence = scope.fence, .graph_retirement = scope } };
    try std.testing.expectError(error.InvalidBatchRequest, engine.test_support.applyRelationalTopologyControlWithReplication(
        &db,
        .{ .action = .seal_graph_retirement, .fence = scope.fence },
        .{ .term = 5, .index = 8 },
        null,
        null,
        null,
        null,
    ));
    const missing_marker = try std.json.Stringify.valueAlloc(alloc, replication_effects.BatchMutationPayload{ .schema_version = 9, .request = seal_req }, .{});
    defer alloc.free(missing_marker);
    record.lsn = 2;
    record.previous_lsn = 1;
    record.payload = missing_marker;
    try std.testing.expectError(error.InvalidGraphRetirementSeal, replication_ingress.applyRecord(&db, record));
    const invalid_marker = try std.json.Stringify.valueAlloc(alloc, replication_effects.BatchMutationPayload{ .schema_version = 9, .request = seal_req, .graph_retirement_raft_entry = .{ .term = 0, .index = 8 } }, .{});
    defer alloc.free(invalid_marker);
    record.payload = invalid_marker;
    try std.testing.expectError(error.InvalidGraphRetirementSeal, replication_ingress.applyRecord(&db, record));
    const sealed_payload = try replication_effects.encodeGraphRetirementSealMutationRequestAlloc(alloc, seal_req, .{ .term = 5, .index = 8 });
    defer alloc.free(sealed_payload);
    record.payload = sealed_payload;
    db.core.index_manager.graph_metric_schedule_pins.store(1, .release);
    try std.testing.expectError(error.StorageBusy, replication_ingress.applyRecord(&db, record));
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try seal.status(&read)).receipt == null);
    }
    db.core.index_manager.graph_metric_schedule_pins.store(0, .release);
    try replication_ingress.applyRecord(&db, record);
    try replication_ingress.applyRecord(&db, record);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const status = try seal.status(&read);
        try std.testing.expect(status.intent.?.eql(scope));
        try std.testing.expectEqual(@as(u64, 5), status.receipt.?.applied_term);
        try std.testing.expectEqual(@as(u64, 8), status.receipt.?.applied_index);
        try std.testing.expectEqualDeep(try scope.sealDigest(), status.receipt.?.digest);
    }
    try std.testing.expect(!db.core.index_manager.graphRetirementAdmissionOpen());
    db.close();
    db_open = false;
    db = try DB.open(alloc, std.mem.span(path), .{ .identity_namespace = namespace, .start_index_workers = false, .start_optional_runtimes = false });
    db_open = true;
    try std.testing.expect(!db.core.index_manager.graphRetirementAdmissionOpen());
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const status = try seal.status(&read);
        try std.testing.expect(status.intent.?.eql(scope));
        try std.testing.expectEqualDeep(try scope.sealDigest(), status.receipt.?.digest);
    }
}

test "storage.hot_standby db mirrors appended derived replay records into HA stream" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 200,
        .shard_id = 3,
        .table_id = 9,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();

    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var failures = @import("antfly_platform").atomic.Value(u64).init(0);
    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_v1", "mentions", "doc:b");
    defer alloc.free(artifact_key);
    {
        var db = try DB.open(alloc, std.mem.span(db_path), .{
            .identity_namespace = .{ .shard_id = 3, .table_id = 9 },
            .replication_async_effect_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
                .last_lsn = &last_lsn,
                .failure_count = &failures,
            }),
        });
        defer db.close();

        const changed_artifact_keys = [_][]const u8{artifact_key};
        const sequence = try appendDerivedBatchRecord(&db, .{
            .changed_artifact_keys = changed_artifact_keys[0..],
        });
        try std.testing.expectEqual(@as(u64, 1), sequence);
    }

    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));

    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(@as(@TypeOf(entry.record.kind), .derived_effect), entry.record.kind);
    try std.testing.expectEqual(@as(u64, 200), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 3), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 9), entry.record.table_id);

    var decoded = try replication_effects_mod.decodeDerivedChangeRecord(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u64, 1), decoded.record.sequence);
    try std.testing.expectEqualStrings(artifact_key, decoded.record.changed_artifact_keys[0]);
    try std.testing.expectEqual(change_journal_mod.TargetHint.graph, decoded.record.target_hints[0]);
}

test "storage.hot_standby db waits for remote apply before completing derived enrichment" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 201,
        .shard_id = 3,
        .table_id = 9,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    const SyncWait = struct {
        calls: u64 = 0,

        fn wait(ctx: *anyopaque, primary_arg_ctx: *anyopaque, target_lsn: u64, policy: hot_standby_primary_mod.SyncPolicy) !void {
            const primary_arg: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(primary_arg_ctx));
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            try std.testing.expectEqual(hot_standby_primary_mod.DurabilityMode.remote_apply, policy.mode);
            try primary_arg.standbyStatusUpdate("standby-a", primary_arg.identity.timeline_id, target_lsn, target_lsn);
        }
    };

    var wait_state = SyncWait{};
    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .identity_namespace = .{ .shard_id = 3, .table_id = 9 },
        .replication_async_effect_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .last_lsn = &last_lsn,
            .sync_policy = .{
                .mode = .remote_apply,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = SyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_v1", "mentions", "doc:b");
    defer alloc.free(artifact_key);
    const changed_artifact_keys = [_][]const u8{artifact_key};
    const sequence = try appendDerivedBatchRecord(&db, .{
        .changed_artifact_keys = changed_artifact_keys[0..],
    });

    try std.testing.expectEqual(@as(u64, 1), sequence);
    try std.testing.expectEqual(@as(u64, 1), wait_state.calls);
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@backingInt(hot_standby_commit_gate_mod.Action.acknowledge), gate_action.load(.acquire));
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
}

test "storage.hot_standby db mirrors committed batch mutations into HA stream for standby apply" {
    const alloc = std.testing.allocator;

    var primary_db_path_tmp = try TestDirectory.init("db");
    defer primary_db_path_tmp.cleanup();
    const primary_db_path = primary_db_path_tmp.path().ptr;
    defer cleanupTempDir(primary_db_path);
    var standby_db_path_tmp = try TestDirectory.init("db");
    defer standby_db_path_tmp.cleanup();
    const standby_db_path = standby_db_path_tmp.path().ptr;
    defer cleanupTempDir(standby_db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);
    var standby_log_path_tmp = try TestDirectory.init("db");
    defer standby_log_path_tmp.cleanup();
    const standby_log_path = standby_log_path_tmp.path().ptr;
    defer cleanupTempDir(standby_log_path);
    var standby_progress_path_tmp = try TestDirectory.init("db");
    defer standby_progress_path_tmp.cleanup();
    const standby_progress_path = standby_progress_path_tmp.path().ptr;
    defer cleanupTempDir(standby_progress_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 250,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();

    var standby = try hot_standby_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, .{
        .cluster_id = 250,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer standby.close();

    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var failures = @import("antfly_platform").atomic.Value(u64).init(0);
    {
        var db = try DB.open(alloc, std.mem.span(primary_db_path), .{
            .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
            .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
                .last_lsn = &last_lsn,
                .failure_count = &failures,
            }),
            .start_index_workers = false,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .deletes = &.{"doc:old"},
            .timestamp_ns = 123,
            .sync_level = .write,
        });
    }

    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(@as(@TypeOf(entry.record.kind), .batch_mutation), entry.record.kind);
    try std.testing.expectEqual(@as(u64, 250), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 4), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 10), entry.record.table_id);

    var decoded = try replication_effects_mod.decodeBatchMutationRequest(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1), decoded.value.request.writes.len);
    try std.testing.expectEqualStrings("doc:a", decoded.value.request.writes[0].key);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", decoded.value.request.writes[0].value);
    try std.testing.expectEqual(@as(usize, 1), decoded.value.request.deletes.len);
    try std.testing.expectEqualStrings("doc:old", decoded.value.request.deletes[0]);
    try std.testing.expectEqual(@as(u64, 123), decoded.value.request.timestamp_ns);

    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .replication_write_gate = .{ .borrowed = hot_standby_write_gate_adapter.bindStandby(&standby) },
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .last_lsn = &last_lsn,
            .failure_count = &failures,
        }),
    });
    defer standby_db.close();

    try standby_db.batchReplicatedApply(decoded.value.request);
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    var found = (try standby_db.lookup(alloc, "doc:a", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", found.json);
}

test "storage.hot_standby seed capture barrier prevents local commit without matching wal" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.heap.c_allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 251,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();

    var barrier: MutationBarrier = .{};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .mutation_barrier = &barrier,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    var capture = barrier.acquireExclusive();
    var write_probe = ConcurrentWriteProbe{ .db = &db };
    var write_thread = try std.testing.io.concurrent(ConcurrentWriteProbe.runBatch, .{&write_probe});
    errdefer {
        capture.release();
        write_thread.await(std.testing.io);
    }

    try std.testing.expect(waitForAtomicFlag(&write_probe.started, 1, 10_000));
    var attempts: usize = 0;
    while (attempts < 10_000) : (attempts += 1) {
        if (barrier.pendingSharedAcquisitions() > 0) break;
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }

    try std.testing.expect(barrier.pendingSharedAcquisitions() > 0);
    try std.testing.expectEqual(@as(u8, 0), write_probe.done.load(.monotonic));
    try std.testing.expectEqual(@as(u8, 0), write_probe.failed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), primary.lastLsn());
    try std.testing.expect((try db.get(alloc, "doc:b")) == null);

    capture.release();
    write_thread.await(std.testing.io);
    try std.testing.expectEqual(@as(u8, 0), write_probe.failed.load(.monotonic));
    try std.testing.expectEqual(@as(u8, 1), write_probe.done.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    const stored = (try db.get(alloc, "doc:b")) orelse return error.TestExpectedEqual;
    defer alloc.free(stored);
    try std.testing.expectEqualStrings("{\"title\":\"bravo\"}", stored);
}

test "storage.hot_standby seed snapshot predrains enrichment before exclusive capture" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    defer {
        var snapshots_buf: [512]u8 = undefined;
        if (std.fmt.bufPrint(&snapshots_buf, "{s}.snapshots", .{std.mem.span(db_path)})) |snapshots| {
            std.Io.Dir.cwd().deleteTree(std.testing.io, snapshots) catch {};
        } else |_| {}
    }
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 252,
        .shard_id = 5,
        .table_id = 11,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();

    var gated = GateDenseEmbedder{
        .allowed_successes = .init(0),
        .blocked_error = error.ResourceTemporarilyUnavailable,
    };
    var barrier: MutationBarrier = .{};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .identity_namespace = .{ .shard_id = 5, .table_id = 11 },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = gated.interface(),
            .inline_retry_max_attempts = 1,
        },
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .mutation_barrier = &barrier,
        }),
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" }},
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < default_test_wait_attempts) : (attempts += 1) {
        if (gated.blocked_requests.load(.acquire) > 0) break;
        sleepNs(10 * std.time.ns_per_ms);
    }
    try std.testing.expect(gated.blocked_requests.load(.acquire) > 0);

    // This is the production deadlock ordering: capture has frozen durable
    // mutations while enrichment still needs a shared lease to publish its
    // result. The final verification must return within its budget, never wait
    // forever with HA state locked.
    // Match production's injected deadline clock: native POSIX monotonic time
    // and std.Io's awake clock need not have the same epoch on every platform.
    const maintenance_clock = db.backend_runtime.monotonicClock();
    {
        var premature_capture = barrier.acquireExclusive();
        defer premature_capture.release();
        gated.allowAll();
        const premature_started_ns = maintenance_clock.nowRealtimeNs();
        if (db.snapshotWithMaintenanceDeadline("premature", premature_started_ns +| 50 * std.time.ns_per_ms)) |_| {
            return error.TestExpectedSeedSnapshotRuntimeBusy;
        } else |err| {
            try std.testing.expect(err == error.EnrichmentWaitTimeout or err == error.EnrichmentRetryInProgress);
        }
        try std.testing.expect(maintenance_clock.nowRealtimeNs() -| premature_started_ns < std.time.ns_per_s);
    }

    // Production performs this drain before taking the exclusive barrier.
    try db.drainSnapshotMaintenance(maintenance_clock.nowRealtimeNs() +| 10 * std.time.ns_per_s);
    var capture = barrier.acquireExclusive();
    defer capture.release();
    const snapshot_size = try db.snapshotWithMaintenanceDeadline(
        "predrained",
        maintenance_clock.nowRealtimeNs() +| std.time.ns_per_s,
    );
    try std.testing.expect(snapshot_size > 0);
}

test "storage.hot_standby fence cannot strand a local commit beyond the HA tail" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.heap.c_allocator;
    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 251,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    var public_gate = hot_standby_public_gate_state_mod.State{};
    public_gate.configurePrimary(&primary, false);
    var transition_mutex: std.atomic.Mutex = .unlocked;
    var barrier: MutationBarrier = .{};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .mutation_barrier = &barrier,
            .transition_mutex = &transition_mutex,
        }),
        .replication_write_gate = .{ .shared = .{ .state = public_gate.storageWriteState() } },
        .start_index_workers = false,
    });
    defer db.close();

    lockAtomic(&transition_mutex);
    var transition_locked = true;
    var write_probe = ConcurrentWriteProbe{ .db = &db };
    var write_thread = try std.testing.io.concurrent(ConcurrentWriteProbe.runBatch, .{&write_probe});
    var thread_joined = false;
    errdefer {
        if (transition_locked) transition_mutex.unlock();
        if (!thread_joined) write_thread.await(std.testing.io);
    }
    try std.testing.expect(waitForAtomicFlag(&write_probe.started, 1, 10_000));
    var local_commit_observed = false;
    for (0..10_000) |_| {
        if (try db.get(alloc, "doc:b")) |stored| {
            alloc.free(stored);
            local_commit_observed = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    try std.testing.expect(local_commit_observed);
    try std.testing.expectEqual(@as(u64, 0), primary.lastLsn());

    public_gate.publishPrimaryFence(true);
    transition_mutex.unlock();
    transition_locked = false;
    write_thread.await(std.testing.io);
    thread_joined = true;

    try std.testing.expectEqual(@as(u8, 1), write_probe.failed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    const stored = (try db.get(alloc, "doc:b")) orelse return error.TestExpectedEqual;
    defer alloc.free(stored);
    try std.testing.expectEqualStrings("{\"title\":\"bravo\"}", stored);
}

test "storage.hot_standby schema json mutation does not reacquire shared barrier behind queued capture" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 253,
        .shard_id = 6,
        .table_id = 12,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    var barrier: MutationBarrier = .{};
    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var failures = @import("antfly_platform").atomic.Value(u64).init(0);
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .identity_namespace = .{ .shard_id = 6, .table_id = 12 },
        .replication_async_metadata_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .mutation_barrier = &barrier,
            .last_lsn = &last_lsn,
            .failure_count = &failures,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    const CaptureProbe = struct {
        barrier: *MutationBarrier,
        started: std.atomic.Value(u8) = .init(0),
        acquired: std.atomic.Value(u8) = .init(0),
        release: std.atomic.Value(u8) = .init(0),

        fn run(self: *@This()) void {
            self.started.store(1, .release);
            var capture = self.barrier.acquireExclusive();
            self.acquired.store(1, .release);
            while (self.release.load(.acquire) == 0) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
            capture.release();
        }
    };

    var outer = barrier.acquireShared();
    var probe = CaptureProbe{ .barrier = &barrier };
    var capture_thread = try std.testing.io.concurrent(CaptureProbe.run, .{&probe});
    errdefer {
        outer.release();
        probe.release.store(1, .release);
        capture_thread.await(std.testing.io);
    }
    try std.testing.expect(waitForAtomicFlag(&probe.started, 1, 10_000));
    var capture_queued = false;
    for (0..10_000) |_| {
        if (barrier.pendingExclusiveAcquisitions() != 0) {
            capture_queued = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    try std.testing.expect(capture_queued);

    const schema_json =
        \\{"version":1,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    try db.setSchemaJson(alloc, schema_json);
    outer.release();
    try std.testing.expect(waitForAtomicFlag(&probe.acquired, 1, 10_000));
    probe.release.store(1, .release);
    capture_thread.await(std.testing.io);

    const stored = (try db.getSchemaJson(alloc)) orelse return error.TestExpectedEqual;
    defer alloc.free(stored);
    try std.testing.expectEqualStrings(schema_json, stored);
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
}

test "storage.hot_standby db evaluates sync commit gate for mirrored batch mutations" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 253,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var degraded = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    {
        var db = try DB.open(alloc, std.mem.span(db_path), .{
            .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
                .last_lsn = &last_lsn,
                .sync_policy = .{
                    .mode = .remote_write,
                    .standby_names = &standby_names,
                    .failure_policy = .degrade_to_async,
                },
                .last_gate_lsn = &gate_lsn,
                .last_gate_action = &gate_action,
                .sync_degraded_count = &degraded,
            }),
            .start_index_workers = false,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:sync", .value = "{\"title\":\"sync\"}" }},
            .sync_level = .write,
        });
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, replication_batch_outbox_key));
    }

    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@backingInt(hot_standby_commit_gate_mod.Action.acknowledge_degraded), gate_action.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), degraded.load(.acquire));
}

test "storage.hot_standby db block sync policy waits for standby acknowledgement" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 255,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    const SyncWait = struct {
        calls: u64 = 0,

        fn wait(ctx: *anyopaque, primary_arg_ctx: *anyopaque, target_lsn: u64, policy: hot_standby_primary_mod.SyncPolicy) !void {
            const primary_arg: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(primary_arg_ctx));
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            try std.testing.expectEqual(hot_standby_primary_mod.DurabilityMode.remote_write, policy.mode);
            try primary_arg.standbyStatusUpdate("standby-a", primary_arg.identity.timeline_id, target_lsn, 0);
        }
    };

    var wait_state = SyncWait{};
    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .last_lsn = &last_lsn,
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = SyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:block", .value = "{\"title\":\"block\"}" }},
        .sync_level = .write,
    });
    try std.testing.expectEqual(@as(u64, 1), wait_state.calls);
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@backingInt(hot_standby_commit_gate_mod.Action.acknowledge), gate_action.load(.acquire));
    var found = (try db.lookup(alloc, "doc:block", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"block\"}", found.json);
}

test "storage.hot_standby synchronous waits pipeline later commits by lsn" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.heap.c_allocator;
    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 255,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var io_impl = std.Io.Threaded.init(alloc, .{ .concurrent_limit = .limited(2) });
    defer io_impl.deinit();
    const io = io_impl.io();

    const SyncWait = struct {
        io: std.Io,
        first_waiting: std.atomic.Value(u8) = .init(0),
        second_acknowledged: std.atomic.Value(u8) = .init(0),

        fn wait(ctx: *anyopaque, primary_arg_ctx: *anyopaque, target_lsn: u64, _: hot_standby_primary_mod.SyncPolicy) !void {
            const primary_arg: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(primary_arg_ctx));
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (target_lsn == 1) {
                self.first_waiting.store(1, .release);
                const deadline = monotonicTimeNs() +| 5 * std.time.ns_per_s;
                while (self.second_acknowledged.load(.acquire) == 0 and monotonicTimeNs() < deadline)
                    try self.io.sleep(.fromMilliseconds(1), .awake);
                if (self.second_acknowledged.load(.acquire) == 0) return error.TestExpectedSecondCommitToPipeline;
                return;
            }
            try std.testing.expectEqual(@as(u64, 2), target_lsn);
            try primary_arg.standbyStatusUpdate("standby-a", primary_arg.identity.timeline_id, target_lsn, target_lsn);
            self.second_acknowledged.store(1, .release);
        }
    };
    const Write = struct {
        db: *DB,
        key: []const u8,
        value: []const u8,
        failed: std.atomic.Value(u8) = .init(0),

        fn run(self: *@This()) void {
            self.db.batch(.{
                .writes = &.{.{ .key = self.key, .value = self.value }},
                .sync_level = .write,
            }) catch {
                self.failed.store(1, .release);
            };
        }
    };

    var wait_state = SyncWait{ .io = io };
    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .last_lsn = &last_lsn,
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = SyncWait.wait,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    var first = Write{ .db = &db, .key = "doc:first", .value = "{\"title\":\"first\"}" };
    var first_future = std.Io.async(io, Write.run, .{&first});
    var first_awaited = false;
    defer if (!first_awaited) first_future.await(io);
    for (0..10_000) |_| {
        if (wait_state.first_waiting.load(.acquire) == 1) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(u8, 1), wait_state.first_waiting.load(.acquire));

    var second = Write{ .db = &db, .key = "doc:second", .value = "{\"title\":\"second\"}" };
    var second_future = std.Io.async(io, Write.run, .{&second});
    second_future.await(io);
    first_future.await(io);
    first_awaited = true;

    try std.testing.expectEqual(@as(u8, 0), first.failed.load(.acquire));
    try std.testing.expectEqual(@as(u8, 0), second.failed.load(.acquire));
    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 2), last_lsn.load(.acquire));
    const first_value = (try db.get(alloc, "doc:first")) orelse return error.TestExpectedEqual;
    defer alloc.free(first_value);
    const second_value = (try db.get(alloc, "doc:second")) orelse return error.TestExpectedEqual;
    defer alloc.free(second_value);
}

test "storage.hot_standby durable outbox recovery does not duplicate an appended batch" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 256,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    const SyncWait = struct {
        calls: usize = 0,

        fn wait(ctx: *anyopaque, primary_arg_ctx: *anyopaque, target_lsn: u64, _: hot_standby_primary_mod.SyncPolicy) !void {
            const primary_arg: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(primary_arg_ctx));
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            try primary_arg.standbyStatusUpdate("standby-a", primary_arg.identity.timeline_id, target_lsn, target_lsn);
        }
    };
    var wait_state = SyncWait{};
    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .last_lsn = &last_lsn,
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = SyncWait.wait,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    const request = types.BatchRequest{
        .writes = &.{.{ .key = "doc:recovered", .value = "{\"title\":\"recovered\"}" }},
        .sync_level = .write,
    };
    const payload = try replication_effects_mod.encodeBatchMutationRequestAlloc(alloc, request);
    defer alloc.free(payload);
    const from_lsn = primary.nextLsn();
    const outbox = try encodeDurableReplicationOutboxAlloc(alloc, from_lsn, payload);
    defer alloc.free(outbox);
    try db.core.store.putBatch(&.{.{ .key = replication_batch_outbox_key, .value = outbox }}, &.{});
    // Direct fixture insertion bypasses the normal writer publication fence.
    db.local_execution.durable_replication_outbox_maybe.store(true, .release);

    // Model a crash after the HA append succeeds but before the local outbox
    // delete commits. Recovery must acknowledge this exact record, not append
    // the non-idempotent request a second time.
    const appended_lsn = try @import("effects.zig").appendEncodedBatchMutationRequest(&primary, payload, .{
        .shard_id = 4,
        .table_id = 10,
    });
    try std.testing.expectEqual(@as(u64, 1), appended_lsn);
    try engine.test_support.flushDurableReplicationOutboxes(&db);

    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), wait_state.calls);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, replication_batch_outbox_key));
}

test "storage.hot_standby db session sync wait satisfies remote apply through standby DB apply" {
    const alloc = std.testing.allocator;

    var primary_db_path_tmp = try TestDirectory.init("db");
    defer primary_db_path_tmp.cleanup();
    const primary_db_path = primary_db_path_tmp.path().ptr;
    defer cleanupTempDir(primary_db_path);
    var standby_db_path_tmp = try TestDirectory.init("db");
    defer standby_db_path_tmp.cleanup();
    const standby_db_path = standby_db_path_tmp.path().ptr;
    defer cleanupTempDir(standby_db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);
    var standby_log_path_tmp = try TestDirectory.init("db");
    defer standby_log_path_tmp.cleanup();
    const standby_log_path = standby_log_path_tmp.path().ptr;
    defer cleanupTempDir(standby_log_path);
    var standby_progress_path_tmp = try TestDirectory.init("db");
    defer standby_progress_path_tmp.cleanup();
    const standby_progress_path = standby_progress_path_tmp.path().ptr;
    defer cleanupTempDir(standby_progress_path);

    const identity = hot_standby_standby_mod.Identity{
        .cluster_id = 257,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var standby = try hot_standby_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();

    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .replication_write_gate = .{ .borrowed = hot_standby_write_gate_adapter.bindStandby(&standby) },
        .start_index_workers = false,
    });
    defer standby_db.close();

    var wait_state = HotStandbySessionSyncWait{
        .alloc = alloc,
        .slot_name = "standby-a",
        .standby = &standby,
        .apply_ctx = &standby_db,
        .apply_fn = replication_ingress.applyCallback,
    };
    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var primary_db = try DB.open(alloc, std.mem.span(primary_db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .last_lsn = &last_lsn,
            .sync_policy = .{
                .mode = .remote_apply,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = HotStandbySessionSyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        }),
        .start_index_workers = false,
    });
    defer primary_db.close();

    try primary_db.batch(.{
        .writes = &.{.{ .key = "doc:remote-apply", .value = "{\"title\":\"remote-apply\"}" }},
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@backingInt(hot_standby_commit_gate_mod.Action.acknowledge), gate_action.load(.acquire));
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.replicationAppliedSequence());

    var found = (try standby_db.lookup(alloc, "doc:remote-apply", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"remote-apply\"}", found.json);
}

test "storage.hot_standby db allows progress but rejects acknowledgement when fenced during remote apply wait" {
    const alloc = std.testing.allocator;

    var primary_db_path_tmp = try TestDirectory.init("db");
    defer primary_db_path_tmp.cleanup();
    const primary_db_path = primary_db_path_tmp.path().ptr;
    defer cleanupTempDir(primary_db_path);
    var standby_db_path_tmp = try TestDirectory.init("db");
    defer standby_db_path_tmp.cleanup();
    const standby_db_path = standby_db_path_tmp.path().ptr;
    defer cleanupTempDir(standby_db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);
    var standby_log_path_tmp = try TestDirectory.init("db");
    defer standby_log_path_tmp.cleanup();
    const standby_log_path = standby_log_path_tmp.path().ptr;
    defer cleanupTempDir(standby_log_path);
    var standby_progress_path_tmp = try TestDirectory.init("db");
    defer standby_progress_path_tmp.cleanup();
    const standby_progress_path = standby_progress_path_tmp.path().ptr;
    defer cleanupTempDir(standby_progress_path);

    const identity = hot_standby_standby_mod.Identity{
        .cluster_id = 262,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var standby = try hot_standby_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();
    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .replication_write_gate = .{ .borrowed = hot_standby_write_gate_adapter.bindStandby(&standby) },
        .start_index_workers = false,
    });
    defer standby_db.close();

    var public_gate = hot_standby_public_gate_state_mod.State{};
    public_gate.configurePrimary(&primary, false);

    var transition_mutex: std.atomic.Mutex = .unlocked;
    var mutation_barrier: MutationBarrier = .{};
    const FencingRemoteApplyWait = struct {
        session: HotStandbySessionSyncWait,
        transition_mutex: *std.atomic.Mutex,
        mutation_barrier: *MutationBarrier,
        public_gate: *hot_standby_public_gate_state_mod.State,
        primary_db: ?*DB = null,
        calls: u64 = 0,

        fn wait(ctx: *anyopaque, primary_arg_ctx: *anyopaque, target_lsn: u64, policy: hot_standby_primary_mod.SyncPolicy) !void {
            const primary_arg: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(primary_arg_ctx));
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            // Remote progress must be able to enter while the client waits;
            // authority is serialized again for the final acknowledgement.
            try std.testing.expect(self.transition_mutex.tryLock());
            self.transition_mutex.unlock();
            const db = self.primary_db orelse return error.TestUnexpectedResult;
            // Point reads must remain live while the client acknowledgement
            // waits on remote durability. Holding the exclusive apply lock
            // here starves every reader behind an unavailable standby.
            try std.testing.expect(db.core.apply_mutex.tryLockShared());
            db.core.apply_mutex.unlockShared();
            // A replacement seed capture must be able to freeze the exact
            // local commit/HA-tail pair while this client waits for the missing
            // standby. Holding the mutation lease here deadlocks the operation
            // that can restore remote durability.
            var capture = self.mutation_barrier.tryAcquireExclusive() orelse
                return error.HASeedCaptureBlockedByRemoteDurabilityWait;
            capture.release();
            try HotStandbySessionSyncWait.wait(&self.session, primary_arg, target_lsn, policy);
            // Fence only after remote apply. The final client gate must observe
            // this transition after reacquiring the same mutex.
            lockAtomic(self.transition_mutex);
            self.public_gate.publishPrimaryFence(true);
            self.transition_mutex.unlock();
        }
    };
    var wait_state = FencingRemoteApplyWait{
        .session = .{
            .alloc = alloc,
            .slot_name = "standby-a",
            .standby = &standby,
            .apply_ctx = &standby_db,
            .apply_fn = replication_ingress.applyCallback,
        },
        .transition_mutex = &transition_mutex,
        .mutation_barrier = &mutation_barrier,
        .public_gate = &public_gate,
    };
    const standby_names = [_][]const u8{"standby-a"};
    var primary_db = try DB.open(alloc, std.mem.span(primary_db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .transition_mutex = &transition_mutex,
            .mutation_barrier = &mutation_barrier,
            .sync_policy = .{
                .mode = .remote_apply,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = FencingRemoteApplyWait.wait,
        }),
        .replication_write_gate = .{ .shared = .{ .state = public_gate.storageWriteState() } },
        .start_index_workers = false,
    });
    defer primary_db.close();
    wait_state.primary_db = &primary_db;

    try std.testing.expectError(error.HAFencedPrimary, primary_db.batch(.{
        .writes = &.{.{ .key = "doc:authority-expired", .value = "{\"title\":\"replicated-but-not-acknowledged\"}" }},
        .sync_level = .write,
    }));
    try std.testing.expectEqual(@as(u64, 1), wait_state.calls);

    // The mutation committed locally and reached remote apply before fencing,
    // but the stale client receives an error rather than success.
    var local = (try primary_db.lookup(alloc, "doc:authority-expired", .{})) orelse return error.TestExpectedEqual;
    defer local.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"replicated-but-not-acknowledged\"}", local.json);
    var remote = (try standby_db.lookup(alloc, "doc:authority-expired", .{})) orelse return error.TestExpectedEqual;
    defer remote.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"replicated-but-not-acknowledged\"}", remote.json);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.replicationAppliedSequence());
}

test "storage.hot_standby db session sync wait remote write acknowledges durable receive despite apply failure" {
    const alloc = std.testing.allocator;

    var primary_db_path_tmp = try TestDirectory.init("db");
    defer primary_db_path_tmp.cleanup();
    const primary_db_path = primary_db_path_tmp.path().ptr;
    defer cleanupTempDir(primary_db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);
    var standby_log_path_tmp = try TestDirectory.init("db");
    defer standby_log_path_tmp.cleanup();
    const standby_log_path = standby_log_path_tmp.path().ptr;
    defer cleanupTempDir(standby_log_path);
    var standby_progress_path_tmp = try TestDirectory.init("db");
    defer standby_progress_path_tmp.cleanup();
    const standby_progress_path = standby_progress_path_tmp.path().ptr;
    defer cleanupTempDir(standby_progress_path);

    const identity = hot_standby_standby_mod.Identity{
        .cluster_id = 258,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var standby = try hot_standby_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();

    const ApplyFailure = struct {
        calls: u64 = 0,

        fn apply(ctx: *anyopaque, _: replication_record_mod.RecordView) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            return error.IntentionalApplyFailure;
        }
    };

    var apply_failure = ApplyFailure{};
    var wait_state = HotStandbySessionSyncWait{
        .alloc = alloc,
        .slot_name = "standby-a",
        .standby = &standby,
        .apply_ctx = &apply_failure,
        .apply_fn = ApplyFailure.apply,
    };
    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var primary_db = try DB.open(alloc, std.mem.span(primary_db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .last_lsn = &last_lsn,
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = HotStandbySessionSyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        }),
        .start_index_workers = false,
    });
    defer primary_db.close();

    try primary_db.batch(.{
        .writes = &.{.{ .key = "doc:remote-write", .value = "{\"title\":\"remote-write\"}" }},
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@backingInt(hot_standby_commit_gate_mod.Action.acknowledge), gate_action.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), apply_failure.calls);
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 0), slot.applied_lsn);
    try std.testing.expectEqualStrings("IntentionalApplyFailure", slot.last_error.?);

    var found = (try primary_db.lookup(alloc, "doc:remote-write", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"remote-write\"}", found.json);
}

test "storage.hot_standby db primary progress sync wait observes reported remote apply ack" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 259,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    const RemoteAck = struct {
        calls: usize = 0,

        fn poll(ctx: *anyopaque, primary_arg: *hot_standby_primary_mod.Primary, target_lsn: u64, policy: hot_standby_primary_mod.SyncPolicy, round: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            try std.testing.expectEqual(hot_standby_primary_mod.DurabilityMode.remote_apply, policy.mode);
            try std.testing.expectEqual(self.calls - 1, round);
            if (self.calls == 1) {
                try primary_arg.standbyStatusUpdate("standby-a", primary_arg.identity.timeline_id, target_lsn, 0);
            } else {
                try primary_arg.standbyStatusUpdate("standby-a", primary_arg.identity.timeline_id, target_lsn, target_lsn);
            }
        }
    };

    var remote_ack = RemoteAck{};
    var wait_state = HotStandbyPrimaryProgressSyncWait{
        .max_rounds = 3,
        .poll_ctx = &remote_ack,
        .poll_fn = RemoteAck.poll,
    };
    var gate_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .sync_policy = .{
                .mode = .remote_apply,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = HotStandbyPrimaryProgressSyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:progress-wait", .value = "{\"title\":\"progress-wait\"}" }},
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 2), remote_ack.calls);
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@backingInt(hot_standby_commit_gate_mod.Action.acknowledge), gate_action.load(.acquire));
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
}

test "storage.hot_standby primary progress sync wait fast fails without enough eligible candidates" {
    const alloc = std.testing.allocator;

    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 260,
        .shard_id = 4,
        .table_id = 11,
        .timeline_id = 2,
        .epoch = 2,
    }, .{});
    defer primary.close();
    const target_lsn = try primary.append(.{ .payload = "locally-committed-after-promotion" });

    const Poll = struct {
        calls: usize = 0,

        fn poll(ctx: *anyopaque, _: *hot_standby_primary_mod.Primary, _: u64, _: hot_standby_primary_mod.SyncPolicy, round: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try std.testing.expectEqual(self.calls, round);
            self.calls += 1;
        }
    };

    var poll = Poll{};
    var wait_state = HotStandbyPrimaryProgressSyncWait{
        .max_rounds = 200,
        .poll_ctx = &poll,
        .poll_fn = Poll.poll,
    };
    const standby_names = [_][]const u8{"former-primary"};
    try std.testing.expectError(
        error.HASyncCommitWouldBlock,
        HotStandbyPrimaryProgressSyncWait.wait(&wait_state, &primary, target_lsn, .{
            .mode = .remote_apply,
            .standby_names = &standby_names,
            .failure_policy = .block,
        }),
    );
    try std.testing.expectEqual(@as(usize, 1), poll.calls);
}

test "storage.hot_standby db primary progress sync wait returns would block without reported ack" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 260,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var wait_state = HotStandbyPrimaryProgressSyncWait{ .max_rounds = 1 };
    var gate_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = HotStandbyPrimaryProgressSyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.HASyncCommitWouldBlock, db.batch(.{
        .writes = &.{.{ .key = "doc:progress-timeout", .value = "{\"title\":\"progress-timeout\"}" }},
        .sync_level = .write,
    }));

    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@backingInt(hot_standby_commit_gate_mod.Action.wait_for_standby), gate_action.load(.acquire));
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 0), slot.received_lsn);
}

test "storage.hot_standby pending acknowledgement preserves batch and replay tail order" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 264,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var wait_state = HotStandbyPrimaryProgressSyncWait{ .max_rounds = 1 };
    const standby_names = [_][]const u8{"standby-a"};
    const mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
        .sync_policy = .{
            .mode = .remote_write,
            .standby_names = &standby_names,
            .failure_policy = .block,
        },
        .sync_wait_ctx = &wait_state,
        .sync_wait_fn = HotStandbyPrimaryProgressSyncWait.wait,
    });
    var batch_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var replay_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_async_batch_mirror = blk: {
            var configured = hot_standby_publisher_adapter.options(mirror);
            configured.last_lsn = &batch_lsn;
            break :blk hot_standby_publisher_adapter.bindMirror(&primary, configured);
        },
        .replication_async_effect_mirror = blk: {
            var configured = hot_standby_publisher_adapter.options(mirror);
            configured.last_lsn = &replay_lsn;
            break :blk hot_standby_publisher_adapter.bindMirror(&primary, configured);
        },
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.HASyncCommitWouldBlock, db.batch(.{
        .writes = &.{.{ .key = "doc:pending-tail", .value = "{\"title\":\"pending-tail\"}" }},
        .sync_level = .write,
    }));

    // A pending client result cannot omit the replay/effect record for a local
    // commit. Both records are appended in commit order before either remote
    // gate is allowed to wait or fail.
    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), batch_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 2), replay_lsn.load(.acquire));
    var local = (try db.lookup(alloc, "doc:pending-tail", .{})) orelse return error.TestExpectedEqual;
    defer local.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"pending-tail\"}", local.json);
}

test "db transaction HA retry drains durable mirror outbox" {
    const alloc = std.testing.allocator;
    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 263,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    const AckOnRetry = struct {
        calls: usize = 0,
        fn wait(ctx: *anyopaque, active_primary_ctx: *anyopaque, target_lsn: u64, _: hot_standby_primary_mod.SyncPolicy) !void {
            const active_primary: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(active_primary_ctx));
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            if (self.calls == 1) return error.InjectedMirrorWaitFailure;
            try active_primary.standbyStatusUpdate("standby-a", 1, target_lsn, target_lsn);
        }
    };
    var ack = AckOnRetry{};
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &ack,
            .sync_wait_fn = AckOnRetry.wait,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    const txn_id = try db.beginTransaction(20_000);
    try db.writeTransaction(txn_id, .{ .writes = &.{.{
        .key = "doc:ha-txn",
        .value = "{\"title\":\"committed\"}",
    }} });
    try std.testing.expectError(error.InjectedMirrorWaitFailure, db.commitTransaction(txn_id, 20_001));
    try std.testing.expectEqual(transactions_mod.TxnStatus.committed, try db.getTransactionStatus(txn_id));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    const configured_mirror = db.local_execution.replication_async_batch_mirror;
    db.local_execution.replication_async_batch_mirror = null;
    try std.testing.expectError(error.ReplicationPublisherUnavailable, db.commitTransaction(txn_id, 20_001));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    db.local_execution.replication_async_batch_mirror = configured_mirror;

    try db.commitTransaction(txn_id, 20_001);
    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
    try db.commitTransaction(txn_id, 20_001);
    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
}

test "storage.hot_standby db primary progress sync wait survives primary restart before ack" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    const identity = hot_standby_primary_mod.Identity{
        .cluster_id = 261,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    const standby_names = [_][]const u8{"standby-a"};
    const policy = hot_standby_primary_mod.SyncPolicy{
        .mode = .remote_apply,
        .standby_names = &standby_names,
        .failure_policy = .block,
    };

    var target_lsn: u64 = 0;
    {
        var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, identity, .{});
        defer primary.close();
        try primary.createSlot("standby-a", 0);

        var wait_state = HotStandbyPrimaryProgressSyncWait{ .max_rounds = 1 };
        var gate_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
        var gate_action = std.atomic.Value(u8).init(255);
        var waits = @import("antfly_platform").atomic.Value(u64).init(0);
        var db = try DB.open(alloc, std.mem.span(db_path), .{
            .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
                .sync_policy = policy,
                .sync_wait_ctx = &wait_state,
                .sync_wait_fn = HotStandbyPrimaryProgressSyncWait.wait,
                .last_gate_lsn = &gate_lsn,
                .last_gate_action = &gate_action,
                .sync_wait_count = &waits,
            }),
            .start_index_workers = false,
        });
        defer db.close();

        try std.testing.expectError(error.HASyncCommitWouldBlock, db.batch(.{
            .writes = &.{.{ .key = "doc:restart-before-ack", .value = "{\"title\":\"restart-before-ack\"}" }},
            .sync_level = .write,
        }));
        target_lsn = primary.lastLsn();
        try std.testing.expectEqual(@as(u64, 1), target_lsn);
        try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
        try std.testing.expectEqual(@backingInt(hot_standby_commit_gate_mod.Action.wait_for_standby), gate_action.load(.acquire));

        const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 0), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 0), slot.applied_lsn);
    }

    {
        var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, identity, .{});
        defer primary.close();
        try std.testing.expectEqual(target_lsn, primary.lastLsn());

        var wait_state = HotStandbyPrimaryProgressSyncWait{ .max_rounds = 1 };
        try std.testing.expectError(
            error.HASyncCommitWouldBlock,
            HotStandbyPrimaryProgressSyncWait.wait(&wait_state, &primary, target_lsn, policy),
        );

        try primary.standbyStatusUpdate("standby-a", identity.timeline_id, target_lsn, target_lsn);
        try HotStandbyPrimaryProgressSyncWait.wait(&wait_state, &primary, target_lsn, policy);

        const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(target_lsn, slot.received_lsn);
        try std.testing.expectEqual(target_lsn, slot.applied_lsn);
    }
}

test "storage.hot_standby db block sync policy surfaces wait provider errors" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 256,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    const SyncWait = struct {
        calls: u64 = 0,

        fn timeout(ctx: *anyopaque, _: *anyopaque, _: u64, _: hot_standby_primary_mod.SyncPolicy) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            return error.HASyncCommitWaitTimeout;
        }
    };

    var wait_state = SyncWait{};
    var gate_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .sync_policy = .{
                .mode = .remote_apply,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = SyncWait.timeout,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.HASyncCommitWaitTimeout, db.batch(.{
        .writes = &.{.{ .key = "doc:timeout", .value = "{\"title\":\"timeout\"}" }},
        .sync_level = .write,
    }));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), wait_state.calls);
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@backingInt(hot_standby_commit_gate_mod.Action.wait_for_standby), gate_action.load(.acquire));
}

test "storage.hot_standby db fail-closed sync policy rejects before local batch commit" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 254,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var gate_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var rejected = @import("antfly_platform").atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .fail_closed,
            },
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_reject_count = &rejected,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.SyncPolicyUnsatisfied, db.batch(.{
        .writes = &.{.{ .key = "doc:rejected", .value = "{\"title\":\"rejected\"}" }},
        .sync_level = .write,
    }));
    try std.testing.expectEqual(@as(u64, 0), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@backingInt(hot_standby_commit_gate_mod.Action.reject), gate_action.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), rejected.load(.acquire));
    try std.testing.expect((try db.lookup(alloc, "doc:rejected", .{})) == null);
}

test "storage.hot_standby schema wait failure reports unknown after durable local commit" {
    const alloc = std.testing.allocator;
    const public_schema_json =
        \\{"version":7,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);

    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, .{
        .cluster_id = 265,
        .shard_id = 5,
        .table_id = 12,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    const FailOnceWait = struct {
        calls: usize = 0,

        fn wait(ctx: *anyopaque, active_primary_ctx: *anyopaque, target_lsn: u64, _: hot_standby_primary_mod.SyncPolicy) !void {
            const active_primary: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(active_primary_ctx));
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            if (self.calls == 1) return error.InjectedSchemaMirrorWaitFailure;
            try active_primary.standbyStatusUpdate("standby-a", active_primary.identity.timeline_id, target_lsn, target_lsn);
        }
    };
    var wait_state = FailOnceWait{};
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .identity_namespace = .{ .shard_id = 5, .table_id = 12 },
        .replication_async_metadata_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .sync_policy = .{
                .mode = .remote_apply,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = FailOnceWait.wait,
        }),
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.DurabilityOutcomeUnknown, db.setSchemaJson(alloc, public_schema_json));
    try std.testing.expectEqual(@as(usize, 1), wait_state.calls);
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    const stored = (try db.getSchemaJson(alloc)) orelse return error.TestExpectedEqual;
    defer alloc.free(stored);
    try std.testing.expectEqualStrings(public_schema_json, stored);
    const loaded_schema = (try schema_mod.loadSchema(db.core.store, alloc)) orelse return error.TestExpectedEqual;
    defer schema_mod.freeSchema(alloc, loaded_schema);
    try std.testing.expectEqual(@as(u32, 7), loaded_schema.version);

    const pending = try db.core.store.scanPrefixPage(alloc, replication_outbox_v2_prefix, null, 2);
    defer docstore_mod.DocStore.freeResults(alloc, pending);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    try std.testing.expectEqual(DurableReplicationOutboxKind.schema, try durableReplicationOutboxKindFromKey(pending[0].key));

    // Recovery recognizes the already-appended schema record, obtains the
    // missing acknowledgement, and clears the exact mutation-scoped outbox.
    try engine.test_support.flushDurableReplicationOutboxes(&db);
    try std.testing.expectEqual(@as(usize, 2), wait_state.calls);
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    const remaining = try db.core.store.scanPrefixPage(alloc, replication_outbox_v2_prefix, null, 2);
    defer docstore_mod.DocStore.freeResults(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "storage.hot_standby db mirrors and applies schema metadata mutation records" {
    const alloc = std.testing.allocator;
    const public_schema_json =
        \\{"version":12,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","enum":["active"]}},"required":["id","status"],"additionalProperties":false}}}}
    ;

    var primary_db_path_tmp = try TestDirectory.init("db");
    defer primary_db_path_tmp.cleanup();
    const primary_db_path = primary_db_path_tmp.path().ptr;
    defer cleanupTempDir(primary_db_path);
    var standby_db_path_tmp = try TestDirectory.init("db");
    defer standby_db_path_tmp.cleanup();
    const standby_db_path = standby_db_path_tmp.path().ptr;
    defer cleanupTempDir(standby_db_path);
    var replication_log_path_tmp = try TestDirectory.init("db");
    defer replication_log_path_tmp.cleanup();
    const replication_log_path = replication_log_path_tmp.path().ptr;
    defer cleanupTempDir(replication_log_path);
    var replication_slots_path_tmp = try TestDirectory.init("db");
    defer replication_slots_path_tmp.cleanup();
    const replication_slots_path = replication_slots_path_tmp.path().ptr;
    defer cleanupTempDir(replication_slots_path);
    var standby_log_path_tmp = try TestDirectory.init("db");
    defer standby_log_path_tmp.cleanup();
    const standby_log_path = standby_log_path_tmp.path().ptr;
    defer cleanupTempDir(standby_log_path);
    var standby_progress_path_tmp = try TestDirectory.init("db");
    defer standby_progress_path_tmp.cleanup();
    const standby_progress_path = standby_progress_path_tmp.path().ptr;
    defer cleanupTempDir(standby_progress_path);

    const identity = hot_standby_standby_mod.Identity{
        .cluster_id = 252,
        .shard_id = 5,
        .table_id = 11,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try hot_standby_primary_mod.Primary.open(alloc, replication_log_path, replication_slots_path, identity, .{});
    defer primary.close();
    var standby = try hot_standby_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();

    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    var failures = @import("antfly_platform").atomic.Value(u64).init(0);
    {
        var db = try DB.open(alloc, std.mem.span(primary_db_path), .{
            .identity_namespace = .{ .shard_id = 5, .table_id = 11 },
            .replication_async_metadata_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
                .last_lsn = &last_lsn,
                .failure_count = &failures,
            }),
            .start_index_workers = false,
        });
        defer db.close();

        try db.setSchemaJson(alloc, public_schema_json);
    }

    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(@as(@TypeOf(entry.record.kind), .metadata_mutation), entry.record.kind);
    try std.testing.expectEqual(@as(u64, 252), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 5), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 11), entry.record.table_id);

    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .identity_namespace = .{ .shard_id = 5, .table_id = 11 },
        .replication_write_gate = .{ .borrowed = hot_standby_write_gate_adapter.bindStandby(&standby) },
        .start_index_workers = false,
    });
    defer standby_db.close();

    try std.testing.expectError(error.HAReadOnlyStandby, standby_db.setSchema(.{ .version = 99 }));
    try replication_ingress.applyRecord(&standby_db, entry.record);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.replicationAppliedSequence());

    const replicated_schema = (try schema_mod.loadSchema(standby_db.core.store, alloc)).?;
    defer schema_mod.freeSchema(alloc, replicated_schema);
    try std.testing.expectEqual(@as(u32, 12), replicated_schema.version);
    try std.testing.expectEqualStrings("row", replicated_schema.default_type);
    try std.testing.expectEqual(schema_mod.StorageMode.relational, replicated_schema.storage_mode);
    const replicated_public_schema = (try standby_db.getSchemaJson(alloc)) orelse return error.TestExpectedEqual;
    defer alloc.free(replicated_public_schema);
    try std.testing.expectEqualStrings(public_schema_json, replicated_public_schema);

    // Promotion must preserve public constraints, not merely the physical row
    // codec. Removing the test-only standby gate models the authority handoff.
    standby_db.local_execution.replication_write_gate = null;
    try std.testing.expectError(error.InvalidBatchRequest, standby_db.batch(.{
        .writes = &.{.{
            .key = "row:invalid",
            .value = "{\"id\":\"invalid\",\"status\":\"inactive\"}",
        }},
    }));
    try standby_db.batch(.{
        .writes = &.{.{
            .key = "row:valid",
            .value = "{\"id\":\"valid\",\"status\":\"active\"}",
        }},
    });

    try replication_ingress.applyRecord(&standby_db, entry.record);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.replicationAppliedSequence());
}

test "storage.hot_standby row policy metadata publication replays with its exact Raft cut" {
    const alloc = std.testing.allocator;
    var primary_tmp = try TestDirectory.init("ha-policy-primary");
    defer primary_tmp.cleanup();
    var replica_tmp = try TestDirectory.init("ha-policy-replica");
    defer replica_tmp.cleanup();
    var log_tmp = try TestDirectory.init("ha-policy-log");
    defer log_tmp.cleanup();
    var slots_tmp = try TestDirectory.init("ha-policy-slots");
    defer slots_tmp.cleanup();
    const identity = hot_standby_standby_mod.Identity{ .cluster_id = 421, .shard_id = 8, .table_id = 7, .timeline_id = 1, .epoch = 1 };
    var primary = try hot_standby_primary_mod.Primary.open(alloc, std.mem.span(log_tmp.path().ptr), std.mem.span(slots_tmp.path().ptr), identity, .{});
    defer primary.close();
    var mutation_barrier = MutationBarrier{};
    const namespace: DocIdentityNamespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 };
    var owner = try DB.open(alloc, std.mem.span(primary_tmp.path().ptr), .{
        .identity_namespace = namespace,
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{ .mutation_barrier = &mutation_barrier }),
        .replication_async_metadata_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{ .mutation_barrier = &mutation_barrier }),
        .start_index_workers = false,
        .start_optional_runtimes = false,
    });
    var owner_open = true;
    defer if (owner_open) owner.close();
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    try owner.setSchemaJson(alloc, schema_json);
    var replica = try DB.open(alloc, std.mem.span(replica_tmp.path().ptr), .{ .identity_namespace = namespace, .start_index_workers = false, .start_optional_runtimes = false });
    defer replica.close();
    var schema_entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer schema_entry.deinit(alloc);
    try replication_ingress.applyRecord(&replica, schema_entry.record);
    try owner.batch(.{ .writes = &.{.{ .key = "row:a", .value = "{\"id\":\"a\"}" }} });
    var row_entry = (try primary.log.entryAt(alloc, 2)) orelse return error.TestExpectedEqual;
    defer row_entry.deinit(alloc);
    try std.testing.expectEqual(replication_record_mod.RecordKind.batch_mutation, row_entry.record.kind);
    try replication_ingress.applyRecord(&replica, row_entry.record);
    const schema_bytes = try schema_mod.serializeSchema(alloc, owner.core.schema.?);
    defer alloc.free(schema_bytes);
    var schema_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(schema_bytes, &schema_digest, .{});
    const policy: @import("antfly_local_sources").system_catalog_policies.Record = .{
        .id = 1,
        .generation = 1,
        .table_id = 7,
        .schema_version = 1,
        .schema_digest = schema_digest,
        .name = "visible",
        .commands = .{ .select = true },
        .roles = &.{"PUBLIC"},
        .using = .{ .instructions = &.{.{ .type = .{ .kind = .boolean }, .operation = .{ .literal = .{ .bool = true } } }}, .root = 0 },
    };
    const bundle = try std.json.Stringify.valueAlloc(alloc, @import("antfly_local_sources").system_catalog_policies.InstallSnapshot{
        .table_id = 7,
        .schema_version = 1,
        .schema_digest = schema_digest,
        .policy_generation = 1,
        .catalog_epoch = 2,
        .phase = .pending_install,
        .records = &.{policy},
        .settings = &.{},
    }, .{});
    defer alloc.free(bundle);
    const range = owner.core.byteRange();
    var request: @import("antfly_local_sources").system_catalog_policies.InstallRequest = .{
        .table_id = 7,
        .expected_generation = 1,
        .expected_catalog_epoch = 2,
        .expected_phase = .pending_install,
        .owner_group_id = 17,
        .expected_descriptor_digest = try (@import("antfly_local_sources").system_catalog_policies.OwnerDescriptor{
            .table_id = 7,
            .group_id = 17,
            .shard_id = 8,
            .range_id = 9,
            .schema_version = 1,
            .schema_digest = schema_digest,
            .range_start = range.start,
            .range_end = range.end,
        }).digest(),
    };
    try std.testing.expect((try owner.applyReplicatedRowPolicyPublication(bundle, request, .{ .term = 3, .index = 11 })) == null);
    try std.testing.expectEqual(@as(u64, 3), primary.lastLsn());
    var policy_entry = (try primary.log.entryAt(alloc, 3)) orelse return error.TestExpectedEqual;
    defer policy_entry.deinit(alloc);
    try std.testing.expectEqual(replication_record_mod.RecordKind.metadata_mutation, policy_entry.record.kind);
    try replication_ingress.applyRecord(&replica, policy_entry.record);
    try replication_ingress.applyRecord(&replica, policy_entry.record);
    try std.testing.expectEqual(@as(u64, 3), try replica.replicationAppliedSequence());
    try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.preparing, replica.local_execution.row_policy_gate.currentPhase());
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, replica.get(alloc, "unseen"));
    const receipt = try replica.loadRowPolicyReceipt(1, .pending_install);
    try std.testing.expectEqual(@as(u64, 11), receipt.applied_index);
    try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.preparing, replica.local_execution.row_policy_gate.currentPhase());

    var serving = try std.json.parseFromSlice(@import("antfly_local_sources").system_catalog_policies.InstallSnapshot, alloc, bundle, .{});
    defer serving.deinit();
    serving.value.phase = .serving_install;
    const serving_bundle = try std.json.Stringify.valueAlloc(alloc, serving.value, .{});
    defer alloc.free(serving_bundle);
    request.expected_phase = .serving_install;
    try std.testing.expect((try owner.applyReplicatedRowPolicyPublication(serving_bundle, request, .{ .term = 3, .index = 12 })) == null);
    _ = try owner.loadRowPolicyReceipt(1, .serving_install);
    try std.testing.expectEqual(@as(u64, 4), primary.lastLsn());
    var serving_entry = (try primary.log.entryAt(alloc, 4)) orelse return error.TestExpectedEqual;
    defer serving_entry.deinit(alloc);
    try replication_ingress.applyRecord(&replica, serving_entry.record);
    _ = try replica.loadRowPolicyReceipt(1, .serving_install);
    try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.active, replica.local_execution.row_policy_gate.currentPhase());
    try std.testing.expectEqual(@as(u64, 4), try replica.replicationAppliedSequence());

    // Simulate a crash after the exact policy WAL append but before local
    // outbox deletion. Reopen must reconcile, not duplicate, that record.
    const from_lsn = serving_entry.record.lsn;
    const outbox_bytes = try encodeDurableReplicationOutboxAlloc(alloc, from_lsn, serving_entry.record.payload);
    defer alloc.free(outbox_bytes);
    const outbox_key = try durableReplicationOutboxKeyAlloc(alloc, .row_policy, from_lsn, owner.core.root_generation, serving_entry.record.payload);
    defer alloc.free(outbox_key);
    try owner.core.store.put(outbox_key, outbox_bytes);
    owner.close();
    owner_open = false;
    var reopened = try DB.open(alloc, std.mem.span(primary_tmp.path().ptr), .{
        .identity_namespace = namespace,
        .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{ .mutation_barrier = &mutation_barrier }),
        .replication_async_metadata_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{ .mutation_barrier = &mutation_barrier }),
        .start_index_workers = false,
        .start_optional_runtimes = false,
    });
    defer reopened.close();
    try std.testing.expectEqual(table_catalog_mod.RowPolicyPhase.active, reopened.local_execution.row_policy_gate.currentPhase());
    try engine.test_support.ensureDurableReplicationStartupBarrier(&reopened);
    try std.testing.expectEqual(@as(u64, 4), primary.lastLsn());
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, outbox_key));
}

test "storage.hot_standby db applies batch mutation records through replication session callback" {
    const alloc = std.testing.allocator;

    var standby_db_path_tmp = try TestDirectory.init("db");
    defer standby_db_path_tmp.cleanup();
    const standby_db_path = standby_db_path_tmp.path().ptr;
    defer cleanupTempDir(standby_db_path);
    var primary_log_path_tmp = try TestDirectory.init("db");
    defer primary_log_path_tmp.cleanup();
    const primary_log_path = primary_log_path_tmp.path().ptr;
    defer cleanupTempDir(primary_log_path);
    var primary_slots_path_tmp = try TestDirectory.init("db");
    defer primary_slots_path_tmp.cleanup();
    const primary_slots_path = primary_slots_path_tmp.path().ptr;
    defer cleanupTempDir(primary_slots_path);
    var standby_log_path_tmp = try TestDirectory.init("db");
    defer standby_log_path_tmp.cleanup();
    const standby_log_path = standby_log_path_tmp.path().ptr;
    defer cleanupTempDir(standby_log_path);
    var standby_progress_path_tmp = try TestDirectory.init("db");
    defer standby_progress_path_tmp.cleanup();
    const standby_progress_path = standby_progress_path_tmp.path().ptr;
    defer cleanupTempDir(standby_progress_path);

    const identity = hot_standby_standby_mod.Identity{
        .cluster_id = 251,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try hot_standby_primary_mod.Primary.open(alloc, primary_log_path, primary_slots_path, identity, .{});
    defer primary.close();
    var standby = try hot_standby_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();
    try primary.createSlot("standby-a", 0);

    _ = try @import("effects.zig").appendBatchMutationRequest(alloc, &primary, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"replicated-session\"}" }},
        .sync_level = .full_index,
    }, .{});
    _ = try primary.append(.{
        .kind = .backup_start,
        .payload_codec = .json,
        .payload = "{\"manifest_id\":\"base-session\"}",
    });
    _ = try @import("effects.zig").appendDerivedChangeRecord(alloc, &primary, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    }, .{});

    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .replication_write_gate = .{ .borrowed = hot_standby_write_gate_adapter.bindStandby(&standby) },
        .start_index_workers = false,
    });
    defer standby_db.close();

    const result = try hot_standby_session_mod.replicateAvailable(
        alloc,
        &primary,
        "standby-a",
        &standby,
        &standby_db,
        replication_ingress.applyCallback,
    );
    try std.testing.expectEqual(@as(usize, 3), result.received_count);
    try std.testing.expectEqual(@as(usize, 3), result.applied_count);
    try std.testing.expectEqual(@as(u64, 3), result.progress.received_lsn);
    try std.testing.expectEqual(@as(u64, 3), result.progress.applied_lsn);

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 3), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 3), slot.applied_lsn);
    try std.testing.expectEqual(@as(u64, 3), try standby_db.replicationAppliedSequence());

    var found = (try standby_db.lookup(alloc, "doc:a", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"replicated-session\"}", found.json);

    const replay_entries = try replay_stream_mod.iterateFrom(alloc, standby_db.core.store, 1);
    defer {
        for (replay_entries) |*entry| entry.deinit(alloc);
        alloc.free(replay_entries);
    }
    try std.testing.expectEqual(@as(usize, 2), replay_entries.len);
    try std.testing.expectEqual(@as(u64, 1), replay_entries[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), replay_entries[1].sequence);

    var replicated_effect_record = try change_journal_mod.decodeRecord(alloc, replay_entries[1].payload);
    defer replicated_effect_record.deinit();
    try std.testing.expectEqual(@as(u64, 2), replicated_effect_record.record.sequence);
    try std.testing.expectEqualStrings("doc:a", replicated_effect_record.record.changed_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 1), replicated_effect_record.record.target_hints.len);
    try std.testing.expectEqual(change_journal_mod.TargetHint.full_text, replicated_effect_record.record.target_hints[0]);

    var duplicate_batch = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer duplicate_batch.deinit(alloc);
    try replication_ingress.applyRecord(&standby_db, duplicate_batch.record);
    var duplicate_derived = (try primary.log.entryAt(alloc, 3)) orelse return error.TestExpectedEqual;
    defer duplicate_derived.deinit(alloc);
    try replication_ingress.applyRecord(&standby_db, duplicate_derived.record);
    try std.testing.expectEqual(@as(u64, 3), try standby_db.replicationAppliedSequence());

    const replay_after_duplicates = try replay_stream_mod.iterateFrom(alloc, standby_db.core.store, 1);
    defer {
        for (replay_after_duplicates) |*entry| entry.deinit(alloc);
        alloc.free(replay_after_duplicates);
    }
    try std.testing.expectEqual(@as(usize, 2), replay_after_duplicates.len);
}

test "storage.hot_standby db persists applied replication marker across reopen" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var primary_log_path_tmp = try TestDirectory.init("db");
    defer primary_log_path_tmp.cleanup();
    const primary_log_path = primary_log_path_tmp.path().ptr;
    defer cleanupTempDir(primary_log_path);
    var primary_slots_path_tmp = try TestDirectory.init("db");
    defer primary_slots_path_tmp.cleanup();
    const primary_slots_path = primary_slots_path_tmp.path().ptr;
    defer cleanupTempDir(primary_slots_path);

    const identity = hot_standby_standby_mod.Identity{
        .cluster_id = 252,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try hot_standby_primary_mod.Primary.open(alloc, primary_log_path, primary_slots_path, identity, .{});
    defer primary.close();

    _ = try @import("effects.zig").appendBatchMutationRequest(alloc, &primary, .{
        .writes = &.{.{ .key = "doc:persisted-marker", .value = "{\"title\":\"persisted\"}" }},
        .sync_level = .write,
    }, .{});
    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);

    {
        var db = try DB.open(alloc, std.mem.span(db_path), .{ .start_index_workers = false });
        defer db.close();
        try replication_ingress.applyRecord(&db, entry.record);
        try std.testing.expectEqual(@as(u64, 1), try db.replicationAppliedSequence());
    }

    var reopened = try DB.open(alloc, std.mem.span(db_path), .{ .start_index_workers = false });
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 1), try reopened.replicationAppliedSequence());
    try replication_ingress.applyRecord(&reopened, entry.record);
    try std.testing.expectEqual(@as(u64, 1), try reopened.replicationAppliedSequence());

    const replay_entries = try replay_stream_mod.iterateFrom(alloc, reopened.core.store, 1);
    defer {
        for (replay_entries) |*replay_entry| replay_entry.deinit(alloc);
        alloc.free(replay_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), replay_entries.len);
    try std.testing.expectEqual(@as(u64, 1), replay_entries[0].sequence);
}

test "storage.hot_standby db write gate rejects client writes on standby but allows replicated apply" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var standby_log_path_tmp = try TestDirectory.init("db");
    defer standby_log_path_tmp.cleanup();
    const standby_log_path = standby_log_path_tmp.path().ptr;
    defer cleanupTempDir(standby_log_path);
    var standby_progress_path_tmp = try TestDirectory.init("db");
    defer standby_progress_path_tmp.cleanup();
    const standby_progress_path = standby_progress_path_tmp.path().ptr;
    defer cleanupTempDir(standby_progress_path);

    var standby = try hot_standby_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, .{
        .cluster_id = 300,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer standby.close();

    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_write_gate = .{ .borrowed = hot_standby_write_gate_adapter.bindStandby(&standby) },
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.HAReadOnlyStandby, db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"client\"}" }},
    }));
    try std.testing.expectError(error.HAReadOnlyStandby, db.beginBulkIngestSession());
    try std.testing.expectError(error.HAReadOnlyStandby, db.beginDenseAutoBulkIngestSession());
    try std.testing.expectError(error.HAReadOnlyStandby, db.beginPrimaryStoreAutoBulkIngestSession());
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.finishBulkIngestSessionWithOptions(.{}),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.finishDenseAutoBulkIngestSessionWithOptions(.{}),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.finishPrimaryStoreAutoBulkIngestSessionWithOptions(.{}),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.updateRange(.{ .start = "doc:a", .end = "doc:z" }),
    );

    try db.batchReplicatedApply(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"replicated\"}" }},
    });
    var found = (try db.lookup(alloc, "doc:a", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"replicated\"}", found.json);
}

test "storage.hot_standby db write gate rejects fenced former primary writes" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var primary_log_path_tmp = try TestDirectory.init("db");
    defer primary_log_path_tmp.cleanup();
    const primary_log_path = primary_log_path_tmp.path().ptr;
    defer cleanupTempDir(primary_log_path);
    var primary_slots_path_tmp = try TestDirectory.init("db");
    defer primary_slots_path_tmp.cleanup();
    const primary_slots_path = primary_slots_path_tmp.path().ptr;
    defer cleanupTempDir(primary_slots_path);
    var fence_path_tmp = try TestDirectory.init("db");
    defer fence_path_tmp.cleanup();
    const fence_path = fence_path_tmp.path().ptr;
    defer cleanupTempDir(fence_path);

    const identity = hot_standby_primary_mod.Identity{
        .cluster_id = 301,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try hot_standby_primary_mod.Primary.open(alloc, primary_log_path, primary_slots_path, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "before-fence" });

    var fence_store = try hot_standby_fencing_mod.Store.open(alloc, fence_path, .{});
    defer fence_store.close();
    const receipt = try fence_store.acquirePromotionFence(.{
        .identity = identity,
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .generation = 1,
        .required_lsn = 1,
        .observed_lsn = 1,
        .reason = "db-write-gate-test",
    });
    defer hot_standby_fencing_mod.freeReceipt(alloc, receipt);

    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_write_gate = .{ .captured = hot_standby_write_gate_adapter.bindFencedPrimary(.{
            .primary = &primary,
            .fence_store = &fence_store,
            .node_id = "primary-a",
        }) },
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.HAFencedPrimary, db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"blocked\"}" }},
    }));

    const gate = db.local_execution.replication_write_gate orelse return error.TestExpectedEqual;
    switch (gate) {
        .captured => |fenced| {
            const decision = try hot_standby_write_gate_mod.evaluateFencedPrimary(try hot_standby_write_gate_mod.runtimeFencedPrimary(fenced), .{});
            try std.testing.expectEqual(hot_standby_write_gate_mod.Action.reject_fenced_primary, decision.action);
        },
        else => return error.TestExpectedEqual,
    }
}

test "storage.hot_standby db standby role suppresses mutating background runtimes" {
    const alloc = std.testing.allocator;

    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    defer cleanupTempDir(db_path);
    var standby_log_path_tmp = try TestDirectory.init("db");
    defer standby_log_path_tmp.cleanup();
    const standby_log_path = standby_log_path_tmp.path().ptr;
    defer cleanupTempDir(standby_log_path);
    var standby_progress_path_tmp = try TestDirectory.init("db");
    defer standby_progress_path_tmp.cleanup();
    const standby_progress_path = standby_progress_path_tmp.path().ptr;
    defer cleanupTempDir(standby_progress_path);

    var standby = try hot_standby_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, .{
        .cluster_id = 301,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer standby.close();

    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .replication_write_gate = .{ .borrowed = hot_standby_write_gate_adapter.bindStandby(&standby) },
        .start_index_workers = true,
        .start_optional_runtimes = true,
        .enrichment = .{ .enable_without_producers = true },
        .ttl_cleanup = .{ .enabled = true },
        .transaction_recovery = .{ .enabled = true },
        .text_merge = .{ .enabled = true },
        .sparse_compaction = .{ .enabled = true },
    });
    defer db.close();

    try std.testing.expect(!db.start_index_workers);
    try std.testing.expect(!db.executor.hasWorkers());
    try std.testing.expect(db.enrichment_runtime == null);
    try std.testing.expect(db.resolution_runtime == null);
    try std.testing.expect(db.promotion_runtime == null);
    try std.testing.expect(db.ttl_runtime == null);
    try std.testing.expect(db.transaction_runtime == null);
    try std.testing.expect(db.text_merge_runtime == null);
    try std.testing.expect(db.sparse_compaction_runtime == null);
}

test "db graph ttl HA carries primary effects and duplicate receipt across reopen" {
    const alloc = std.testing.allocator;
    var primary_tmp = try TestDirectory.init("db-graph-review-ha-primary");
    defer primary_tmp.cleanup();
    var replica_tmp = try TestDirectory.init("db-graph-review-ha-replica");
    defer replica_tmp.cleanup();
    var log_tmp = try TestDirectory.init("db-graph-review-ha-log");
    defer log_tmp.cleanup();
    var slots_tmp = try TestDirectory.init("db-graph-review-ha-slots");
    defer slots_tmp.cleanup();
    var stream = try hot_standby_primary_mod.Primary.open(alloc, log_tmp.path().ptr, slots_tmp.path().ptr, .{
        .cluster_id = 200,
        .shard_id = 3,
        .table_id = 9,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer stream.close();
    try stream.createSlot("standby-a", 0);
    const Wait = struct {
        fail: bool = false,
        fn wait(ptr: *anyopaque, active_ctx: *anyopaque, target: u64, _: hot_standby_primary_mod.SyncPolicy) !void {
            const active: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(active_ctx));
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.InjectedTtlHAWait;
            try active.standbyStatusUpdate("standby-a", active.identity.timeline_id, target, target);
        }
    };
    var wait = Wait{};
    const names = [_][]const u8{"standby-a"};
    var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
    const primary_opts: OpenOptions = .{
        .start_optional_runtimes = false,
        .replication_async_effect_mirror = hot_standby_publisher_adapter.bindMirror(&stream, .{
            .last_lsn = &last_lsn,
            .sync_policy = .{ .mode = .remote_apply, .standby_names = &names, .failure_policy = .block },
            .sync_wait_ctx = &wait,
            .sync_wait_fn = Wait.wait,
        }),
    };
    var primary = try DB.open(alloc, primary_tmp.path(), primary_opts);
    defer primary.close();
    var replica = try DB.open(alloc, replica_tmp.path(), .{ .start_optional_runtimes = false });
    defer replica.close();
    // Bootstrap the standby with the primary index incarnation and physical
    // source state, matching HA metadata/base-backup semantics.
    const primary_key = try GraphPrimaryPublicationTest.seed(&primary, true);
    defer alloc.free(primary_key);
    const replica_key = try GraphPrimaryPublicationTest.seedWithGeneration(&replica, true, primary.core.index_manager.graphIndex("g").?.config.coverage_generation);
    defer alloc.free(replica_key);
    _ = try applyDerivedBatchToIndexAsync(primary.async_context, .{ .changed_artifact_keys = &.{primary_key} }, .{ .name = "g", .kind = .graph }, .{});
    const owner_prefix = try internal_keys.documentExactPrefixAlloc(alloc, "doc:a");
    defer alloc.free(owner_prefix);
    const bootstrap = try primary.core.store.scanPrefix(alloc, owner_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, bootstrap);
    for (bootstrap) |row| try replica.core.store.put(row.key, row.value);
    const bootstrap_due = try primary.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, bootstrap_due);
    for (bootstrap_due) |row| try replica.core.store.put(row.key, row.value);
    _ = try applyDerivedBatchToIndexAsync(replica.async_context, .{ .changed_artifact_keys = &.{replica_key} }, .{ .name = "g", .kind = .graph }, .{});
    try GraphPrimaryPublicationTest.expectCount(&primary, 1);
    try GraphPrimaryPublicationTest.expectCount(&replica, 1);
    const due = try primary.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due);
    const candidate = (try graph_edge_ttl_expiration.decodeDue(due[0].value)).source;
    var clock = platform_clock.ManualClock{};
    clock.setRealtimeNs(candidate.deadline_ns + 1);
    var ttl = TtlCleanupContext{ .batch = engine.test_support.batchContext(&primary), .grace_period_ns = 0, .clock = clock.clock() };
    wait.fail = true;
    try std.testing.expectError(error.InjectedTtlHAWait, expireGraphTtlCandidateContext(&ttl, candidate));
    const pending = try primary.core.store.scanPrefix(alloc, replication_outbox_v2_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, pending);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    const logged = stream.lastLsn();
    primary.close();
    wait.fail = false;
    primary = try DB.open(alloc, primary_tmp.path(), primary_opts);
    try engine.test_support.flushDurableReplicationOutboxes(&primary);
    try engine.test_support.flushDurableReplicationOutboxes(&primary);
    try std.testing.expectEqual(logged, stream.lastLsn());
    const recovered = try primary.core.store.scanPrefix(alloc, replication_outbox_v2_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, recovered);
    try std.testing.expectEqual(@as(usize, 0), recovered.len);
    try GraphPrimaryPublicationTest.expectCount(&primary, 0);
    var effect = (try stream.log.entryAt(alloc, last_lsn.load(.acquire))) orelse return error.TestUnexpectedResult;
    defer effect.deinit(alloc);
    _ = try replication_ingress.applyDerivedRecord(&replica, effect.record);
    try replica.runUntilIdle();
    try GraphPrimaryPublicationTest.expectCount(&replica, 0);
    const remaining_due = try replica.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, remaining_due);
    try std.testing.expectEqual(@as(usize, 0), remaining_due.len);
    const tomb_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, candidate.edge_key, "g", candidate.generation, candidate.state_key);
    defer alloc.free(tomb_key);
    const tomb = try replica.core.store.get(alloc, tomb_key);
    defer alloc.free(tomb);
    const primary_tomb = try primary.core.store.get(alloc, tomb_key);
    defer alloc.free(primary_tomb);
    try std.testing.expectEqualSlices(u8, primary_tomb, tomb);
    try replica.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:c", .edge_type = "links" }}, .sync_level = .full_index });
    try GraphPrimaryPublicationTest.expectCount(&replica, 1);
    replica.close();
    replica = try DB.open(alloc, replica_tmp.path(), .{ .start_optional_runtimes = false });
    const tip = replica.core.store.nextReplaySequence(1);
    try std.testing.expectEqual(@as(u64, 0), try replication_ingress.applyDerivedRecord(&replica, effect.record));
    try std.testing.expectEqual(tip, replica.core.store.nextReplaySequence(1));
    try GraphPrimaryPublicationTest.expectCount(&replica, 1);
}

test "db graph ttl HA replicates direct expiration and document relational withdrawal" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |relational| for ([_]bool{ false, true }) |owner_expiration| {
        var primary_tmp = try TestDirectory.init("db-ttl-ha-direct-primary");
        defer primary_tmp.cleanup();
        var replica_tmp = try TestDirectory.init("db-ttl-ha-direct-replica");
        defer replica_tmp.cleanup();
        var log_tmp = try TestDirectory.init("db-ttl-ha-direct-log");
        defer log_tmp.cleanup();
        var slots_tmp = try TestDirectory.init("db-ttl-ha-direct-slots");
        defer slots_tmp.cleanup();
        var stream = try hot_standby_primary_mod.Primary.open(alloc, log_tmp.path().ptr, slots_tmp.path().ptr, .{ .cluster_id = 200, .shard_id = 3, .table_id = 9, .timeline_id = 1, .epoch = 1 }, .{});
        defer stream.close();
        var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
        var primary = try DB.open(alloc, primary_tmp.path(), .{ .start_optional_runtimes = false, .replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&stream, .{}), .replication_async_effect_mirror = hot_standby_publisher_adapter.bindMirror(&stream, .{ .last_lsn = &last_lsn }) });
        defer primary.close();
        var replica = try DB.open(alloc, replica_tmp.path(), .{ .start_optional_runtimes = false });
        defer replica.close();
        for ([_]*DB{ &primary, &replica }) |database| {
            try database.setSchema(.{
                .version = 1,
                .storage_mode = if (relational) .relational else .document,
                .relational_columns = if (relational) &.{.{ .name = "title", .path = "title", .column_type = .string }} else &.{},
            });
            try database.addIndex(.{ .name = "direct", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
            try database.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"keep\"}" }}, .graph_writes = &.{.{ .index_name = "direct", .source = "doc:a", .target = "doc:b", .edge_type = "links" }}, .sync_level = .full_index });
        }
        const prefix = try internal_keys.documentExactPrefixAlloc(alloc, "doc:a");
        defer alloc.free(prefix);
        const bootstrap = try primary.core.store.scanPrefix(alloc, prefix);
        defer docstore_mod.DocStore.freeResults(alloc, bootstrap);
        for (bootstrap) |row| try replica.core.store.put(row.key, row.value);
        const replica_due = try replica.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, replica_due);
        for (replica_due) |row| try replica.core.store.delete(row.key);
        const due = try primary.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, due);
        for (due) |row| try replica.core.store.put(row.key, row.value);
        const candidate = (try graph_edge_ttl_expiration.decodeDue(due[0].value)).direct;
        var clock = platform_clock.ManualClock{};
        clock.setRealtimeNs(candidate.deadline_ns + 1);
        var ttl = TtlCleanupContext{ .batch = engine.test_support.batchContext(&primary), .grace_period_ns = 0, .clock = clock.clock() };
        var effect_lsn: u64 = 0;
        if (owner_expiration) {
            try std.testing.expectEqual(@as(u32, 1), try executeDeleteBatchContext(&ttl.batch, &.{"doc:a"}, .full_index, null));
        } else if (!relational) {
            // Fail in the HA encoder after the guarded primary commit. The
            // asynchronous mirror must retain an obligation and recover it
            // ahead of the next ordinary primary mutation.
            const before_lsn = stream.lastLsn();
            const stream_alloc = stream.alloc;
            var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
            stream.alloc = failing.allocator();
            defer stream.alloc = stream_alloc;
            try std.testing.expectError(error.OutOfMemory, expireDirectGraphTtlCandidateContext(&ttl, candidate));
            try std.testing.expectEqual(before_lsn, stream.lastLsn());
            try std.testing.expect(primary.async_context.primary_replication_append_pending.load(.acquire));
            try std.testing.expectError(error.ReplicationPublisherUnavailable, primary.addEnrichment(.{ .name = "blocked", .kind = .asset, .field = "title", .content_type = "text/plain" }));
            try std.testing.expectError(error.ReplicationPublisherUnavailable, appendDerivedBatchRecordContext(&ttl.batch, .{ .changed_artifact_keys = &.{candidate.artifact_key} }));
            stream.alloc = stream_alloc;
            try primary.batch(.{ .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"later\"}" }}, .sync_level = .full_index });
            try std.testing.expect(!primary.async_context.primary_replication_append_pending.load(.acquire));
            effect_lsn = before_lsn + 1;
            const pending = try primary.core.store.scanPrefix(alloc, replication_outbox_v2_prefix);
            defer docstore_mod.DocStore.freeResults(alloc, pending);
            try std.testing.expectEqual(@as(usize, 0), pending.len);
            var later = (try stream.log.entryAt(alloc, effect_lsn + 1)) orelse return error.TestUnexpectedResult;
            defer later.deinit(alloc);
            try std.testing.expectEqual(replication_record_mod.RecordKind.batch_mutation, later.record.kind);
        } else try std.testing.expect(try expireDirectGraphTtlCandidateContext(&ttl, candidate));
        if (effect_lsn == 0) effect_lsn = last_lsn.load(.acquire);
        var effect = (try stream.log.entryAt(alloc, effect_lsn)) orelse return error.TestUnexpectedResult;
        try std.testing.expect(replication_effects_mod.primary_effect.isPrimaryEffect(effect.record.payload));
        defer effect.deinit(alloc);
        try replication_ingress.applyRecord(&replica, effect.record);
        try replica.runUntilIdle();
        try std.testing.expectError(error.NotFound, replica.core.store.get(alloc, candidate.artifact_key));
        const deadlines = try replica.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, deadlines);
        try std.testing.expectEqual(@as(usize, 0), deadlines.len);
        const edges = try replica.getEdges(alloc, "direct", "doc:a", "links", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
        const doc = try replica.get(alloc, "doc:a");
        defer if (doc) |value| alloc.free(value);
        try std.testing.expectEqual(!owner_expiration, doc != null);
        if (owner_expiration) {
            try std.testing.expectEqual(@as(u64, 0), replica.core.identity_visibility.summary.?.live_ordinals);
            try std.testing.expectEqual(@as(u64, 0), replica.core.table_catalog.row_count);
        }
    };
}

test "db ordered artifact inventory reconciles committed receiver catalog before atomic admission" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const reference_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifact-reference", .{tmp.sub_path});
    defer alloc.free(reference_path);
    const follower_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifact-follower", .{tmp.sub_path});
    defer alloc.free(follower_path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var reference = try DB.open(alloc, reference_path, options);
    defer reference.close();
    try reference.setSchemaJson(alloc, "{}");
    try reference.addIndex(.{ .name = "expected", .kind = .full_text, .config_json = "{}" });
    var command = try reference.artifactInventoryCommand(alloc);
    defer command.catalogs.deinit(alloc);
    var follower = try DB.open(alloc, follower_path, options);
    defer follower.close();
    try follower.setSchemaJson(alloc, "{}");
    try follower.updateRange(.{ .start = "m", .end = "z" });
    try follower.batch(.{ .writes = &.{.{ .key = "n", .value = "{\"body\":\"receiver alpha\"}" }}, .sync_level = .write });
    try follower.addIndex(.{ .name = "stale", .kind = .full_text, .config_json = "{}" });
    try follower.core.addResolver(.{ .name = "stale-resolver", .table = "entities", .source_artifact = "relations", .resolution_artifact = "resolved", .key_template = "{{ _entity.label }}", .config_generation = 1 });
    for (0..129) |i| {
        const doc = try std.fmt.allocPrint(alloc, "row-{d:0>4}", .{i});
        defer alloc.free(doc);
        const artifact = try internal_keys.resolutionArtifactKeyAlloc(alloc, doc, "resolved");
        defer alloc.free(artifact);
        try follower.core.store.put(artifact, "{}");
    }
    var obsolete = obsolete: {
        var read = try follower.core.store.beginReadTxn();
        defer read.abort();
        break :obsolete .{ .catalogs = try @import("antfly_local_sources").storage_db_artifact_inventory.copyCatalogs(alloc, &read) };
    };
    defer obsolete.catalogs.deinit(alloc);
    const invalid_scope: @import("antfly_local_sources").storage_db_online_source_contract.Scope = .{
        .fence = .{ .admission_epoch = 1, .attempt = 1, .transition_id = 9, .owner_group_id = 2, .peer_group_id = 3, .role = .merge_source, .namespace = options.identity_namespace.?, .catalog_digest = @splat(99) },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    try std.testing.expectError(error.IntegrityCatalogChanged, @import("../server_db_adapter.zig").applyOrdered(&follower, .{ .artifact_catalog = command, .online_source = .{ .admit = .{ .scope = invalid_scope, .artifact_catalog = command.binding } } }, .{ .term = 1, .index = 1 }));
    try std.testing.expect(follower.hasIndex("stale"));
    {
        var read = try follower.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try @import("antfly_local_sources").storage_db_artifact_reconcile_intent.load(alloc, &read)) == null);
    }
    // An independently built donor has a distinct physical generation but the
    // same definition. The receiver repairs to its own ordered generation.
    const donor_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifact-independent-donor", .{tmp.sub_path});
    defer alloc.free(donor_path);
    var donor = try DB.open(alloc, donor_path, options);
    defer donor.close();
    try donor.setSchemaJson(alloc, "{}");
    try donor.addIndex(.{ .name = "expected", .kind = .full_text, .config_json = "{}" });
    var donor_command = try donor.artifactInventoryCommand(alloc);
    defer donor_command.catalogs.deinit(alloc);
    const donor_binding = donor_command.binding;
    try std.testing.expect(!std.mem.eql(u8, &donor_binding.digest, &command.binding.digest));
    try std.testing.expect(donor_binding.compatible(command.binding));
    const request: types.BatchRequest = .{ .artifact_catalog = command, .merge_replication = .{ .transition_id = 10, .donor_group_id = 3, .receiver_group_id = 2, .identity_namespace = options.identity_namespace.?, .copy_attempt = .{} }, .merge_checkpoint = .{ .kind = .accept, .transition_id = 10, .donor_group_id = 3, .receiver_group_id = 2, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z", .page_receiver_namespace = options.identity_namespace, .page_source = .{ .namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .pin_digest = @splat(5), .applied_index = 4, .retention = .{ .epoch = 1, .after_sequence = 0 }, .artifact_catalog = donor_binding } } };
    var retries: usize = 0;
    var saved_context: ?@import("antfly_local_sources").storage_db_artifact_reconcile_intent.Context = null;
    var restarted_cleanup = false;
    var saw_pending_repair = false;
    var standby_gate: @import("public_gate_state.zig").State = .{};
    standby_gate.role.store(@backingInt(@import("public_gate_state.zig").Role.standby), .release);
    const replication_payload = try replication_effects_mod.encodeArtifactCatalogMutationRequestAlloc(alloc, request, .{ .term = 1, .index = 1 });
    defer alloc.free(replication_payload);
    const replication_record: replication_record_mod.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = replication_payload };
    while (true) {
        follower.local_execution.replication_write_gate = .{ .shared = .{ .state = standby_gate.storageWriteState() } };
        replication_ingress.applyRecord(&follower, replication_record) catch |err| {
            follower.local_execution.replication_write_gate = null;
            if (err != error.ArtifactCatalogDrift) return err;
            retries += 1;
            try std.testing.expect(retries <= 32);
            try std.testing.expect((try follower.orderedApplyReceipt()) == null);
            try std.testing.expect((try follower.artifactInventoryStatus()).ordered == null);
            {
                var read = try follower.core.store.beginReadTxn();
                defer read.abort();
                const intent = (try @import("antfly_local_sources").storage_db_artifact_reconcile_intent.load(alloc, &read)).?;
                defer intent.deinit();
                saved_context = .{ .token = intent.value.token };
                try std.testing.expectError(error.ArtifactCatalogEpochChanged, @import("antfly_local_sources").storage_db_artifact_reconcile_intent.requireContext(alloc, &read, .{ .token = @splat(0) }));
                if (follower.hasIndex("expected")) {
                    try std.testing.expectError(error.IntegrityTopologyBusy, @import("antfly_local_sources").storage_db_artifact_reconcile_intent.permitCatalog(alloc, &read, @import("antfly_local_sources").storage_db_artifact_inventory.index_key, ""));
                    try std.testing.expectError(error.IntegrityTopologyBusy, @import("antfly_local_sources").storage_db_artifact_reconcile_intent.permitCatalog(alloc, &read, @import("antfly_local_sources").storage_db_artifact_inventory.index_key, obsolete.catalogs.indexes));
                }
            }
            try std.testing.expectError(error.IntegrityTopologyBusy, follower.setSchemaJson(alloc, "{\"version\":2}"));
            try std.testing.expectError(error.IntegrityTopologyBusy, follower.addIndex(.{ .name = "unrelated", .kind = .full_text, .config_json = "{}" }));
            var partial_archive: std.ArrayList(u8) = .empty;
            defer partial_archive.deinit(alloc);
            try std.testing.expectError(error.IntegrityTopologyBusy, portable_backup.exportPortable(alloc, follower.core.store, &partial_archive));
            try std.testing.expectError(error.IntegrityTopologyBusy, follower.snapshotNative("pending-artifacts"));
            try std.testing.expectError(error.IntegrityTopologyBusy, follower.isPortableImportTargetEmpty(alloc));
            {
                var rejected = try follower.core.store.beginWriteTxn();
                defer rejected.abort();
                try std.testing.expectError(error.IntegrityTopologyBusy, @import("antfly_local_sources").storage_db_relational_integrity_topology.stageCancel(&rejected, invalid_scope.fence));
                try std.testing.expectError(error.IntegrityTopologyBusy, @import("antfly_local_sources").storage_db_relational_integrity_topology.stageAbortTransition(&rejected, invalid_scope.fence));
            }
            const producer_base = engine.test_support.batchContext(&follower);
            var producer: EnrichmentAppendContext = .{
                .alloc = alloc,
                .store = producer_base.store,
                .applied_sequence_checkpoint_path = producer_base.applied_sequence_checkpoint_path,
                .shard_manager = producer_base.shard_manager,
                .index_manager = producer_base.index_manager,
                .apply_mutex = producer_base.apply_mutex,
                .change_journal = producer_base.change_journal,
                .replay_source = producer_base.replay_source,
                .executor = producer_base.executor,
                .async_context = follower.async_context,
                .log_mutex = producer_base.log_mutex,
            };
            const late_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "a-behind-cursor", "resolved");
            defer alloc.free(late_key);
            const late_write: resolution_runtime_mod.RecordWrite = .{ .batch = .{}, .artifact_writes = &.{.{ .key = late_key, .value = "{}" }}, .publish_resolution_handoff = true };
            try std.testing.expectError(error.IntegrityTopologyBusy, appendResolutionRecord(&producer, late_write));
            try std.testing.expectError(error.IntegrityTopologyBusy, publishResolutionHandoffContext(&producer_base, late_write));
            try std.testing.expectError(error.NotFound, follower.core.store.get(alloc, late_key));
            if (!restarted_cleanup) if (try follower.core.getStoreValue(alloc, @import("antfly_local_sources").storage_db_artifact_reconcile_intent.resolver_cursor_key)) |cursor_before| {
                defer alloc.free(cursor_before);
                follower.close();
                follower = try DB.open(alloc, follower_path, options);
                const cursor_after = try follower.core.store.get(alloc, @import("antfly_local_sources").storage_db_artifact_reconcile_intent.resolver_cursor_key);
                defer alloc.free(cursor_after);
                try std.testing.expectEqualSlices(u8, cursor_before, cursor_after);
                var resumed = try follower.artifactInventoryCommand(alloc);
                defer resumed.catalogs.deinit(alloc);
                try std.testing.expectEqualDeep(command.binding, resumed.binding);
                restarted_cleanup = true;
            };
            if (follower.hasIndex("expected") and follower.core.index_manager.hasRepairUnavailableIndexes()) {
                saw_pending_repair = true;
                var repairs = try follower.loadIndexRepairState(alloc);
                defer repairs.deinit(alloc);
                follower.local_execution.replication_write_gate = .{ .shared = .{ .state = standby_gate.storageWriteState() } };
                defer follower.local_execution.replication_write_gate = null;
                for (repairs.entries.items) |repair| if (repair.intent.phase != .terminal) {
                    _ = try follower.advanceIndexRepairIntent(alloc, repair.intent.repair_id, .{});
                };
            }
            continue;
        };
        follower.local_execution.replication_write_gate = null;
        break;
    }
    try std.testing.expect(retries >= 2);
    try std.testing.expect(restarted_cleanup);
    try std.testing.expect(saw_pending_repair);
    try std.testing.expect((try follower.artifactInventoryStatus()).ready);
    try std.testing.expect(follower.hasIndex("expected"));
    try std.testing.expect(!follower.hasIndex("stale"));
    var rebuilt_rows = try follower.search(alloc, .{ .index_name = "expected", .full_text = .{ .match = .{ .field = "body", .text = "alpha" } } });
    defer rebuilt_rows.deinit();
    try std.testing.expectEqual(@as(u32, 1), rebuilt_rows.total_hits);
    const resolution_rows = try follower.core.store.scanPrefixKeysPage(alloc, &.{internal_keys.user_namespace}, null, 256);
    defer {
        for (resolution_rows) |key| alloc.free(key);
        alloc.free(resolution_rows);
    }
    for (resolution_rows) |key| {
        if (try internal_keys.parseResolutionArtifactKeyAlloc(alloc, key)) |resolution| {
            alloc.free(resolution.doc_key);
            alloc.free(resolution.artifact_name);
            return error.TestUnexpectedResult;
        }
    }
    try std.testing.expectEqual(@as(u64, 1), (try follower.orderedApplyReceipt()).?.index);
    {
        var read = try follower.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try @import("antfly_local_sources").storage_db_artifact_reconcile_intent.load(alloc, &read)) == null);
        try std.testing.expectError(error.ArtifactCatalogEpochChanged, @import("antfly_local_sources").storage_db_artifact_reconcile_intent.requireContext(alloc, &read, saved_context.?));
        try std.testing.expectError(error.NotFound, read.get(@import("antfly_local_sources").storage_db_artifact_reconcile_intent.resolver_cursor_key));
    }
    try std.testing.expectError(error.IntegrityTopologyBusy, follower.addIndex(.{ .name = "late", .kind = .full_text, .config_json = "{}" }));
}

test "db graph ttl HA retirement preserves different replay progress" {
    const Case = struct { shared_edge: bool, ahead: bool, changed_revision: bool = false, owner_expiration: bool = false, delayed_source: bool = false };
    for ([_]Case{
        .{ .shared_edge = false, .ahead = true },
        .{ .shared_edge = true, .ahead = true },
        .{ .shared_edge = false, .ahead = false },
        .{ .shared_edge = true, .ahead = false },
        .{ .shared_edge = true, .ahead = true, .changed_revision = true },
        .{ .shared_edge = true, .ahead = false, .changed_revision = true },
        .{ .shared_edge = false, .ahead = true, .owner_expiration = true },
        .{ .shared_edge = true, .ahead = true, .owner_expiration = true },
        .{ .shared_edge = false, .ahead = false, .owner_expiration = true },
        .{ .shared_edge = false, .ahead = true, .delayed_source = true },
    }) |case| {
        const alloc = std.testing.allocator;
        var primary_tmp = try TestDirectory.init("review-ttl-primary");
        defer primary_tmp.cleanup();
        var replica_tmp = try TestDirectory.init("review-ttl-replica");
        defer replica_tmp.cleanup();
        var log_tmp = try TestDirectory.init("review-ttl-log");
        defer log_tmp.cleanup();
        var slots_tmp = try TestDirectory.init("review-ttl-slots");
        defer slots_tmp.cleanup();
        var stream = try hot_standby_primary_mod.Primary.open(alloc, log_tmp.path().ptr, slots_tmp.path().ptr, .{ .cluster_id = 200, .shard_id = 3, .table_id = 9, .timeline_id = 1, .epoch = 1 }, .{});
        defer stream.close();
        var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
        var primary = try DB.open(alloc, primary_tmp.path(), .{ .start_index_workers = false, .start_optional_runtimes = false, .replication_async_effect_mirror = hot_standby_publisher_adapter.bindMirror(&stream, .{ .last_lsn = &last_lsn }) });
        defer primary.close();
        var replica = try DB.open(alloc, replica_tmp.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
        defer replica.close();
        for ([_]*DB{ &primary, &replica }) |db| {
            try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
            try db.addEnrichment(.{ .name = "relations_v2", .kind = .asset, .field = "relations", .content_type = "application/json" });
            try db.addIndex(.{ .name = "g", .kind = .graph, .coverage_generation = 7, .config_json = "{\"ttl\":{\"duration\":\"1h\"},\"max_edges_per_document\":2,\"sources\":[{\"artifact\":\"relations_v1\"},{\"artifact\":\"relations_v2\"}]}" });
            try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .full_index });
        }
        const a = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
        defer alloc.free(a);
        const b = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", if (case.changed_revision) "relations_v1" else "relations_v2");
        defer alloc.free(b);
        for ([_]*DB{ &primary, &replica }) |db| try db.core.store.put(a, "{\"type\":\"links\",\"target\":{\"document_id\":\"doc:b\"}}");
        _ = try applyDerivedBatchToIndexAsync(primary.async_context, .{ .changed_artifact_keys = &.{a} }, .{ .name = "g", .kind = .graph }, .{});
        const prefix = try internal_keys.documentExactPrefixAlloc(alloc, "doc:a");
        defer alloc.free(prefix);
        const due = try primary.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, due);
        // Materialize independently: matching revisions can have different
        // server-assigned lifetimes and therefore different local due keys.
        if (!case.delayed_source) {
            _ = try applyDerivedBatchToIndexAsync(replica.async_context, .{ .changed_artifact_keys = &.{a} }, .{ .name = "g", .kind = .graph }, .{});
            const local_due = try replica.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
            defer docstore_mod.DocStore.freeResults(alloc, local_due);
            try std.testing.expectEqual(@as(usize, 1), local_due.len);
            try std.testing.expect(!std.mem.eql(u8, due[0].key, local_due[0].key));
        }
        // Model an existing overdue source on both nodes before the update.
        if (case.changed_revision) {
            const old = (try graph_edge_ttl_expiration.decodeDue(due[0].value)).source;
            for ([_]*DB{ &primary, &replica }) |db| {
                const global_key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, "g", 7, old.edge_key, old.source_priority, old.state_key);
                defer alloc.free(global_key);
                const global_raw = try db.core.store.get(alloc, global_key);
                defer alloc.free(global_raw);
                const view = (try graph_edge_contender.decode(global_raw, 7)).?;
                var decoded_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, view.payload);
                defer decoded_edge.deinit(alloc);
                const expired_payload = try enrichment_artifact_codec.encodeGraphEdgeWithTtlAlloc(alloc, null, 7, decoded_edge.weight, decoded_edge.created_at, decoded_edge.updated_at, 1, decoded_edge.metadata_json);
                defer alloc.free(expired_payload);
                const contender = try graph_edge_contender.encodeAlloc(alloc, 7, old.source_priority, old.edge_key, old.state_key, expired_payload);
                defer alloc.free(contender);
                const local_key = try internal_keys.graphEdgeContenderKeyAlloc(alloc, "doc:a", "g", old.edge_key, old.state_key);
                defer alloc.free(local_key);
                const life_key = try internal_keys.graphEdgeTtlLifetimeKeyAlloc(alloc, old.edge_key, "g", 7, old.state_key);
                defer alloc.free(life_key);
                var life: [8]u8 = undefined;
                std.mem.writeInt(u64, &life, 1, .big);
                var overdue = old;
                overdue.deadline_ns = 1 + std.time.ns_per_hour;
                GraphTtlSha256.hash(contender, &overdue.contender_digest, .{});
                const overdue_key = try graph_edge_ttl_expiration.indexKeyAlloc(alloc, overdue.deadline_ns, global_key);
                defer alloc.free(overdue_key);
                const overdue_value = try graph_edge_ttl_expiration.encodeAlloc(alloc, overdue);
                defer alloc.free(overdue_value);
                const prior_due = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
                defer docstore_mod.DocStore.freeResults(alloc, prior_due);
                try db.core.store.putBatch(&.{ .{ .key = global_key, .value = contender }, .{ .key = local_key, .value = contender }, .{ .key = old.edge_key, .value = expired_payload }, .{ .key = life_key, .value = &life }, .{ .key = overdue_key, .value = overdue_value } }, &.{prior_due[0].key});
            }
        }
        // This source write precedes expiration in the primary/HA order. Only the
        // standby has completed source replay when the primary expires source A.
        const next_source = if (case.changed_revision) "{\"type\":\"links\",\"weight\":17,\"target\":{\"document_id\":\"doc:b\"}}" else if (case.shared_edge) "{\"type\":\"links\",\"target\":{\"document_id\":\"doc:b\"}}" else "{\"type\":\"links\",\"target\":{\"document_id\":\"doc:c\"}}";
        for ([_]*DB{ &primary, &replica }) |db| try db.core.store.put(b, next_source);
        _ = try appendDerivedBatchRecord(&primary, .{ .changed_artifact_keys = &.{b} });
        var source_record = (try stream.log.entryAt(alloc, last_lsn.load(.acquire))) orelse return error.TestUnexpectedResult;
        defer source_record.deinit(alloc);
        _ = try replication_ingress.applyDerivedRecord(&replica, source_record.record);
        if (case.ahead) try replica.runUntilIdle();
        try GraphPrimaryPublicationTest.expectCount(&primary, 1);
        const initial_count_key = try internal_keys.graphEdgeContenderCountKeyAlloc(alloc, "doc:a", "g");
        defer alloc.free(initial_count_key);
        const initial_count = try replica.core.store.get(alloc, initial_count_key);
        defer alloc.free(initial_count);
        try std.testing.expectEqual(@as(usize, if (case.ahead and !case.shared_edge and !case.delayed_source) 2 else 1), (try graph_edge_contender.decodeVisibleCount(initial_count, 7)).?);
        const current_due = try primary.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, current_due);
        const candidate = (try graph_edge_ttl_expiration.decodeDue(current_due[0].value)).source;
        var clock = platform_clock.ManualClock{};
        clock.setRealtimeNs(candidate.deadline_ns + 1);
        var ttl = TtlCleanupContext{ .batch = engine.test_support.batchContext(&primary), .grace_period_ns = 0, .clock = clock.clock() };
        if (case.owner_expiration) {
            try std.testing.expectEqual(@as(u32, 1), try executeDeleteBatchContext(&ttl.batch, &.{"doc:a"}, .full_index, null));
        } else {
            // Earlier source replay must finish before retirement is certified.
            const before_lsn = last_lsn.load(.acquire);
            try std.testing.expect(!try expireGraphTtlCandidateContext(&ttl, candidate));
            try std.testing.expectEqual(before_lsn, last_lsn.load(.acquire));
            try primary.runUntilIdle();
            if (case.changed_revision) {
                try std.testing.expect(!try expireGraphTtlCandidateContext(&ttl, candidate));
                const refreshed = try primary.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
                defer docstore_mod.DocStore.freeResults(alloc, refreshed);
                try std.testing.expectEqual(@as(usize, 1), refreshed.len);
                const current = (try graph_edge_ttl_expiration.decodeDue(refreshed[0].value)).source;
                try std.testing.expectEqual(candidate.deadline_ns, current.deadline_ns);
                try std.testing.expect(try expireGraphTtlCandidateContext(&ttl, current));
            } else {
                try std.testing.expect(try expireGraphTtlCandidateContext(&ttl, candidate));
            }
        }
        var expiration_record = (try stream.log.entryAt(alloc, last_lsn.load(.acquire))) orelse return error.TestUnexpectedResult;
        defer expiration_record.deinit(alloc);
        _ = try replication_ingress.applyDerivedRecord(&replica, expiration_record.record);
        if (case.changed_revision) {
            const retired_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, candidate.edge_key, candidate.index_name, candidate.generation, candidate.state_key);
            defer alloc.free(retired_key);
            const retired = try replica.core.store.get(alloc, retired_key);
            defer alloc.free(retired);
            try std.testing.expectEqual(candidate.deadline_ns, (try graph_edge_ttl_tombstone.Tombstone.decode(retired)).deadline_ns);
        }
        try replica.runUntilIdle();
        if (case.delayed_source) _ = try applyDerivedBatchToIndexAsync(replica.async_context, .{ .changed_artifact_keys = &.{a} }, .{ .name = "g", .kind = .graph }, .{});
        if (!case.owner_expiration and !case.changed_revision) {
            _ = try applyDerivedBatchToIndexAsync(primary.async_context, .{ .changed_artifact_keys = &.{b} }, .{ .name = "g", .kind = .graph }, .{});
            try GraphPrimaryPublicationTest.expectCount(&primary, 1);
        }
        const global_prefix = try internal_keys.graphGlobalEdgeContenderRootPrefixAlloc(alloc, "doc:a");
        defer alloc.free(global_prefix);
        const survivors = try replica.core.store.scanPrefix(alloc, global_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, survivors);
        const expected: usize = if (case.owner_expiration or case.changed_revision) 0 else 1;
        try std.testing.expectEqual(expected, survivors.len);
        const surviving_due = try replica.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, surviving_due);
        try std.testing.expectEqual(expected, surviving_due.len);
        if (!case.owner_expiration and !case.changed_revision) try GraphPrimaryPublicationTest.expectCount(&replica, 1);
        const edges = try replica.getEdges(alloc, "g", "doc:a", "links", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(expected, edges.len);
        if (!case.owner_expiration and !case.changed_revision) try std.testing.expectEqualStrings(if (case.shared_edge) "doc:b" else "doc:c", edges[0].target);

        if (case.owner_expiration) {
            try std.testing.expectError(error.NotFound, replica.core.store.get(alloc, "doc:a"));
            const remaining_due = try replica.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
            defer docstore_mod.DocStore.freeResults(alloc, remaining_due);
            try std.testing.expectEqual(@as(usize, 0), remaining_due.len);
            const remaining_owner = try replica.core.store.scanPrefix(alloc, prefix);
            defer docstore_mod.DocStore.freeResults(alloc, remaining_owner);
            try std.testing.expectEqual(@as(usize, 0), remaining_owner.len);
        }
        const tip = replica.core.store.nextReplaySequence(1);
        try std.testing.expectEqual(@as(u64, 0), try replication_ingress.applyDerivedRecord(&replica, expiration_record.record));
        try std.testing.expectEqual(tip, replica.core.store.nextReplaySequence(1));
        replica.close();
        replica = try DB.open(alloc, replica_tmp.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
        if (!case.owner_expiration and !case.changed_revision) try GraphPrimaryPublicationTest.expectCount(&replica, 1);
        const reopened_edges = try replica.getEdges(alloc, "g", "doc:a", "links", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, reopened_edges);
        try std.testing.expectEqual(expected, reopened_edges.len);
        if (case.changed_revision) {
            const after_retirement = "{\"type\":\"links\",\"weight\":23,\"target\":{\"document_id\":\"doc:b\"}}";
            for ([_]*DB{ &primary, &replica }) |db| try db.core.store.put(a, after_retirement);
            _ = try appendDerivedBatchRecord(&primary, .{ .changed_artifact_keys = &.{a} });
            var new_record = (try stream.log.entryAt(alloc, last_lsn.load(.acquire))) orelse return error.TestUnexpectedResult;
            defer new_record.deinit(alloc);
            _ = try replication_ingress.applyDerivedRecord(&replica, new_record.record);
            for ([_]*DB{ &primary, &replica }) |db| {
                try db.runUntilIdle();
                const renewed = try db.getEdges(alloc, "g", "doc:a", "links", .out);
                defer graph_mod.GraphIndex.freeEdges(alloc, renewed);
                try std.testing.expectEqual(@as(usize, 1), renewed.len);
                try std.testing.expectEqual(@as(f64, 23), renewed[0].weight);
                const new_due = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
                defer docstore_mod.DocStore.freeResults(alloc, new_due);
                try std.testing.expectEqual(@as(usize, 1), new_due.len);
                try std.testing.expect((try graph_edge_ttl_expiration.decodeDue(new_due[0].value)).source.deadline_ns > candidate.deadline_ns);
            }
            // A duplicate retirement cannot overwrite this newer lifecycle.
            try std.testing.expectEqual(@as(u64, 0), try replication_ingress.applyDerivedRecord(&replica, expiration_record.record));
            const renewed = try replica.getEdges(alloc, "g", "doc:a", "links", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, renewed);
            try std.testing.expectEqual(@as(usize, 1), renewed.len);
        }
    }
}
