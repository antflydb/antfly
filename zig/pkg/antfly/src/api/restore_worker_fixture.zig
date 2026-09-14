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

//! Production worker integration fixture: real metadata Raft/apply store,
//! immutable native seals, scoped owners, and distributed integrity activation.
const std = @import("std");
const http = @import("http_server.zig");
const metadata_service = @import("../metadata/service.zig");
const metadata = @import("../metadata/api.zig");
const tables = @import("../metadata/table_manager.zig");
const stages = @import("../metadata/restore_staging.zig");
const cohort = @import("../metadata/backup_cohort.zig");
const db = @import("../storage/db/mod.zig");
const native = @import("../storage/db/restore_staging.zig");
const owner_api = @import("restore_owner.zig");
const catalog_mod = @import("restore_catalog.zig");
const reads = @import("table_reads.zig");
const writes = @import("table_writes.zig");
const distributed = @import("distributed_txn.zig");
const contract = @import("distributed_txn_contract.zig");
const operation = @import("operation.zig");
const backups = @import("backups.zig");
const restore_jobs = @import("restore_jobs.zig");
const driver = @import("restore_staging_driver.zig");
const raft_host = @import("../raft/host.zig");
const raft_engine = @import("raft_engine");
const read_gate = @import("../raft/read_gate.zig");

const Fixture = struct {
    alloc: std.mem.Allocator,
    raft: *raft_engine.core.MemoryStorage,
    runtime: *db.background_runtime.BackendRuntime,
    source: http.StatusSource = undefined,
    non_raft: bool = false,
    owner_count: usize = 3,
    node_config: ?*const @import("../common/config.zig").Config = null,
    dbs: [3]*db.DB = undefined,
    scopes: [3]native.Scope = undefined,
    cache_paths: [3][]const u8 = undefined,
    indices: [3]u64 = @splat(0),
    sequence: u8 = 0,
    private_catalog: ?*catalog_mod.Catalog = null,
    imports: usize = 0,
    validations: usize = 0,
    publications: usize = 0,
    target_paths: [3][]const u8 = undefined,
    target_open: [3]bool = @splat(false),
    restart_after_commit: bool = false,
    faults_seen: [3]u8 = @splat(0),

    fn descriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &.{record.local_node_id});
        errdefer self.alloc.free(peers);
        return .{ .group = .{ .group_id = record.group_id, .local_node_id = record.local_node_id, .raft_config = .{ .id = record.local_node_id, .group_id = record.group_id, .peers = peers, .election_tick = 5, .heartbeat_tick = 1, .pre_vote = false, .check_quorum = true }, .storage = self.raft.storage() }, .bootstrap = try raft_host.catalog.runtimeBootstrapFromRecord(self.alloc, record) };
    }
    fn freeDescriptor(_: *anyopaque, alloc: std.mem.Allocator, value: *raft_engine.runtime.ReplicaDescriptor) void {
        raft_host.catalog.freeRuntimeBootstrap(alloc, &value.bootstrap);
        alloc.free(value.group.raft_config.peers);
    }
    fn local(_: *anyopaque) u64 {
        return 1;
    }
    fn leader(_: *anyopaque, _: u64) ?u64 {
        return 1;
    }
    fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
        return .active;
    }
    fn uri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
        return null;
    }
    fn index(name: []const u8) !usize {
        for ([_][]const u8{ "parent", "child", "docs" }, 0..) |value, i| if (std.mem.eql(u8, value, name)) return i;
        return error.TableNotFound;
    }
    const Apply = struct {
        fixture: *Fixture,
        index: usize,
        fn propose(ptr: *anyopaque, request: db.types.BatchRequest, context: operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            self.fixture.indices[self.index] += 1;
            var mutation = request;
            mutation.restore_staging_scope = self.fixture.scopes[self.index].digest();
            if (self.fixture.non_raft) try self.fixture.dbs[self.index].batchWithVisibilityCancellation(mutation, context.cancellation) else try self.fixture.dbs[self.index].batchRaftReplicatedApply(mutation, .{ .index = self.fixture.indices[self.index], .term = 1 });
        }
    };
    fn owner(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, group: u64, request: owner_api.Request, context: operation.RequestContext) !owner_api.Response {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const i = try index(name);
        try std.testing.expectEqual(self.scopes[i].target_namespace.shard_id, group);
        const source = self.source;
        const progress = (try source.getRestoreStagingProgress(alloc, request.scope.plan_id, context)).?;
        if (request.action == .publish) try std.testing.expectEqual(stages.State.published, progress.state);
        if (request.action == .validate) {
            try std.testing.expectEqual(stages.State.validating, progress.state);
            for (self.dbs[0..self.owner_count]) |target| {
                var state = (try target.restoreStagingStatus(alloc)).?;
                defer state.deinit();
                try std.testing.expect(state.value.phase == .imported or state.value.phase == .validated);
            }
            self.validations += 1;
        }
        var apply: Apply = .{ .fixture = self, .index = i };
        const result = try owner_api.executeResident(alloc, self.dbs[i], .{ .io = std.testing.io, .runtime = self.runtime, .location_options = .{ .filesystem_io = std.testing.io, .node_config = self.node_config }, .cache_path = self.cache_paths[i], .proposer = .{ .ptr = &apply, .propose = Apply.propose } }, request, context);
        if (request.action == .import_page) self.imports += 1;
        if (request.action == .publish) self.publications += 1;
        const fault_bit: u8 = switch (request.action) {
            .begin => 1,
            .import_page => if (result.rows != 0 or result.phase == .imported) 2 else 0,
            .validate => if (result.phase == .validated) 4 else 0,
            .publish => 8,
            .cancel => 16,
            .status => 0,
        };
        if (self.restart_after_commit and fault_bit != 0 and self.faults_seen[i] & fault_bit == 0) {
            self.faults_seen[i] |= fault_bit;
            // Lose the acknowledgement only after durable native apply. The
            // coordinator must rediscover the same scope/phase, not invent a
            // new owner or infer publication from its missing response.
            self.dbs[i].close();
            self.target_open[i] = false;
            self.dbs[i].* = try db.DB.open(alloc, self.target_paths[i], .{ .backend_runtime = self.runtime, .identity_namespace = self.scopes[i].target_namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
            self.target_open[i] = true;
            return error.ConnectionResetByPeer;
        }
        return result;
    }
    fn lookup(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, key: []const u8, opts: db.types.LookupOptions, consistency: read_gate.ReadConsistency) !?reads.LookupResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(read_gate.ReadConsistency.read_index, consistency);
        const i = try index(name);
        var scoped = opts;
        scoped.restore_staging_scope = try self.private_catalog.?.source().restoreScopeForGroup(name, self.scopes[i].target_namespace.shard_id);
        const row = (try self.dbs[i].lookup(alloc, key, scoped)) orelse return null;
        return .{ .json = row.json, .version = try self.dbs[i].getTimestamp(alloc, key) };
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db.types.ScanOptions, _: read_gate.ReadConsistency) !?reads.ScanResponse {
        return error.UnexpectedCall;
    }
    fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db.types.SearchRequest, _: read_gate.ReadConsistency) !?@import("query_response.zig").QueryResponse {
        return error.UnexpectedCall;
    }
    fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db.types.BatchRequest) !?void {
        return error.UnexpectedCall;
    }
    fn begin(ptr: *anyopaque, alloc: std.mem.Allocator, group: u64, name: []const u8, request: distributed.TxnBeginRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const participant = try distributed.participantIdForGroup(alloc, name, group);
        defer alloc.free(participant);
        _ = try self.dbs[try index(name)].beginTransactionScoped(request.txn_id, request.begin_timestamp, request.begin_timestamp, request.participants, std.mem.eql(u8, participant, request.participants[0]), false, request.restore_staging_scope);
    }
    fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, name: []const u8, request: distributed.TxnPrepareRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.dbs[try index(name)].writeTransaction(request.txn_id, request.req);
    }
    fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, name: []const u8, request: distributed.TxnResolveRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.dbs[try index(name)].resolveTransactionIntents(request.txn_id, request.status, request.commit_version);
    }
    fn status(ptr: *anyopaque, _: std.mem.Allocator, _: u64, name: []const u8, id: db.types.TxnId) !db.types.TxnStatus {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.dbs[try index(name)].getTransactionStatus(id);
    }
    fn commit(ptr: *anyopaque, alloc: std.mem.Allocator, requests: []const contract.TableCommitRequest, sync: db.types.SyncLevel, cancel: db.types.CancellationToken) !?contract.CommitOutcome {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try cancel.check();
        self.sequence += 1;
        return try distributed.executeMultiTableCommit(alloc, self.private_catalog.?.source(), .{ .ptr = self, .vtable = &.{ .begin_group = begin, .prepare_group = prepare, .resolve_group = resolve, .status_group = status } }, @splat(self.sequence), @as(u64, self.sequence) * 1000, @as(u64, self.sequence) * 1000 + 1, requests, sync, null);
    }
    fn bind(ptr: *anyopaque, catalog: *catalog_mod.Catalog) !catalog_mod.ValidationPort.SourcePair {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.private_catalog = catalog;
        return .{ .reader = .{ .ptr = self, .vtable = &.{ .lookup = lookup, .scan = scan, .query = query } }, .writer = .{ .ptr = self, .vtable = &.{ .batch = batch, .commit_batch_with_cancellation = commit } } };
    }
};

pub fn run(comptime Driver: type, invalid_child: bool) !void {
    return runWithSource(Driver, invalid_child, null);
}

pub fn runWithSource(comptime Driver: type, invalid_child: bool, override: ?http.StatusSource) !void {
    return runWithPersistence(Driver, invalid_child, override, null);
}

pub fn runWithPersistence(comptime Driver: type, invalid_child: bool, override: ?http.StatusSource, persistence: ?restore_jobs.ReplicatedPersistence) !void {
    return runWithPolicy(Driver, invalid_child, override, persistence, .{});
}

pub const Policy = struct { failover_safe: bool = false, guard: ?http.RestoreExecutionGuard = null, gate: ?db.HAWriteGate = null, mirror: ?db.HAAsyncEffectMirror = null, term: u64 = 1, portable: bool = false, restart_after_commit: bool = false, table_restore: bool = false, migration: bool = false, benchmark_rows: usize = 1 };
pub fn runWithPolicy(comptime Driver: type, invalid_child: bool, override: ?http.StatusSource, persistence: ?restore_jobs.ReplicatedPersistence, policy: Policy) !void {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    var runtime = try db.background_runtime.BackendRuntime.init(alloc, .{ .backend = .manual, .filesystem_io = std.testing.io });
    defer runtime.deinit();
    var api_runtime = try db.background_runtime.BackendRuntime.init(alloc, .{ .backend = .manual, .filesystem_io = std.testing.io, .borrowed_io = .{ .general = std.testing.io, .api = std.testing.io } });
    defer api_runtime.deinit();
    var raft = raft_engine.core.MemoryStorage.init(alloc);
    defer raft.deinit();
    var svc: metadata_service.MetadataService = undefined;
    var fixture: Fixture = .{ .alloc = alloc, .raft = &raft, .runtime = &runtime, .non_raft = override != null, .restart_after_commit = policy.restart_after_commit, .owner_count = if (policy.table_restore) 1 else 3 };
    var opened: usize = 0;
    defer for (fixture.dbs[0..opened], 0..) |target, i| {
        if (fixture.target_open[i]) target.close();
        alloc.destroy(target);
    };
    svc = try metadata_service.MetadataService.init(alloc, .{ .host = .{ .local_node_id = 1, .metadata_group_id = 1988, .replica_root_dir = try std.fmt.allocPrint(a, "{s}/metadata", .{root}), .replica_catalog_path = try std.fmt.allocPrint(a, "{s}/metadata-catalog", .{root}) } }, .{ .host = .{ .host = .{ .descriptor_factory = .{ .ptr = &fixture, .vtable = &.{ .build_descriptor = Fixture.descriptor, .free_descriptor = Fixture.freeDescriptor } } } } }, .{});
    defer svc.deinit();
    _ = try svc.ensureMetadataReplica(.{ .group_id = 1988, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .empty });
    try svc.campaignMetadataGroup();
    try svc.runRound();
    const source = override orelse http.StatusSource.fromMetadataService(&svc);
    fixture.source = source;
    var node_config = try Driver.nodeConfig(alloc);
    defer node_config.deinit();
    fixture.node_config = &node_config;
    var server = http.ApiHttpServer.init(alloc, .{ .backend_runtime = &api_runtime, .node_config = &node_config, .restore_owner = .{ .ptr = &fixture, .execute_fn = Fixture.owner }, .restore_validation = .{ .status = source, .factory = .{ .ptr = &fixture, .bind = Fixture.bind } }, .session_router = .{ .ptr = &fixture, .vtable = &.{ .local_node_id = Fixture.local, .local_status = Fixture.localStatus, .group_leader_node_id = Fixture.leader, .node_base_uri = Fixture.uri } } }, source, null, null);
    defer server.deinit();
    server.cfg.ha_failover_safe_mutations_only = policy.failover_safe;
    server.cfg.restore_execution_guard = policy.guard;
    server.restore_leadership_term.store(policy.term, .release);
    server.restore_job_store.deinit();
    server.restore_job_store = restore_jobs.Store.initWithIo(alloc, std.testing.io);
    if (persistence) |store| {
        try server.restore_job_store.attachReplicated(store);
        try server.restore_job_store.prepareReplicatedLeadership(alloc, policy.term);
    } else {
        const job_store = try alloc.create(restore_jobs.OpenedStore);
        job_store.* = try restore_jobs.OpenedStore.open(alloc, try std.fmt.allocPrint(a, "{s}/restore-jobs", .{root}));
        try server.restore_job_store.attach(job_store);
    }
    const location = try std.fmt.allocPrint(a, "file://{s}", .{root});
    const admitted = try server.restore_job_store.start(a, .{ .scope = if (policy.table_restore) .table else .cluster, .source_kind = .cluster_cohort, .table_name = if (policy.table_restore) "parent" else null, .backup_id = "daily", .location = location, .connection = "test-backups", .idempotency_namespace = "restore-worker-mixed", .table_names = if (policy.table_restore) null else &.{ "parent", "child", "docs" } });
    const queued = try std.json.parseFromSlice(restore_jobs.JobState, a, admitted, .{});
    const running = (try server.restore_job_store.begin(a, queued.value.job_id)).?;
    const worker = try std.json.parseFromSlice(restore_jobs.JobState, a, running, .{});
    _ = try server.restore_job_store.ensureStagingAttempt(a, worker.value.job_id, worker.value.attempt_id);
    const plan_id = try stages.idForAttempt(worker.value.job_id, worker.value.attempt_id);
    const plain_schema = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
    const parent_schema = "{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"unique_constraints\":[{\"name\":\"pk\",\"columns\":[\"id\"]}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
    const child_schema = "{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"foreign_keys\":[{\"name\":\"parent_fk\",\"child_columns\":[\"id\"],\"parent_table\":\"parent\",\"parent_columns\":[\"id\"]}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
    const previous_doc_schema = "{\"version\":0,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\",\"x-antfly-types\":[\"text\"]}}}}}}";
    const active_doc_schema = "{\"version\":1,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\",\"x-antfly-types\":[\"keyword\"]}}}}}}";
    var manifests: [3]backups.TableBackupManifest = undefined;
    var owners: [3]cohort.Owner = undefined;
    var proofs: [3]cohort.TableProof = undefined;
    var seals: [3]cohort.SealReceipt = undefined;
    for ([_][]const u8{ "parent", "child", "docs" }, 0..) |name, i| {
        const namespace: @import("../storage/db/doc_identity.zig").Namespace = .{ .table_id = 10 + i, .shard_id = 20 + i, .range_id = 20 + i };
        const path = try std.fmt.allocPrint(a, "{s}/source-{d}", .{ root, i });
        var original = try db.DB.open(alloc, path, .{ .backend_runtime = &runtime, .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
        defer original.close();
        try original.setSchemaJson(alloc, if (i == 2) (if (policy.migration) previous_doc_schema else "{}") else plain_schema);
        const input = try a.alloc(db.types.BatchWrite, policy.benchmark_rows);
        for (input, 0..) |*row, ordinal| row.* = .{ .key = if (ordinal == 0) "row" else try std.fmt.allocPrint(a, "row-{d:0>8}", .{ordinal}), .value = if (policy.migration and i == 2) "{\"id\":1,\"name\":\"Old Mapping\"}" else try std.fmt.allocPrint(a, "{{\"id\":{d}}}", .{ordinal + (if (invalid_child and i == 1) @as(usize, 2) else 1)}) };
        try original.batch(.{ .timestamp_ns = 123, .writes = input });
        const schema = switch (i) {
            0 => parent_schema,
            1 => child_schema,
            else => if (policy.migration) active_doc_schema else "{}",
        };
        try original.setSchemaJson(alloc, schema);
        const identity = try original.relationalTopologyIdentity();
        const fence: @import("../storage/db/relational_integrity_topology.zig").Fence = .{ .transition_id = 700, .attempt = 1, .owner_group_id = namespace.shard_id, .peer_group_id = namespace.shard_id, .admission_epoch = identity.next_epoch, .role = .backup_snapshot, .namespace = namespace, .catalog_digest = identity.catalog_digest };
        try original.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null);
        const seal = try original.sealBackupCohort("cohort", fence, .none);
        try original.applyRelationalTopologyControl(.{ .fence = fence, .action = .release }, null);
        const format: backups.BackupFormat = if (policy.portable) .portable else .native;
        const relative = if (policy.portable) try std.fmt.allocPrint(a, "source-{d}.afb", .{i}) else try std.fmt.allocPrint(a, "source-{d}.snapshots/snapshot", .{i});
        const artifact_path = try std.fmt.allocPrint(a, "{s}/{s}", .{ root, relative });
        if (policy.portable) {
            var file = try std.Io.Dir.cwd().createFile(std.testing.io, artifact_path, .{});
            defer file.close(std.testing.io);
            var buffer: [65536]u8 = undefined;
            var writer = file.writer(std.testing.io, &buffer);
            try original.exportBackupCohortPortable(seal, &writer.interface, .{}, .none);
            try writer.end();
            try file.sync(std.testing.io);
        } else _ = try original.exportBackupCohort(seal, "snapshot", .none);
        const integrity = try backups.artifactIntegrityAlloc(a, std.testing.io, format, artifact_path);
        const inventory = if (!policy.portable) try backups.nativeGenerationManifestIntegrityAllocWithCancellation(a, std.testing.io, artifact_path, .none) else null;
        const table: tables.TableRecord = .{ .table_id = namespace.table_id, .name = name, .schema_json = schema, .read_schema_json = if (policy.migration and i == 2) previous_doc_schema else "", .indexes_json = if (policy.migration and i == 2) "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}" else "{}" };
        manifests[i] = try backups.createManifest(a, "daily", format, &table, &.{.{ .group_id = namespace.shard_id, .range_id = namespace.range_id, .doc_identity_shard_id = namespace.shard_id, .doc_identity_range_id = namespace.range_id, .start_key = "", .snapshot_path = relative, .artifact_size_bytes = integrity.size_bytes, .artifact_sha256 = integrity.sha256, .native_manifest_size_bytes = if (inventory) |v| v.size_bytes else 0, .native_manifest_sha256 = if (inventory) |v| v.sha256 else "" }});
        owners[i] = .{ .table_name = name, .range_start = "", .range_end = "", .fence = fence, .artifact_id = "snapshot", .capture_node_id = 1 };
        proofs[i] = .{ .table_id = namespace.table_id, .name = name, .definition = @splat(1), .manifest_definition = cohort.manifestDefinition(name, table.description, schema, table.read_schema_json, table.indexes_json, table.replication_sources_json) };
        seals[i] = .{ .handle = seal, .source_node_id = 1 };
    }
    const proof: cohort.Job = .{ .id = 700, .revision = 20, .attempt_id = "fixture", .backup_id = "daily", .location = location, .connection = "test-backups", .artifact_format = if (policy.portable) .portable else .native, .tables = &proofs, .state = .{ .phase = .publishing, .metadata_digest = @splat(4), .owners = &owners }, .seals = &seals };
    var sources: [3]driver.SourceTable = undefined;
    std.mem.sort(cohort.Owner, &owners, {}, struct {
        fn less(_: void, lhs: cohort.Owner, rhs: cohort.Owner) bool {
            return std.mem.lessThan(u8, lhs.table_name, rhs.table_name);
        }
    }.less);
    std.mem.sort(cohort.TableProof, &proofs, {}, struct {
        fn less(_: void, lhs: cohort.TableProof, rhs: cohort.TableProof) bool {
            return std.mem.lessThan(u8, lhs.name, rhs.name);
        }
    }.less);
    for (&manifests, &sources) |*manifest, *selected| selected.* = try driver.cohortSource(a, proof, manifest);
    const aggregate: backups.ClusterBackupManifest = .{ .backup_id = "daily", .timestamp = "2026-01-01T00:00:00Z", .location = location, .antfly_version = "test", .expected_table_count = 3, .completed_table_count = 3, .tables = &.{ .{ .name = "parent", .table_backup_id = "daily" }, .{ .name = "child", .table_backup_id = "daily" }, .{ .name = "docs", .table_backup_id = "daily" } }, .cohort_json = try std.json.Stringify.valueAlloc(a, proof, .{}) };
    const aggregate_bytes = try std.json.Stringify.valueAlloc(a, aggregate, .{});
    var aggregate_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(aggregate_bytes, &aggregate_digest, .{});
    var plan = (try driver.buildPlan(a, plan_id, aggregate_digest, sources[0..fixture.owner_count], &.{}, &.{}, "fail_if_exists")).plan.?;
    plan.source_location = location;
    plan.source_connection = "test-backups";
    const command = try std.json.Stringify.valueAlloc(a, stages.Command{ .id = plan.id, .action = .reserve, .plan = plan }, .{});
    const reserved = try source.applyRestoreStaging(a, command, .{});
    const job = try std.json.parseFromSlice(stages.Job, a, reserved, .{});
    try std.testing.expectEqual(stages.State.importing, job.value.state);
    for (job.value.plan.targets, 0..) |target, i| {
        const scope = try stages.ownerScope(a, job.value.plan, job.value.plan_digest, target, target.ranges[0]);
        fixture.scopes[i] = scope;
        fixture.cache_paths[i] = try std.fmt.allocPrint(a, "{s}/decoder-{d}", .{ root, i });
        const database = try alloc.create(db.DB);
        fixture.target_paths[i] = try std.fmt.allocPrint(a, "{s}/target-{d}", .{ root, i });
        database.* = try db.DB.open(alloc, fixture.target_paths[i], .{ .backend_runtime = &runtime, .identity_namespace = scope.target_namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false, .ha_write_gate = policy.gate });
        fixture.dbs[i] = database;
        fixture.target_open[i] = true;
        opened += 1;
        try database.setSchemaJson(alloc, target.table.schema_json);
        try database.reserveRestoreStagingScoped(alloc, scope);
        try database.installRestoreStagingBootstrap(alloc, .{ .scope = scope, .table_name = target.table.name, .schema_json = target.table.schema_json, .read_schema_json = target.table.read_schema_json, .indexes_json = target.table.indexes_json, .byte_range = .{ .start = "", .end = "" } });
        _ = try @import("../metadata/table_provisioner.zig").reconcileDbIndexesWithOptions(alloc, database, target.table.indexes_json, .{ .restore_build_only = true });
        // Match production provisioning: initialization is local and hidden;
        // only the authorized owner generation may start emitting HA effects.
        try database.attachRestoreStagingHAMirror(policy.mirror);
    }
    var hidden = (try source.adminSnapshot()).?;
    try std.testing.expectEqual(@as(usize, 0), hidden.tables.len);
    source.freeAdminSnapshot(&hidden);
    _ = try server.restore_job_store.retryRunning(a, worker.value, "RestoreStagingYield", 0);
    const started_ns = @import("antfly_platform").time.monotonicNs();
    for (0..(if (policy.restart_after_commit) @as(usize, 6000) else 120)) |_| {
        try Driver.work(&server, worker.value.job_id);
        const bytes = (try server.restore_job_store.load(a, worker.value.job_id)).?;
        const state = try std.json.parseFromSlice(restore_jobs.JobState, a, bytes, .{});
        if (state.value.phase == .succeeded) {
            try std.testing.expect(!invalid_child);
            break;
        }
        if (state.value.phase == .failed) {
            try std.testing.expect(invalid_child);
            try std.testing.expectEqual(restore_jobs.StagingResolution.canceled, state.value.staging_resolution);
            try std.testing.expectEqualStrings("ConstraintActivationFailed", state.value.staging_failure);
            break;
        }
        try std.testing.io.sleep(.fromMilliseconds(12), .awake);
    } else {
        const state = try std.json.parseFromSlice(restore_jobs.JobState, a, (try server.restore_job_store.load(a, worker.value.job_id)).?, .{});
        std.debug.print("restore worker did not converge: phase={s} error={s}\n", .{ @tagName(state.value.phase), state.value.last_error orelse "none" });
        return error.RestoreWorkerDidNotConverge;
    }
    var published = (try source.adminSnapshot()).?;
    if (policy.benchmark_rows > 1) {
        var artifact_bytes: u64 = 0;
        for (manifests[0..fixture.owner_count]) |manifest| for (manifest.shards) |shard| {
            artifact_bytes += shard.artifact_size_bytes;
        };
        std.debug.print("restore benchmark format={s} rows={d} artifact_bytes={d} owner_import_calls={d} validation_calls={d} integrity_transactions={d} elapsed_ms={d}\n", .{ if (policy.portable) "portable" else "native", policy.benchmark_rows * fixture.owner_count, artifact_bytes, fixture.imports, fixture.validations, fixture.sequence, (@import("antfly_platform").time.monotonicNs() - started_ns) / std.time.ns_per_ms });
    }
    defer source.freeAdminSnapshot(&published);
    if (policy.restart_after_commit) for (fixture.faults_seen[0..fixture.owner_count]) |seen| {
        const expected: u8 = if (invalid_child) 1 | 2 | 16 else 1 | 2 | 4 | 8;
        try std.testing.expectEqual(expected, seen);
    };
    if (invalid_child) {
        try std.testing.expectEqual(@as(usize, 0), published.tables.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.validations);
        try std.testing.expectEqual(@as(usize, 0), fixture.publications);
        for (fixture.dbs[0..fixture.owner_count]) |target| {
            try std.testing.expectError(error.RestoreStagingCanceled, target.lookup(alloc, "row", .{}));
            var progress = (try target.restoreStagingStatus(alloc)).?;
            defer progress.deinit();
            try std.testing.expectEqual(native.Phase.canceled, progress.value.phase);
        }
        return;
    }
    try std.testing.expectEqual(fixture.owner_count, published.tables.len);
    try std.testing.expect(fixture.imports >= fixture.owner_count and fixture.validations >= fixture.owner_count and fixture.sequence > 0);
    if (policy.restart_after_commit) try std.testing.expect(fixture.publications >= fixture.owner_count) else try std.testing.expectEqual(fixture.owner_count, fixture.publications);
    for (fixture.dbs[0..fixture.owner_count]) |target| {
        var row = (try target.lookup(alloc, "row", .{})).?;
        defer row.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, row.json, "1") != null);
        var progress = (try target.restoreStagingStatus(alloc)).?;
        defer progress.deinit();
        try std.testing.expectEqual(native.Phase.published, progress.value.phase);
    }
    if (policy.migration) {
        fixture.dbs[2].close();
        fixture.target_open[2] = false;
        fixture.dbs[2].* = try db.DB.open(alloc, fixture.target_paths[2], .{ .backend_runtime = &runtime, .identity_namespace = fixture.scopes[2].target_namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
        fixture.target_open[2] = true;
        const target = fixture.dbs[2];
        var read_result = try target.search(alloc, .{ .index_name = "full_text_index_v0", .full_text = .{ .match = .{ .field = "name", .text = "mapping" } }, .limit = 1 });
        defer read_result.deinit();
        try std.testing.expectEqual(@as(u32, 1), read_result.total_hits);
        var active_result = try target.search(alloc, .{ .index_name = "full_text_index_v1", .full_text = .{ .term = .{ .field = "name", .term = "Old Mapping" } }, .limit = 1 });
        defer active_result.deinit();
        try std.testing.expectEqual(@as(u32, 1), active_result.total_hits);
    }
}
