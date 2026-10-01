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
const Allocator = std.mem.Allocator;
const DB = @import("antfly_source_root").antfly_sources.physical_db.DB;
const wire = @import("online_merge_io_contract.zig");
const source = @import("online_source.zig");
const pages = @import("merge_page_contract.zig");
const tail = @import("merge_tail_reader.zig");
const types = @import("types.zig");
const topology = @import("relational_integrity_topology.zig");

/// One bounded retained frame and one prepared fragment per owner. Offset
/// recovery advances at most 128 physical effects per request. No cache mutex
/// is acquired by an apply-locked operation; release retires it after commit.
pub const Cache = struct {
    mutex: std.Io.Mutex = .init,
    scope: ?source.Scope = null,
    session: ?tail.Session = null,
    fragment: ?tail.Fragment = null,
    snapshot: @import("online_merge_snapshot.zig").Cache = .{},
    receiver: @import("online_merge_receiver.zig").Cache = .{},

    fn clear(self: *Cache) void {
        self.snapshot.clear();
        self.receiver.clear();
        if (self.fragment) |*fragment| fragment.deinit();
        self.fragment = null;
        if (self.session) |*session| session.deinit();
        self.session = null;
        self.scope = null;
    }
    pub fn retire(self: *Cache, io: std.Io, scope: ?source.Scope) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (scope == null or self.scope == null or std.meta.eql(scope.?, self.scope.?)) self.clear();
    }
};

fn expectedNamespace(request: wire.Request) @import("doc_identity_namespace.zig").Namespace {
    return if (request.ownerGroup() == request.scope.fence.owner_group_id) request.scope.fence.namespace else request.scope.receiver_namespace;
}

pub fn executeJson(db: *DB, alloc: Allocator, request: wire.Request, cancellation: types.CancellationToken) ![]u8 {
    try request.validate();
    try cancellation.check();
    if (!db.core.identity_namespace.eql(expectedNamespace(request))) return error.OnlineSourceScopeChanged;
    {
        var authority_read = try db.core.store.beginReadTxn();
        defer authority_read.abort();
        const authority = @import("../source_authority.zig");
        if (request.scope.authority == .native or try authority.load(&authority_read) != null)
            _ = try authority.require(&authority_read, request.scope.authority, @import("online_source_contract.zig").namespaceBytes(expectedNamespace(request)));
    }
    return switch (request.operation) {
        .admission => admissionFactsJson(db, alloc, request, cancellation),
        .artifact_catalog => blk: {
            var command = try db.artifactInventoryCommand(alloc);
            defer command.catalogs.deinit(alloc);
            break :blk try std.json.Stringify.valueAlloc(alloc, command, .{});
        },
        .source_catalog => blk: {
            const progress = try db.onlineSourceStatus(request.scope);
            if (progress.phase == .released or progress.snapshot_phase != .published) return error.OnlineSourceScopeChanged;
            const expected = progress.artifact_catalog orelse return error.ArtifactCatalogDrift;
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            var ordered = (try @import("artifact_inventory.zig").load(alloc, &read)) orelse return error.ArtifactCatalogDrift;
            defer ordered.deinit();
            if (!std.meta.eql(ordered.value.command.binding, expected)) return error.ArtifactCatalogDrift;
            // These are the immutable bytes bound before snapshot publication,
            // not a newly derived successor of the live catalog.
            break :blk std.json.Stringify.valueAlloc(alloc, ordered.value.command.catalogs, .{ .emit_strings_as_arrays = true });
        },
        .status => |side| if (side == .donor) sourceStatusJson(db, alloc, request.scope, cancellation) else receiverStatusJson(db, alloc, request.scope, cancellation),
        .tail => |receipt| prepareTailJson(db, alloc, request.scope, receipt, cancellation),
        .rewrite_tail => |page| rewriteTailJson(db, alloc, request.scope, page.after, page.offset, page.max_bytes, cancellation),
        .snapshot, .integrity => |value| @import("online_merge_snapshot.zig").executeJson(db, alloc, request.scope, value.receipt, value.certificate, cancellation),
        .cleanup, .checkpoint => @import("online_merge_receiver.zig").executeJson(db, alloc, request, cancellation),
        .artifact => |operation| @import("source_artifact_transfer.zig").executeJson(db, alloc, operation, cancellation),
        .revoke => std.json.Stringify.valueAlloc(alloc, wire.Prepared{ .scope = request.scope, .request = .{ .relational_topology = .{ .fence = request.scope.fence, .action = .abort_transition } } }, .{}),
        .publication => blk: {
            const certificate = try db.local_execution.source_publication.poll(db, request.scope, cancellation);
            break :blk std.json.Stringify.valueAlloc(alloc, certificate, .{});
        },
    };
}

fn rewriteTailJson(db: *DB, alloc: Allocator, scope: source.Scope, after: u64, offset: u32, max_bytes: u32, cancellation: types.CancellationToken) ![]u8 {
    const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    const cache = &db.local_execution.online_merge_reader;
    try cache.mutex.lock(io);
    defer cache.mutex.unlock(io);
    try cancellation.check();
    const progress = try db.onlineSourceStatus(scope);
    if (progress.phase == .released or progress.snapshot_phase != .published) return error.OnlineSourceScopeChanged;
    const sequence = try std.math.add(u64, after, 1);
    if (cache.scope == null or !std.meta.eql(cache.scope.?, scope) or
        (cache.session != null and cache.session.?.sequence != sequence)) cache.clear();
    if (cache.session == null) {
        cache.session = try db.beginMergeTailRead(scope, after);
        cache.scope = scope;
    }
    const Chunk = @import("relational_rewrite_contract.zig").TailChunk;
    if (cache.session == null) return std.json.Stringify.valueAlloc(alloc, @as(?Chunk, null), .{});
    const session = &cache.session.?;
    const total = session.frameTotal();
    if (offset >= total) return error.RetainedEffectsCursorMismatch;
    const data = try alloc.alloc(u8, @min(total - offset, max_bytes));
    defer alloc.free(data);
    if (try session.readEncoded(offset, data) != data.len) return error.RetainedEffectsCorrupt;
    const descriptor = session.frameDescriptor();
    return std.json.Stringify.valueAlloc(alloc, Chunk{ .pin = scope.pin(), .sequence = sequence, .frame_digest = session.frameDigest(), .total = @intCast(total), .offset = offset, .data = data, .frame_format = if (descriptor != null) .chunked else .contiguous, .descriptor = if (offset == 0) descriptor else null }, .{});
}

fn admissionFactsJson(db: *DB, alloc: Allocator, request: wire.Request, cancellation: types.CancellationToken) ![]u8 {
    _ = try db.advanceArtifactFootprintPage();
    db.core.lockApplyShared();
    defer db.core.unlockApplyShared();
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    // Exactly the mutation-side rule: absence is legal only for a durably
    // initialized document owner, never a missing relational catalog.
    const catalog = try topology.catalogForFence(&txn, request.scope.fence);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(catalog, &digest, .{});
    const retained = try @import("../retained_effects.zig").load(&txn);
    if (retained) |value| if (!std.mem.eql(u8, &value.namespace, &@import("online_source_contract.zig").namespaceBytes(expectedNamespace(request)))) return error.OnlineSourceScopeChanged;
    const next_consumer = std.math.add(u64, if (retained) |value| value.epoch else 0, 1) catch return error.InvalidOnlineSourceCommand;
    const table = @import("table_catalog.zig");
    const table_bytes = txn.get(table.key) catch |err| switch (err) {
        error.NotFound => return error.IntegrityCatalogChanged,
        else => return err,
    };
    const table_facts = try table.Catalog.decode(table_bytes);
    // REF3 and the certificate-bound shadow protocol preserve routed integrity
    // records as well as both primary storage modes. Pending activation or
    // retirement is not an admissible source/receiver ownership proof.
    var integrity_binding: ?pages.IntegrityBinding = null;
    var integrity_ready = true;
    if (db.core.acquireSchemaView()) |view_value| {
        var view = view_value;
        defer view.release();
        if (view.hasCoordinatedConstraints()) {
            var compiled = try @import("relational_integrity_catalog.zig").decode(alloc, catalog);
            defer compiled.deinit();
            const activation = @import("relational_integrity_activation.zig");
            integrity_binding = .{ .catalog_digest = digest, .generation_set = activation.generationSet(compiled) };
            integrity_ready = (try activation.status(&txn, compiled)).state == .enforced;
        }
    }
    var eligible = table_facts.mode_initialized and
        integrity_ready and !try @import("relational_integrity_retirement.zig").active(&txn) and
        try rowDerivedTransferIndexesAssumeApply(db, alloc, request.scope.authority == .raft);
    var artifact_preview = db.artifactInventoryCommand(alloc) catch |err| switch (err) {
        error.OnlineMergeArtifactTailsUnsupported => null,
        else => return err,
    };
    defer if (artifact_preview) |*preview| preview.catalogs.deinit(alloc);
    const artifact_ready = if (artifact_preview) |preview| try db.artifactMaterializationsReady(&txn, preview.catalogs) else false;
    if (request.scope.authority == .raft and request.scope.fence.role == .merge_source)
        eligible = eligible and artifact_ready;
    if (try topology.current(&txn) != null) return error.IntegrityTopologyBusy;
    if (retained) |value| if (value.active()) {
        return error.IntegrityTopologyBusy;
    };
    const merge = @import("merge_state.zig");
    const merge_raw = txn.get(merge.key) catch |err| switch (err) {
        // Match the committed source-admission cut and merge checkpoint
        // loader: pre-protection receiver state remains authoritative in its
        // legacy key until an atomic migration removes that key. A positive
        // discovery fact must not create an online metadata record which the
        // later source admission can never commit.
        error.NotFound => txn.get(merge.legacy_key) catch |legacy_err| switch (legacy_err) {
            error.NotFound => null,
            else => return legacy_err,
        },
        else => return err,
    };
    if (merge_raw) |raw| {
        var state = try merge.decodeAlloc(alloc, raw);
        defer state.deinit(alloc);
        if (state.phase != .finalized and state.phase != .rolled_back and state.phase != .none) {
            if (state.transition_id != request.scope.fence.transition_id or state.donor_group_id != request.scope.fence.owner_group_id or
                state.receiver_group_id != request.scope.fence.peer_group_id) return error.IntegrityTopologyBusy;
            eligible = false; // Resume this exact already-started ordinary plan.
        }
    }
    const marker = txn.get(&@import("../internal_keys.zig").raft_document_applied_entry_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const term = if (marker) |raw| blk: {
        if (raw.len != 16 or std.mem.readInt(u64, raw[8..16], .little) == 0) return error.CorruptRaftAppliedEntry;
        const value = std.mem.readInt(u64, raw[0..8], .little);
        if (value == 0) return error.CorruptRaftAppliedEntry;
        break :blk value;
    } else 0;
    try cancellation.check();
    var manifest_arena = std.heap.ArenaAllocator.init(alloc);
    defer manifest_arena.deinit();
    const source_schemas = if (request.scope.fence.role == .rewrite_source and request.operation.admission == .donor) manifest: {
        // Probe transactions deliberately lack cursors. A bounded read snapshot
        // under the same apply lease observes immutable historical mappings.
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        break :manifest try @import("relational_rewrite_manifest.zig").read(manifest_arena.allocator(), &read, cancellation);
    } else &.{};
    return std.json.Stringify.valueAlloc(alloc, wire.AdmissionFacts{
        .authority = request.scope.authority,
        .source_schemas = source_schemas,
        .namespace = db.core.identity_namespace,
        .eligible = eligible,
        .catalog_digest = digest,
        .integrity = integrity_binding,
        .artifact_catalog = if (artifact_ready) artifact_preview.?.binding else null,
        .next_topology_epoch = try topology.nextEpoch(&txn),
        .next_consumer_epoch = next_consumer,
        .donor_term = term,
        .next_copy_sequence = next_consumer,
    }, .{});
}

test "relational index system online admission facts are unbound read only and reflect durable native epochs" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/facts", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.updateRange(.{ .start = "m", .end = "z" });
    // Ordinary document owners need no coordinated integrity catalog.
    try db.core.store.delete(@import("relational_integrity_catalog.zig").key);
    try db.addIndex(.{ .name = "text_before_admit", .kind = .full_text, .config_json = "{}" });
    const request: wire.Request = .{ .scope = .{
        .fence = .{ .admission_epoch = 0, .attempt = 0, .transition_id = 7, .owner_group_id = 2, .peer_group_id = 3, .role = .merge_source, .namespace = db.core.identity_namespace, .catalog_digest = @splat(0) },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 },
        .consumer_epoch = 0,
        .copy_attempt = .{},
    }, .operation = .{ .admission = .donor } };
    const Fetch = struct {
        fn run(owner: *DB, req: wire.Request) !wire.AdmissionFacts {
            const bytes = try executeJson(owner, std.testing.allocator, req, .none);
            defer std.testing.allocator.free(bytes);
            var parsed = try std.json.parseFromSlice(wire.AdmissionFacts, std.testing.allocator, bytes, .{});
            defer parsed.deinit();
            return parsed.value;
        }
    };
    const before = db.core.store.lastReplaySequence(0);
    var artifact_command = try db.artifactInventoryCommand(alloc);
    defer artifact_command.catalogs.deinit(alloc);
    const facts = try Fetch.run(&db, request);
    try std.testing.expect(facts.eligible);
    try std.testing.expectEqual(@as(u64, 0), facts.donor_term);
    try std.testing.expectEqual(@as(u64, 1), facts.next_consumer_epoch);
    try std.testing.expectEqual(facts.next_consumer_epoch, facts.next_copy_sequence);
    try std.testing.expectEqual(before, db.core.store.lastReplaySequence(0));
    try std.testing.expectEqualSlices(u8, &(try db.relationalTopologyIdentity()).catalog_digest, &facts.catalog_digest);
    const table = @import("table_catalog.zig");
    var inconsistent = db.core.table_catalog;
    inconsistent.storage_mode = .relational;
    try db.core.store.put(table.key, &inconsistent.encode());
    try std.testing.expectError(error.IntegrityCatalogChanged, Fetch.run(&db, request));
    try db.core.store.put(table.key, &db.core.table_catalog.encode());
    var invalid = request;
    invalid.scope.consumer_epoch = 1;
    try std.testing.expectError(error.InvalidOnlineSourceCommand, Fetch.run(&db, invalid));
    invalid = request;
    invalid.operation = .{ .status = .donor };
    try std.testing.expectError(error.InvalidOnlineSourceCommand, Fetch.run(&db, invalid));
    const canceled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, executeJson(&db, alloc, request, types.CancellationToken.fromAtomic(&canceled)));
    var bound = request.scope;
    bound.fence.catalog_digest = facts.catalog_digest;
    bound.fence.admission_epoch = facts.next_topology_epoch;
    bound.fence.attempt = 1;
    bound.consumer_epoch = facts.next_consumer_epoch;
    bound.copy_attempt = .{ .donor_term = 2, .sequence = facts.next_copy_sequence };
    var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 8, .donor_group_id = 4, .receiver_group_id = 2, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
    try db.batch(.{ .merge_checkpoint = checkpoint });
    const active_merge_raw = try db.core.store.get(alloc, @import("merge_state.zig").key);
    defer alloc.free(active_merge_raw);
    // A receiver has no source-retention marker. The durable merge checkpoint
    // itself must prevent graph/artifact catalog changes during copy and tail.
    try std.testing.expectError(error.IntegrityTopologyBusy, db.addIndex(.{ .name = "graph_during_copy", .kind = .graph, .config_json = "{}" }));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.deleteIndex("text_before_admit"));
    try std.testing.expectError(error.IntegrityTopologyBusy, Fetch.run(&db, request));
    var same_ordinary = request;
    same_ordinary.scope.fence.transition_id = 8;
    same_ordinary.scope.fence.owner_group_id = 4;
    same_ordinary.scope.fence.peer_group_id = 2;
    same_ordinary.scope.fence.namespace = .{ .table_id = 1, .shard_id = 4, .range_id = 4 };
    same_ordinary.scope.receiver_namespace = db.core.identity_namespace;
    same_ordinary.operation = .{ .admission = .receiver };
    try std.testing.expect(!(try Fetch.run(&db, same_ordinary)).eligible);
    try std.testing.expectError(error.IntegrityTopologyBusy, @import("../server_db_adapter.zig").applyOrdered(&db, .{ .artifact_catalog = artifact_command, .online_source = .{ .admit = .{ .scope = bound, .artifact_catalog = facts.artifact_catalog } } }, .{ .term = 2, .index = 1 }));
    try @import("../server_db_adapter.zig").applyOrdered(&db, .{}, .{ .term = 2, .index = 1 });
    try std.testing.expectEqual(@as(u64, 1), (try db.raftAppliedEntry()).?.index);
    checkpoint.kind = .rollback;
    try db.batch(.{ .merge_checkpoint = checkpoint });
    const rolled_back_merge_raw = try db.core.store.get(alloc, @import("merge_state.zig").key);
    defer alloc.free(rolled_back_merge_raw);
    try db.core.store.delete(@import("merge_state.zig").key);
    try db.core.store.put(@import("merge_state.zig").legacy_key, active_merge_raw);
    try std.testing.expectError(error.IntegrityTopologyBusy, Fetch.run(&db, request));
    try std.testing.expect(!(try Fetch.run(&db, same_ordinary)).eligible);
    try std.testing.expectError(error.IntegrityTopologyBusy, @import("../server_db_adapter.zig").applyOrdered(&db, .{ .artifact_catalog = artifact_command, .online_source = .{ .admit = .{ .scope = bound, .artifact_catalog = facts.artifact_catalog } } }, .{ .term = 2, .index = 2 }));
    try std.testing.expectEqual(@as(u64, 1), (try db.raftAppliedEntry()).?.index);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try @import("../retained_effects.zig").load(&read)) == null);
        try std.testing.expect((try @import("../source_pin_state.zig").load(&read)) == null);
    }
    try db.core.store.delete(@import("merge_state.zig").legacy_key);
    try db.core.store.put(@import("merge_state.zig").key, rolled_back_merge_raw);
    try std.testing.expect((try Fetch.run(&db, request)).eligible);
    // The ordered catalog and admission commit together. Followers reconcile
    // the exact committed inventory before admitting, while native rewrite
    // admission repeats its local artifact predicate (covered below).
    try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .artifact_catalog = artifact_command, .online_source = .{ .admit = .{ .scope = bound, .artifact_catalog = facts.artifact_catalog } } }, .{ .term = 2, .index = 2 });
    try std.testing.expectError(error.IntegrityTopologyBusy, Fetch.run(&db, request));
    // Once admission has retained the source, a new graph index or
    // independently produced enrichment would invalidate the row-derived
    // artifact proof. The same guard applies to ordinary catalog DDL, not
    // only Raft topology commands; rejected DDL must not alter the catalog.
    try std.testing.expectError(error.IntegrityTopologyBusy, db.addIndex(.{ .name = "graph_after_admit", .kind = .graph, .config_json = "{}" }));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.deleteIndex("text_before_admit"));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.addEnrichment(.{ .name = "chunks_after_admit", .kind = .chunk, .field = "body", .chunk_size = 8, .chunk_overlap = 2, .full_text_index = true }));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.addResolver(.{ .name = "resolver_after_admit", .table = "entities", .source_artifact = "relations", .resolution_artifact = "resolved", .key_template = "{{ _entity.label }}", .config_generation = 1 }));
    {
        const indexes = try db.core.listIndexes(alloc);
        defer types.freeIndexConfigs(alloc, indexes);
        try std.testing.expectEqual(@as(usize, 1), indexes.len);
        try std.testing.expectEqualStrings("text_before_admit", indexes[0].name);
        const enrichments = try db.core.listEnrichments(alloc);
        defer types.freeEnrichmentConfigs(alloc, enrichments);
        try std.testing.expectEqual(@as(usize, 0), enrichments.len);
        const resolvers = try db.core.listResolvers(alloc);
        defer {
            for (resolvers) |*resolver| resolver.deinit(alloc);
            if (resolvers.len != 0) alloc.free(resolvers);
        }
        try std.testing.expectEqual(@as(usize, 0), resolvers.len);
    }
    checkpoint.kind = .accept;
    checkpoint.transition_id = 9;
    // Merely retaining a source (without an active topology freeze) must
    // exclude receiver controls, while leaving ordinary source writes free.
    try std.testing.expectError(error.IntegrityTopologyBusy, db.batch(.{ .merge_checkpoint = checkpoint }));
    try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .relational_topology = .{ .fence = bound.fence, .action = .begin } }, .{ .term = 2, .index = 3 });
    // Rejected commands cannot stall behind an active topology/source fence.
    // The empty exact-entry apply records no row effects or retained frame.
    try @import("../server_db_adapter.zig").applyOrdered(&db, .{}, .{ .term = 2, .index = 4 });
    try std.testing.expectEqual(@as(u64, 4), (try db.raftAppliedEntry()).?.index);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 0), (try @import("../retained_effects.zig").load(&read)).?.latest);
    }
    try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .relational_topology = .{ .fence = bound.fence, .action = .abort_transition } }, .{ .term = 2, .index = 5 });
    try @import("../server_db_adapter.zig").applyOrdered(&db, .{ .online_source = .{ .release = bound } }, .{ .term = 2, .index = 6 });
    const released = try Fetch.run(&db, request);
    try std.testing.expect(released.eligible);
    try std.testing.expectEqual(@as(u64, 2), released.donor_term);
    try std.testing.expectEqual(@as(u64, 2), released.next_consumer_epoch);
    try db.batch(.{ .merge_checkpoint = checkpoint });
    const typed_path = try std.fmt.allocPrint(alloc, "{s}-typed", .{path});
    defer alloc.free(typed_path);
    var typed = try DB.open(alloc, typed_path, .{ .identity_namespace = db.core.identity_namespace, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer typed.close();
    try typed.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"}},"additionalProperties":false}}}}
    );
    // Typed physical rows and their derived indexes use the same immutable
    // snapshot/retained-tail pipeline as document rows.
    try std.testing.expect(try rowDerivedIndexesAssumeApply(&typed, alloc));
    try std.testing.expect((try Fetch.run(&typed, request)).eligible);
    const enriched_path = try std.fmt.allocPrint(alloc, "{s}-enriched", .{path});
    defer alloc.free(enriched_path);
    const enriched_options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{ .identity_namespace = db.core.identity_namespace, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var enriched = try DB.open(alloc, enriched_path, enriched_options);
    defer enriched.close();
    try enriched.setSchemaJson(alloc, "{}");
    try enriched.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    try std.testing.expect((try Fetch.run(&enriched, request)).eligible);
    try enriched.addEnrichment(.{ .name = "chunks", .kind = .chunk, .field = "body", .chunk_size = 8, .chunk_overlap = 2, .full_text_index = true });
    // Full-text is row-derived only when no independently maintained child
    // artifacts/enrichments exist; their presence survives owner restart.
    try std.testing.expect(!(try Fetch.run(&enriched, request)).eligible);
    enriched.close();
    enriched = try DB.open(alloc, enriched_path, enriched_options);
    try std.testing.expect(!(try Fetch.run(&enriched, request)).eligible);
}

test "relational index system native source admission rechecks artifact DDL at commit" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/native-source-artifact-recheck", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{
        .online_source_authority = .native,
        .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
        .primary_backend = .{ .lsm = .{} },
        .start_index_workers = false,
        .start_optional_runtimes = false,
    });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.updateRange(.{ .start = "m", .end = "z" });
    const identity = try db.relationalTopologyIdentity();
    const scope: source.Scope = .{
        .authority = .native,
        .fence = .{
            .admission_epoch = identity.next_epoch,
            .attempt = 1,
            .transition_id = 7,
            .owner_group_id = 2,
            .peer_group_id = 3,
            .role = .rewrite_source,
            .namespace = db.core.identity_namespace,
            .catalog_digest = identity.catalog_digest,
        },
        .receiver_namespace = .{ .table_id = 9, .shard_id = 3, .range_id = 3 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .sequence = 1 },
    };
    // The discovery snapshot preceded a separately committed enrichment
    // config. Native admission must see it inside the final serialized cut.
    try db.addEnrichment(.{ .name = "chunks_before_admit", .kind = .chunk, .field = "body", .chunk_size = 8, .chunk_overlap = 2, .full_text_index = true });
    try std.testing.expectError(error.OnlineSourceScopeChanged, db.batch(.{ .online_source = .{ .admit = .{ .scope = scope } } }));
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try @import("../retained_effects.zig").load(&read)) == null);
        try std.testing.expect((try @import("../source_pin_state.zig").load(&read)) == null);
    }
}

test "relational index system native source admission rechecks artifact DDL cleanup debt" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/cleanup-admission", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{
        .online_source_authority = .native,
        .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
        .primary_backend = .{ .lsm = .{} },
        .start_index_workers = false,
        .start_optional_runtimes = false,
    });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addIndex(.{ .name = "retired_graph", .kind = .graph, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc", .value = "{}" }},
        .graph_writes = &.{.{ .index_name = "retired_graph", .source = "doc", .target = "target", .edge_type = "links" }},
    });
    const keys = @import("../internal_keys.zig");
    const artifact_key = try keys.graphEdgeArtifactKeyAlloc(alloc, "doc", "retired_graph", "links", "target");
    defer alloc.free(artifact_key);
    const admission_key = try keys.managedIndexAdmissionKeyAlloc(alloc, "retired_graph");
    defer alloc.free(admission_key);
    // Exercise the real atomic catalog-removal/cleanup commit, then stop
    // before the public wrapper schedules background cleanup (crash cut).
    {
        const manager = @import("catalog/index_manager.zig");
        manager.test_inject_generated_artifact_cleanup_error = error.TestPostCommitArtifactCleanup;
        defer manager.test_inject_generated_artifact_cleanup_error = null;
        db.core.lockApply();
        defer db.core.unlockApply();
        try std.testing.expect(try db.core.deleteManagedIndex("retired_graph", admission_key));
    }
    try std.testing.expect(db.core.index_manager.graphIndex("retired_graph") == null);
    const stale = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(stale);
    try std.testing.expect(stale.len != 0);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, requireRowDerivedIndexes(&db, alloc));
    const identity = try db.relationalTopologyIdentity();
    const scope: source.Scope = .{
        .authority = .native,
        .fence = .{ .admission_epoch = identity.next_epoch, .attempt = 1, .transition_id = 7, .owner_group_id = 2, .peer_group_id = 3, .role = .rewrite_source, .namespace = db.core.identity_namespace, .catalog_digest = identity.catalog_digest },
        .receiver_namespace = .{ .table_id = 9, .shard_id = 3, .range_id = 3 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .sequence = 1 },
    };
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.batch(.{ .online_source = .{ .admit = .{ .scope = scope } } }));
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try @import("../retained_effects.zig").load(&read)) == null);
        try std.testing.expect((try @import("../source_pin_state.zig").load(&read)) == null);
    }
    var cleanup_idle = false;
    for (0..64) |_| {
        // The DB state machine also finalizes the physical generation and
        // retires its durable cleanup record; draining bytes alone is not a
        // proof that all cleanup obligations have completed.
        if (try db.advanceGeneratedArtifactCleanupPage(null) == .idle) {
            cleanup_idle = true;
            break;
        }
    }
    try std.testing.expect(cleanup_idle);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, artifact_key));
    try requireRowDerivedIndexes(&db, alloc);
    // Even malformed cleanup debt remains fail-closed without decoding it.
    const malformed_key = try keys.indexArtifactCleanupKeyAlloc(alloc, "unknown", 1);
    defer alloc.free(malformed_key);
    try db.core.store.put(malformed_key, "invalid");
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, requireRowDerivedIndexes(&db, alloc));
}

test "relational index system native source admission rejects portable latent graph without catalog or cleanup debt" {
    const alloc = std.testing.allocator;
    const footprint = @import("../artifact_footprint.zig");
    const backup = @import("../portable_backup.zig");
    const keys = @import("../internal_keys.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const from_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/footprint-from", .{tmp.sub_path});
    defer alloc.free(from_path);
    const to_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/footprint-to", .{tmp.sub_path});
    defer alloc.free(to_path);
    const options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{
        .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
        .primary_backend = .{ .lsm = .{} },
        .start_index_workers = false,
        .start_optional_runtimes = false,
    };
    var from = try DB.open(alloc, from_path, options);
    defer from.close();
    try from.setSchemaJson(alloc, "{}");
    const graph_key = try keys.graphEdgeArtifactKeyAlloc(alloc, "a", "removed_index", "edge", "b");
    defer alloc.free(graph_key);
    const codec = @import("enrichment/artifact_codec.zig");
    const graph_value = try codec.encodeGraphEdgeAlloc(alloc, null, 1, 1, 1, 1, "{}");
    defer alloc.free(graph_value);
    // This is the actual legal portable export shape: graph bytes survive
    // independently of the current index catalog and retirement journal.
    try from.core.store.put(graph_key, graph_value);
    var archive: std.ArrayList(u8) = .empty;
    defer archive.deinit(alloc);
    try backup.exportPortable(alloc, from.core.store, &archive);
    var to = try DB.open(alloc, to_path, options);
    defer to.close();
    try backup.importPortable(alloc, to.core.store, archive.items);
    const imported = try to.core.store.get(alloc, graph_key);
    defer alloc.free(imported);
    try std.testing.expect(codec.isPortableUnboundGraphEdge(imported));
    try std.testing.expect(to.core.index_manager.graphIndex("removed_index") == null);
    const cleanup_prefix = try keys.indexArtifactCleanupRootPrefixAlloc(alloc);
    defer alloc.free(cleanup_prefix);
    const cleanup = try to.core.store.scanPrefixKeysPage(alloc, cleanup_prefix, null, 1);
    defer {
        for (cleanup) |item| alloc.free(item);
        alloc.free(cleanup);
    }
    try std.testing.expectEqual(@as(usize, 0), cleanup.len);
    // Coordinated mode is the real protocol-14 family predicate, not merely
    // the native rewrite restriction. No protocol-15 capability is advertised.
    try std.testing.expect(!try rowDerivedTransferIndexesAssumeApply(&to, alloc, true));
    try std.testing.expectError(error.OnlineSourceScopeChanged, requireAdmissibleSourceAtCommitAssumeApply(&to, alloc));
    try std.testing.expect(!backup.isPortableMetadataKey(footprint.key));
    try std.testing.expect(!backup.isPortableMetadataKey(footprint.scan_key));
    try std.testing.expectError(error.InvalidIntegrityOperation, to.batch(.{ .writes = &.{.{ .key = footprint.key, .value = &(footprint.State{ .certified = footprint.all }).encode() }} }));
    to.close();
    to = try DB.open(alloc, to_path, options);
    try std.testing.expect(!try rowDerivedTransferIndexesAssumeApply(&to, alloc, true));
}

test "relational index system native source admission resumes bounded footprint reconciliation and rejects stale absence" {
    const alloc = std.testing.allocator;
    const footprint = @import("../artifact_footprint.zig");
    const keys = @import("../internal_keys.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/footprint-reconcile", .{tmp.sub_path});
    defer alloc.free(path);
    const options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{
        .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
        .primary_backend = .{ .lsm = .{} },
        .start_index_workers = false,
        .start_optional_runtimes = false,
    };
    var db = try DB.open(alloc, path, options);
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    for (0..300) |i| {
        const name = try std.fmt.allocPrint(alloc, "row-{d:0>4}", .{i});
        defer alloc.free(name);
        const stored = try keys.documentKeyAlloc(alloc, name);
        defer alloc.free(stored);
        try db.core.store.put(stored, "{}");
    }
    // Legal physical keys need not fit the old 8 KiB maintenance cursor.
    const large_name = try alloc.alloc(u8, 96 * 1024);
    defer alloc.free(large_name);
    @memset(large_name, 'z');
    const large_stored = try keys.documentKeyAlloc(alloc, large_name);
    defer alloc.free(large_stored);
    try db.core.store.put(large_stored, "{}");
    // An old root has no imported/imagined absence certificate.
    try db.core.store.delete(footprint.key);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, rowDerivedTransferIndexesAssumeApply(&db, alloc, true));
    try std.testing.expect(!try footprint.reconcilePage(alloc, db.core.store));
    const cursor = try db.core.store.get(alloc, footprint.scan_key);
    defer alloc.free(cursor);
    db.close();
    db = try DB.open(alloc, path, options);
    const reopened_cursor = try db.core.store.get(alloc, footprint.scan_key);
    defer alloc.free(reopened_cursor);
    try std.testing.expectEqualSlices(u8, cursor, reopened_cursor);
    // Insert behind the persisted cursor. The old scan cannot certify this
    // family's absence even though subsequent pages never see the new key.
    const graph_key = try keys.graphEdgeArtifactKeyAlloc(alloc, "a", "hidden_graph", "edge", "b");
    defer alloc.free(graph_key);
    {
        var aborted = try db.core.store.beginWriteTxn();
        try aborted.put(graph_key, "uncommitted graph");
        aborted.abort();
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 0), (try footprint.load(&read)).epochs[@intFromEnum(footprint.Family.graph)]);
        try std.testing.expectError(error.NotFound, read.get(graph_key));
    }
    try db.core.store.put(graph_key, "malformed bytes still occupy graph family");
    for (0..32) |_| {
        if (try footprint.reconcilePage(alloc, db.core.store)) break;
    } else return error.TestUnexpectedResult;
    try std.testing.expect(!try rowDerivedTransferIndexesAssumeApply(&db, alloc, true));
    try db.core.store.delete(graph_key);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, rowDerivedTransferIndexesAssumeApply(&db, alloc, true));
    for (0..32) |_| {
        if (try footprint.reconcilePage(alloc, db.core.store)) break;
    } else return error.TestUnexpectedResult;
    try std.testing.expect(try rowDerivedTransferIndexesAssumeApply(&db, alloc, true));
    // Publication invalidation also defeats an earlier completed proof.
    try footprint.invalidate(db.core.store);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, rowDerivedTransferIndexesAssumeApply(&db, alloc, true));
}

test "relational index system online receiver status preserves persisted positioned rows after reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/receiver-status", .{tmp.sub_path});
    defer alloc.free(path);
    const scope: source.Scope = .{
        .fence = .{ .transition_id = 9, .attempt = 1, .owner_group_id = 2, .peer_group_id = 3, .role = .merge_source, .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 4 }, .catalog_digest = @splat(7) },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 5 },
        .consumer_epoch = 6,
        .copy_attempt = .{ .donor_term = 8, .sequence = 1 },
    };
    const receipt: pages.Progress = .{
        .version = 4,
        .transition_id = 9,
        .donor_group_id = 2,
        .receiver_group_id = 3,
        .receiver_namespace = scope.receiver_namespace,
        .attempt = scope.copy_attempt,
        .source = .{ .namespace = scope.fence.namespace, .pin_digest = @splat(1), .applied_index = 19, .retention = .{ .epoch = 6, .after_sequence = 11 } },
        .phase = .rows,
        .sequence = 2,
        .cursor = "last-complete-row",
        .tail_sequence = 11,
        .snapshot_position = .{ .object = 3, .offset = 17, .remaining = 2 },
        .assembly = .{ .transfer_digest = @splat(2), .last_digest = @splat(3), .next_offset = pages.chunk_bytes },
    };
    const options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{ .identity_namespace = scope.receiver_namespace, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.updateRange(.{ .start = "m", .end = "z" });
        var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 9, .donor_group_id = 2, .receiver_group_id = 3, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
        try db.batch(.{ .merge_checkpoint = checkpoint });
        checkpoint.kind = .begin_copy;
        checkpoint.copy_attempt = scope.copy_attempt;
        checkpoint.page_source = receipt.source;
        checkpoint.page_receiver_namespace = scope.receiver_namespace;
        try db.batch(.{ .merge_checkpoint = checkpoint });
        try receipt.validate();
        const persisted = try pages.encode(alloc, receipt);
        defer alloc.free(persisted);
        try db.core.store.put(pages.key, persisted);
    }
    var reopened = try DB.open(alloc, path, options);
    defer reopened.close();
    const raw = try executeJson(&reopened, alloc, .{ .scope = scope, .operation = .{ .status = .receiver } }, .none);
    defer alloc.free(raw);
    var parsed = try std.json.parseFromSlice(wire.ReceiverStatus, alloc, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const actual = parsed.value.progress.?;
    try std.testing.expectEqual(.rows, actual.phase);
    try std.testing.expectEqualStrings(receipt.cursor, actual.cursor);
    try std.testing.expectEqualDeep(receipt.snapshot_position, actual.snapshot_position);
    try std.testing.expectEqualDeep(receipt.assembly, actual.assembly);
    try wire.validateReceiptScope(scope, actual);
    var wrong = scope;
    wrong.consumer_epoch += 1;
    try std.testing.expectError(error.InvalidMergePage, executeJson(&reopened, alloc, .{ .scope = wrong, .operation = .{ .status = .receiver } }, .none));
}

fn sourceStatusJson(db: *DB, alloc: Allocator, scope: source.Scope, cancellation: types.CancellationToken) ![]u8 {
    const preliminary = db.onlineSourceStatus(scope) catch |err| switch (err) {
        error.OnlineSourceScopeChanged => null,
        else => return err,
    };
    // Once admitted, legal vector mutations may keep the vector family dirty.
    // They do not invalidate the already-certified absence of unsupported
    // families. Do not run unrelated maintenance through a prepared source pin.
    if (preliminary == null or preliminary.?.phase == .released)
        _ = try db.advanceArtifactFootprintPage();
    // Published certificates are replicated ledger state, not local sidecar
    // availability. Normal tail/status polling performs no filesystem work.
    const certificate = if (preliminary != null and preliminary.?.published_certificate != null)
        preliminary.?.published_certificate
    else
        @import("source_pin.zig").publicationCertificateIfPresent(db, scope, cancellation) catch |err| switch (err) {
            error.OnlineSourceScopeChanged, error.FileNotFound => null,
            else => return err,
        };
    db.core.lockApplyShared();
    defer db.core.unlockApplyShared();
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    const progress = source.status(&txn, scope) catch |err| switch (err) {
        error.OnlineSourceScopeChanged => null,
        else => return err,
    };
    const retained = try @import("../retained_effects.zig").load(&txn);
    if (retained) |value| if (!std.mem.eql(u8, &value.namespace, &scope.namespace())) return error.OnlineSourceScopeChanged;
    var manager = try db.core.initTxnManager();
    defer manager.deinit();
    const response: wire.SourceStatus = .{
        .scope = scope,
        .certificate = if (progress != null and progress.?.phase != .released) progress.?.published_certificate orelse certificate else null,
        .progress = progress,
        .retained_head = if (retained) |value| value.latest else 0,
        .retained_reclaimed = if (retained) |value| value.reclaimed else 0,
        .retained_reclaimable = if (retained) |value| value.reclaimableThrough() else 0,
        .fence = try topology.current(&txn),
        .next_epoch = try topology.nextEpoch(&txn),
        .drained = !try manager.hasTopologySensitiveTransactions(),
        .row_derived_indexes = try rowDerivedTransferIndexesAssumeApply(db, alloc, scope.authority == .raft),
    };
    try cancellation.check();
    return std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn rowDerivedIndexesAssumeApply(db: *DB, alloc: Allocator) !bool {
    return rowDerivedTransferIndexesAssumeApply(db, alloc, false);
}

/// Discovery facts are advisory. Repeat their mutable eligibility checks
/// inside the serialized source-admission cut, after taking the index catalog
/// mutex and apply lock, so later graph/resolver/enrichment DDL cannot turn a
/// stale positive observation into an untransferable committed source pin.
pub fn requireAdmissibleSourceAtCommitAssumeApply(db: *DB, alloc: Allocator) !void {
    var txn = try db.core.store.beginProbeTxn();
    defer txn.abort();
    const table = @import("table_catalog.zig");
    const table_bytes = txn.get(table.key) catch |err| switch (err) {
        error.NotFound => return error.IntegrityCatalogChanged,
        else => return err,
    };
    const table_facts = try table.Catalog.decode(table_bytes);
    if (!table_facts.mode_initialized or try @import("relational_integrity_retirement.zig").active(&txn))
        return error.OnlineSourceScopeChanged;
    if (db.core.acquireSchemaView()) |view_value| {
        var view = view_value;
        defer view.release();
        if (view.hasCoordinatedConstraints()) {
            const catalog = txn.get(@import("relational_integrity_catalog.zig").key) catch |err| switch (err) {
                error.NotFound => return error.IntegrityCatalogChanged,
                else => return err,
            };
            var compiled = try @import("relational_integrity_catalog.zig").decode(alloc, catalog);
            defer compiled.deinit();
            if ((try @import("relational_integrity_activation.zig").status(&txn, compiled)).state != .enforced)
                return error.IntegrityCatalogChanged;
        }
    } else if (table_facts.storage_mode == .relational) return error.IntegrityCatalogChanged;
    if (!try rowDerivedTransferIndexesAssumeApply(db, alloc, true))
        return error.OnlineSourceScopeChanged;
}

fn rowDerivedTransferIndexesAssumeApply(db: *DB, alloc: Allocator, coordinated: bool) !bool {
    // Protocol support alone does not prove that this owner's backend can
    // produce and retain the immutable native source pin.
    if (db.backend_runtime.filesystemIo() == null or db.physical_root_mode != .filesystem_managed or db.local_execution.source_vectors.load(.acquire) != null) return false;
    switch (db.core.primary_store_owner) {
        .lsm => |owner| {
            const backend = owner.handle.backend;
            const storage = backend.storage orelse return false;
            if (backend.root_dir == null or backend.options.backend.read_only or
                !storage.supportsHostPathGenerationPublication()) return false;
        },
        .none, .mem => return false,
    }
    // Catalog removal and this cleanup record commit atomically. A graph-free
    // catalog is not a row-derived proof while old graph ownership/manifests
    // remain. One metadata-prefix seek detects debt (including malformed
    // records) without scanning primary documents under the apply fence.
    {
        const prefix = try @import("../internal_keys.zig").indexArtifactCleanupRootPrefixAlloc(alloc);
        defer alloc.free(prefix);
        var cleanup_read = try db.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer cleanup_read.abort();
        var cursor = try cleanup_read.openCursor();
        defer cursor.close();
        if (try cursor.seekAtOrAfter(prefix)) |entry| if (std.mem.startsWith(u8, entry.key, prefix)) return error.StorageReadTemporarilyUnavailable;
    }
    {
        const footprint = @import("../artifact_footprint.zig");
        var read = try db.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        const facts = try footprint.load(&read);
        // Protocol 14 carries direct vectors, never latent graph/generated/
        // resolver records. A graph-free catalog is not evidence of absence.
        const required = footprint.all & ~(if (coordinated) footprint.bit(.vector) else @as(u8, 0));
        if (facts.certified & facts.present & required != 0) return false;
        if (facts.certified & required != required) return error.StorageReadTemporarilyUnavailable;
    }
    if (db.core.acquireSchemaView()) |view_value| {
        var view = view_value;
        defer view.release();
        // Distributed claims/references need their own ordered handoff tail;
        // raw primary afterimages alone cannot certify coordinated integrity.
        if (view.hasCoordinatedConstraints() and !coordinated) return false;
    }
    // Chunk/asset/text enrichment can commit materialized artifacts without a
    // Raft row effect, even when every public index is full-text. The canonical
    // enrichment catalog includes inline chunker definitions as well as
    // explicit enrichment resources; REF3 does not retain those side effects.
    const enrichments = try db.core.listEnrichments(alloc);
    defer types.freeEnrichmentConfigs(alloc, enrichments);
    if (enrichments.len != 0) return false;
    // Resolver decisions and promotions can materialize graph artifacts
    // independently of primary-row Raft effects. An index list alone does not
    // prove their absence: resolver configuration is a separate catalog.
    const resolvers = try db.core.listResolvers(alloc);
    defer {
        for (resolvers) |*resolver| resolver.deinit(alloc);
        if (resolvers.len != 0) alloc.free(resolvers);
    }
    if (resolvers.len != 0) return false;
    const indexes = try db.core.listIndexes(alloc);
    defer types.freeIndexConfigs(alloc, indexes);
    for (indexes) |index| if (index.kind != .full_text) {
        // Native rewrite authorities do not order vector publication through
        // a Raft marker. Only the distributed merge family can retain REF4.
        if (!coordinated or !@import("online_vector_artifacts.zig").isEnabled() or
            !try @import("catalog/index_manager.zig").onlineDirectVectorConfig(alloc, index)) return false;
    };
    // Protocol 14 snapshots and retains historical vector names even when
    // no current vector index consumes them. Native row-only transfers still
    // require vector absence through the footprint mask above.
    // Relational indexes live in the immutable relational catalog, not this
    // artifact index list; primary afterimages rebuild their derived entries.
    return true;
}

pub fn requireRowDerivedIndexes(db: *DB, alloc: Allocator) !void {
    _ = try db.advanceArtifactFootprintPage();
    db.core.lockApplyShared();
    defer db.core.unlockApplyShared();
    if (!try rowDerivedIndexesAssumeApply(db, alloc)) return error.TableTopologyProtocolUpgradeRequired;
}

/// Coordinated transfer is authorized by the immutable published certificate,
/// never by relaxing automatic admission or trusting a caller's live catalog.
pub fn requireSnapshotIndexes(db: *DB, alloc: Allocator, identity: pages.Source, certificate: @import("../source_snapshot.zig").Certificate) !void {
    if (!std.meta.eql(identity.integrity, certificate.integrity) or identity.provenance_required != certificate.provenance_required or !std.mem.eql(u8, &identity.pin_digest, &try certificate.digest())) return error.SourceSnapshotCutMismatch;
    db.core.lockApplyShared();
    defer db.core.unlockApplyShared();
    if (!try rowDerivedTransferIndexesAssumeApply(db, alloc, certificate.integrity != null or identity.artifact_catalog != null)) return error.TableTopologyProtocolUpgradeRequired;
    if (identity.artifact_catalog) |binding| {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        try @import("artifact_inventory.zig").requireReady(alloc, &txn, scope_namespace: {
            var namespace: [24]u8 = undefined;
            @import("doc_identity.zig").encodeNamespace(&namespace, identity.namespace);
            break :scope_namespace namespace;
        }, binding);
    }
    if (certificate.integrity != null) {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        try @import("online_integrity_shadow.zig").requireBinding(alloc, &txn, identity);
    }
}

fn receiverStatusJson(db: *DB, alloc: Allocator, scope: source.Scope, cancellation: types.CancellationToken) ![]u8 {
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    const state_bytes = txn.get(@import("merge_state.zig").key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var state = if (state_bytes) |bytes| try @import("merge_contract.zig").decodeAlloc(alloc, bytes) else null;
    defer if (state) |*value| value.deinit(alloc);
    if (state) |*value| if (value.transition_id != scope.fence.transition_id and (value.phase == .finalized or value.phase == .rolled_back)) {
        value.deinit(alloc);
        state = null;
        return std.json.Stringify.valueAlloc(alloc, wire.ReceiverStatus{ .scope = scope, .state = null, .progress = null }, .{});
    };
    if (state) |value| if (value.transition_id != scope.fence.transition_id or value.donor_group_id != scope.fence.owner_group_id or
        value.receiver_group_id != scope.fence.peer_group_id or (value.copy_attempt.sequence != 0 and !std.meta.eql(value.copy_attempt, scope.copy_attempt))) return error.MergeCopyFenced;
    const receipt_bytes = txn.get(pages.key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var receipt = if (receipt_bytes) |bytes| try pages.decode(alloc, bytes) else null;
    defer if (receipt) |*value| value.deinit();
    if (receipt) |value| try wire.validateReceiptScope(scope, value.value);
    try cancellation.check();
    return std.json.Stringify.valueAlloc(alloc, wire.ReceiverStatus{ .scope = scope, .state = state, .progress = if (receipt) |value| value.value else null }, .{});
}

fn validatePublished(scope: source.Scope, progress: source.Progress, identity: pages.Source) !void {
    if (!std.meta.eql(progress.artifact_catalog, identity.artifact_catalog)) return error.SourceSnapshotCutMismatch;
    if (progress.phase == .released or progress.snapshot_phase != .published or identity.retention == null or
        !identity.namespace.eql(scope.fence.namespace) or identity.retention.?.epoch != scope.consumer_epoch or identity.retention.?.after_sequence != progress.start or
        identity.applied_index != progress.admitted_applied_index or !std.mem.eql(u8, &identity.pin_digest, &progress.snapshot_certificate)) return error.SourceSnapshotCutMismatch;
    if (!std.meta.eql(identity.integrity, if (progress.published_certificate) |certificate| certificate.integrity else null) or
        identity.provenance_required != (if (progress.published_certificate) |certificate| certificate.provenance_required else false)) return error.SourceSnapshotCutMismatch;
}

fn encodePrepared(alloc: Allocator, scope: source.Scope, request: ?types.BatchRequest) ![]u8 {
    return std.json.Stringify.valueAlloc(alloc, wire.Prepared{ .scope = scope, .request = request }, .{});
}

fn prepareTailJson(db: *DB, alloc: Allocator, scope: source.Scope, receipt: pages.Progress, cancellation: types.CancellationToken) ![]u8 {
    const io = db.backend_runtime.io() orelse return error.BackendRuntimeIoUnavailable;
    const cache = &db.local_execution.online_merge_reader;
    try cache.mutex.lock(io);
    defer cache.mutex.unlock(io);
    // No apply fence is held while taking the cache lock. Revalidate after
    // acquiring it so a source release cannot resurrect a retired reader.
    const progress = try db.onlineSourceStatus(scope);
    try validatePublished(scope, progress, receipt.source);
    const next_sequence = try std.math.add(u64, receipt.tail_sequence, 1);
    if (cache.scope == null or !std.meta.eql(cache.scope.?, scope) or
        (cache.session != null and cache.session.?.sequence != next_sequence)) cache.clear();
    if (cache.fragment) |*fragment| {
        if (fragment.tail.fragment.sequence == next_sequence and fragment.tail.fragment.offset == receipt.tail_offset)
            return encodeFragment(alloc, scope, fragment, receipt);
        fragment.deinit();
        cache.fragment = null;
    }
    if (cache.session == null) {
        cache.session = try db.beginMergeTailRead(scope, receipt.tail_sequence);
        cache.scope = scope;
    }
    const context: types.MergeReplicationContext = .{ .transition_id = scope.fence.transition_id, .donor_group_id = scope.fence.owner_group_id, .receiver_group_id = scope.fence.peer_group_id, .identity_namespace = scope.receiver_namespace, .copy_attempt = scope.copy_attempt };
    if (cache.session == null) {
        if (progress.phase != .fenced) return encodePrepared(alloc, scope, null);
        if (receipt.tail_offset != 0 or receipt.tail_sequence != progress.through_sequence) return error.MergePageSequenceGap;
        var result: types.BatchRequest = .{ .merge_replication = context, .merge_page = .{ .source = receipt.source, .sequence = try std.math.add(u64, receipt.sequence, 1), .phase = .tail, .exhausted = true, .digest = @splat(0), .tail = .{ .finish = .{ .through_sequence = progress.through_sequence, .applied_index = progress.applied_index, .cut_digest = progress.cut_digest } } } };
        result.merge_page.?.digest = pages.commandDigest(result);
        try pages.validateRequest(result);
        return encodePrepared(alloc, scope, result);
    }
    const session = &cache.session.?;
    if (session.offset > receipt.tail_offset or session.total < receipt.tail_offset) return error.MergePageSequenceGap;
    var skipped: usize = 0;
    while (session.offset < receipt.tail_offset and skipped < pages.max_rows) : (skipped += 1) {
        try cancellation.check();
        try session.skipOne();
    }
    if (session.offset != receipt.tail_offset) return encodePrepared(alloc, scope, null);
    cache.fragment = (try session.next(db.alloc, pages.max_rows, pages.max_bytes, cancellation)) orelse return error.RetainedEffectsCorrupt;
    try cancellation.check();
    return encodeFragment(alloc, scope, &cache.fragment.?, receipt);
}

fn encodeFragment(alloc: Allocator, scope: source.Scope, fragment: *tail.Fragment, receipt: pages.Progress) ![]u8 {
    const context: types.MergeReplicationContext = .{ .transition_id = scope.fence.transition_id, .donor_group_id = scope.fence.owner_group_id, .receiver_group_id = scope.fence.peer_group_id, .identity_namespace = scope.receiver_namespace, .copy_attempt = scope.copy_attempt };
    const sequence = try std.math.add(u64, receipt.sequence, 1);
    const request = fragment.request(receipt.source, context, sequence) catch |err| switch (err) {
        error.MergePageChunkRequired => blk: {
            const chunks = try fragment.chunkRequests(receipt.source, context, sequence);
            break :blk try chunks.requestAt(if (receipt.assembly) |assembly| assembly.next_offset else 0);
        },
        else => return err,
    };
    return encodePrepared(alloc, scope, request);
}

test "relational index system rewrite admission owns complete immutable historical schema manifest" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/history", .{tmp.sub_path});
    defer alloc.free(path);
    const options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const v1 = "{\"version\":1}";
    const v2 = "{\"version\":2}";
    const v3 = "{\"version\":3}";
    var request: wire.Request = .{ .scope = .{
        .fence = .{ .admission_epoch = 0, .attempt = 0, .transition_id = 7, .owner_group_id = 2, .peer_group_id = 3, .role = .rewrite_source, .namespace = options.identity_namespace.?, .catalog_digest = @splat(0) },
        .receiver_namespace = .{ .table_id = 9, .shard_id = 3, .range_id = 3 },
        .consumer_epoch = 0,
        .copy_attempt = .{},
    }, .operation = .{ .admission = .donor } };
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, v1);
        try db.setSchemaJson(alloc, v2);
        try db.setSchemaJson(alloc, v3);
    }
    var db = try DB.open(alloc, path, options);
    defer db.close();
    const before = db.core.store.lastReplaySequence(0);
    const json = try executeJson(&db, alloc, request, .none);
    defer alloc.free(json);
    const facts = try std.json.parseFromSlice(wire.AdmissionFacts, alloc, json, .{});
    defer facts.deinit();
    try std.testing.expectEqual(@as(usize, 3), facts.value.source_schemas.len);
    for ([_][]const u8{ v1, v2, v3 }, facts.value.source_schemas) |expected, actual| try std.testing.expectEqualStrings(expected, actual);
    try std.testing.expectEqual(before, db.core.store.lastReplaySequence(0));

    const public = @import("../../schema/mod.zig");
    const old_key = try public.versionedSchemaKeyAlloc(alloc, 1);
    defer alloc.free(old_key);
    try db.core.store.delete(old_key);
    try std.testing.expectError(error.UnknownSchemaVersion, executeJson(&db, alloc, request, .none));
    try db.core.store.put(old_key, v2);
    try std.testing.expectError(error.RestoreStagingScopeChanged, executeJson(&db, alloc, request, .none));
    try db.core.store.put(old_key, v1);
    const alias = "\x00\x00__metadata__:schema_json_v01";
    try db.core.store.put(alias, v1);
    try std.testing.expectError(error.RestoreStagingScopeChanged, executeJson(&db, alloc, request, .none));
    try db.core.store.delete(alias);
    const oversized = try alloc.alloc(u8, @import("relational_rewrite_contract.zig").max_schema_bytes + 1);
    defer alloc.free(oversized);
    @memset(oversized, ' ');
    try db.core.store.put(old_key, oversized);
    try std.testing.expectError(error.RelationalRewriteBudgetExceeded, executeJson(&db, alloc, request, .none));
    // Ordinary online admission does not enumerate or allocate historical
    // rewrite metadata, even when unrelated old metadata is malformed.
    request.scope.fence.role = .merge_source;
    request.scope.receiver_namespace.table_id = 1;
    const ordinary = try executeJson(&db, alloc, request, .none);
    defer alloc.free(ordinary);
    const parsed = try std.json.parseFromSlice(wire.AdmissionFacts, alloc, ordinary, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.value.source_schemas.len);
}
