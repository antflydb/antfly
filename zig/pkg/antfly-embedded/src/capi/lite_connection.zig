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

//! Connection lifetimes are independent of writer ownership. A kernel lease
//! covers a complete exported operation (including SQL's commit coordinator).
//! Compatible connections share a persistent storage owner and its caches.
//! External publications reopen that owner; SQL streams retain their pinned
//! generations, and sessions and cursor IDs remain connection-local.
const h = @import("handles.zig");
const api = @import("db.zig");
const std = h.std;
const native = h.lite_backend.native;
const inode_lock = @import("../storage/lite/inode_lock.zig");
const shared_lock_region = @import("../storage/lite/shared_lock_region.zig");
const antfly_platform = @import("antfly_platform");
const alloc = std.heap.c_allocator;

const memo_mod = @import("antfly_inference_query_embedding_cache");
const cache_budget = @import("antfly_cache_budget");
const wal_trace = @import("../storage/write_amplification_trace.zig");
const embedding_memo_bytes = 16 * 1024 * 1024;
const EnrichmentRuntime = std.meta.Child(std.meta.Child(@FieldType(h.db_mod.DB, "enrichment_runtime")));
const PublicationPort = std.meta.Child(@FieldType(EnrichmentRuntime, "generated_publication"));
const PublicationParameters = @typeInfo(std.meta.Child(@FieldType(PublicationPort, "commit"))).@"fn".param_types;
const GeneratedBatch = PublicationParameters[1].?;
const Promotions = PublicationParameters[2].?;
const SourceGuards = PublicationParameters[4].?;
const WriteFence = std.meta.Child(PublicationParameters[5].?);
const StoreLease = @typeInfo(@typeInfo(std.meta.Child(@FieldType(PublicationPort, "acquire_store"))).@"fn".return_type.?).error_union.payload;
const RuntimeStore = @FieldType(StoreLease, "store");
const SourceRead = @typeInfo(@typeInfo(std.meta.Child(@FieldType(RuntimeStore.VTable, "begin_read"))).@"fn".return_type.?).error_union.payload;
const keys = @import("../storage/internal_keys.zig");
const ProducerLease = @FieldType(@FieldType(EnrichmentRuntime, "ownership"), "lease");
const NativeStore = std.meta.Child(std.meta.Child(@FieldType(std.meta.Child(@FieldType(h.Handle, "owned_lite_backend")), "native_docstore")));
const WriterAdmission = std.meta.Child(@FieldType(NativeStore, "writer_admission"));
const PublicationScope = @typeInfo(std.meta.Child(@FieldType(WriterAdmission, "enter"))).@"fn".param_types[1].?;

/// Owned logical fingerprints need no turn-wide catalog pin. New metadata
/// records persist their content digest, so even large catalogs can be checked
/// with bounded record reads, including after their physical pages are reused.
const PublicationCatalog = struct {
    const Txn = @import("../storage/lite/docstore.zig").Txn;
    const metadata_keys = [_][]const u8{
        keys.public_schema_json_key,
        "\x00\x00__metadata__:schema",
        "\x00\x00__metadata__:indexes",
        "\x00\x00__metadata__:enrichments",
        "\x00\x00__metadata__:resolvers",
        @import("../storage/db/catalog/resolver_catalog.zig").merge_catalog_key,
        @import("../storage/db/resolver_projection.zig").epoch_key,
    };
    inode: u64,
    paths: [metadata_keys.len][]const u8,
    fingerprints: [metadata_keys.len]?[32]u8,

    fn capture(a: std.mem.Allocator, root: *h.Handle, database: *h.db_mod.DB) !PublicationCatalog {
        const backend = &root.owned_lite_backend.?;
        const store = backend.native_docstore.?;
        var pin = try store.beginRead();
        defer pin.abort();
        const prefix = if (database == &root.db) "" else prefix: {
            const namespace = try @import("../storage/lite/embedded_catalog.zig").namespaceAlloc(a, database.core.identity_namespace.table_id);
            break :prefix try backend.documentPrefixAlloc(a, namespace);
        };
        var result: PublicationCatalog = .{ .inode = (try (try pin.readFile()).file.stat((try pin.readFile()).runtime())).inode, .paths = undefined, .fingerprints = undefined };
        for (metadata_keys, 0..) |key, i| {
            result.paths[i] = try std.mem.concat(a, u8, &.{ prefix, key });
            result.fingerprints[i] = try pin.valueFingerprint(result.paths[i]);
        }
        return result;
    }
    fn verify(self: *PublicationCatalog, root: *h.Handle) !void {
        var live = try root.owned_lite_backend.?.native_docstore.?.beginRead();
        defer live.abort();
        const file = try live.readFile();
        if ((try file.file.stat(file.runtime())).inode != self.inode) return error.EnrichmentSourceChanged;
        for (self.paths, self.fingerprints) |path, captured|
            if (!std.meta.eql(captured, try live.valueFingerprint(path))) return error.EnrichmentSourceChanged;
    }
    fn deinit(self: *PublicationCatalog) void {
        _ = self; // Paths are owned by the capture arena; no reader is retained.
    }
};

// Completed producer bytes are delivered to the current runtime, rather than
// replaying inference whenever an unrelated primary commit moves the file tip.
pub const ShadowScope = struct { name: []const u8, generation: u64, prefix: []const u8 };

pub const PreparedPublication = struct {
    const DocumentGuards = struct {
        arena: std.heap.ArenaAllocator,
        keys: std.StringHashMapUnmanaged(void) = .empty,
    };
    owner: *Owner,
    name: []const u8,
    identity: std.meta.Child(@FieldType(h.db_mod.OpenOptions, "identity_namespace")),
    incarnation: @FieldType(h.db_mod.DB, "root_incarnation"),
    catalog: ?*PublicationCatalog,
    index_prefix: []const u8,
    shadow_scopes: []const ShadowScope = &.{},
    snapshot_root: *h.Handle,
    snapshot_database: *h.db_mod.DB,
    source_read: ?*SourceRead,
    root_catalog_read: ?*SourceRead = null,
    /// Exists only while one live writer fence excludes intervening commits.
    document_guards: ?*DocumentGuards = null,
    relational_rows: bool,
    index_inputs_changed: bool = false,
    resolver_dependency: ?struct { table: []const u8, template: []const u8, table_id: ?u64, head: ?@import("resolver_authority.zig").Stamp } = null,

    fn current(self: *@This()) !*h.db_mod.DB {
        const root = try self.owner.refresh();
        if (Owner.needsOutcomeReconciliation(root)) return error.EnrichmentWaitCanceled;
        // Reject stale captured catalogs before recovery can open a replacement
        // table runtime. In particular, half-published or invalid schema bytes
        // must fence this task rather than being decoded on its behalf.
        try (self.catalog orelse return error.EnrichmentSourceChanged).verify(root);
        try @import("sql_commit.zig").recover(root);
        try @import("sql_ddl.zig").recover(root);
        const database = try @import("tables.zig").get(root, self.name);
        if (database.root_incarnation != self.incarnation or !std.meta.eql(database.core.identity_namespace, self.identity)) return error.EnrichmentSourceChanged;
        try (self.catalog orelse return error.EnrichmentSourceChanged).verify(root);
        if (self.resolver_dependency) |dependency| {
            const destination = @import("tables.zig").get(root, dependency.table) catch |err| switch (err) {
                error.UndefinedTable => null,
                else => return err,
            };
            const table_id: ?u64 = if (destination) |db| db.core.identity_namespace.table_id else null;
            const head = if (destination) |db| try @import("resolver_authority.zig").namespaceStamp(db, alloc, dependency.template) else null;
            if (table_id != dependency.table_id or !std.meta.eql(head, dependency.head)) return error.EnrichmentSourceChanged;
        }
        if (self.shadow_scopes.len != 0) {
            const jobs = try database.graphReplacementStatusAlloc(alloc);
            defer jobs.deinit();
            for (self.shadow_scopes) |scope| {
                const matches = for (jobs.value) |job| {
                    if (std.mem.eql(u8, job.name, scope.name) and job.generation == scope.generation and job.phase != .failed) break true;
                } else false;
                if (!matches) return error.EnrichmentSourceChanged;
            }
        }
        const live_file = &root.owned_lite_backend.?.native_docstore.?.file;
        try live_file.inheritAllocatorVerification(&self.snapshot_root.owned_lite_backend.?.native_docstore.?.file);
        if (!live_file.free_pages_verified) {
            self.owner.publication_stats.allocator_refusals +|= 1;
            return error.EnrichmentWaitCanceled;
        }
        return database;
    }

    /// Storage publication only needs identity/catalog and physical-input
    /// fences. Opening a named table here reloads every index under the owner
    /// gate, once per output file, and can strand foreground admission.
    fn validateStorageCurrent(self: *@This()) !void {
        if (self.shadow_scopes.len != 0 or self.resolver_dependency != null) {
            _ = try self.current();
            return;
        }
        const root = try self.owner.refresh();
        if (Owner.needsOutcomeReconciliation(root)) return error.EnrichmentWaitCanceled;
        // Reject stale captured catalogs before recovery can open a replacement
        // table runtime. In particular, half-published or invalid schema bytes
        // must fence this task rather than being decoded on its behalf.
        try (self.catalog orelse return error.EnrichmentSourceChanged).verify(root);
        try @import("sql_commit.zig").recover(root);
        try @import("sql_ddl.zig").recover(root);
        if (std.mem.eql(u8, self.name, "default")) {
            if (root.db.root_incarnation != self.incarnation or !std.meta.eql(root.db.core.identity_namespace, self.identity)) return error.EnrichmentSourceChanged;
        } else {
            try @import("tables.zig").loadCatalog(root);
            const id = root.embedded_table_records.get(self.name) orelse return error.EnrichmentSourceChanged;
            const identity: @TypeOf(self.identity) = .{ .table_id = id, .shard_id = id, .range_id = id };
            if (!std.meta.eql(identity, self.identity)) return error.EnrichmentSourceChanged;
            const namespace = try @import("../storage/lite/embedded_catalog.zig").namespaceAlloc(alloc, id);
            defer alloc.free(namespace);
            const incarnation = try root.owned_lite_backend.?.existingRootIncarnation(namespace);
            if (incarnation == null or incarnation.? != self.incarnation) return error.EnrichmentSourceChanged;
        }
        try (self.catalog orelse return error.EnrichmentSourceChanged).verify(root);
        const live_file = &root.owned_lite_backend.?.native_docstore.?.file;
        try live_file.inheritAllocatorVerification(&self.snapshot_root.owned_lite_backend.?.native_docstore.?.file);
        if (!live_file.free_pages_verified) {
            self.owner.publication_stats.allocator_refusals +|= 1;
            return error.EnrichmentWaitCanceled;
        }
    }

    fn commit(ptr: *anyopaque, batch: GeneratedBatch, promotions: Promotions, deletes: []const []const u8, guards: SourceGuards, fence: ?WriteFence) !@typeInfo(@typeInfo(std.meta.Child(@FieldType(PublicationPort, "commit"))).@"fn".return_type.?).error_union.payload {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        // `database.publishPreparedGeneratedBatch` below writes through the
        // same store `Owner.enterMaintenanceWriter` gates, and so reenters
        // that same admission for `self.snapshot_root`. Route through it
        // directly instead of a separate, uncoordinated `gate.mutex` hold --
        // see `acquireStore`'s own doc comment for why that deadlocks the
        // nested call against this one's own hold.
        try Owner.enterMaintenanceWriter(self.snapshot_root, .primary);
        defer Owner.leaveMaintenanceWriter(self.snapshot_root);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
        self.owner.publication_stats.attempts +|= 1;
        self.owner.publication_stats.generated_attempts +|= 1;
        errdefer {
            self.owner.publication_stats.refused +|= 1;
            self.owner.publication_stats.generated_refused +|= 1;
        }
        const held_from = nowNs();
        defer MaintenanceHoldSites.record(&self.owner.publication_stats.maintenance_hold_sites.generated_commit, held_from);
        const database = try self.current();
        // Namespace and catalog are stable under the owner fence. The native
        // writer validates the captured source digests/incarnations and lease
        // inside its live transaction, preserving unrelated primary commits.
        const result = database.publishPreparedGeneratedBatch(batch, promotions, deletes, guards, fence) catch |err| switch (err) {
            error.EnrichmentSourceChanged, error.EnrichmentLeaseFenceLost => return error.EnrichmentWaitCanceled,
            else => return err,
        };
        self.owner.notifyMutation();
        return result;
    }

    pub fn publishPromotions(self: *@This(), allocator: std.mem.Allocator, entries: []const @import("../storage/db/runtime_callbacks.zig").EntityUpsert) !void {
        // `entity_sink.upsertBatch` below writes through the same store
        // `Owner.enterMaintenanceWriter` gates -- see `acquireStore`'s own
        // doc comment for why a separate, uncoordinated `gate.mutex` hold
        // here would deadlock that nested call against this one's own.
        try Owner.enterMaintenanceWriter(self.snapshot_root, .primary);
        defer Owner.leaveMaintenanceWriter(self.snapshot_root);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
        // Validate source namespace/catalog and replacement generation before
        // opening destination intents. Foreground recovery cannot observe or
        // abandon an in-flight preparation while this fence is held.
        _ = try self.current();
        try @import("entity_sink.zig").upsertBatch(self.owner.current.?, allocator, entries);
        self.refreshSnapshotCheckpoint();
        self.owner.notifyMutation();
    }

    const HeldStore = struct {
        publication: *PreparedPublication,
        database: *h.db_mod.DB,
        refusal_recorded: bool = false,
        held_from: i96 = 0,

        fn release(ptr: *anyopaque) void {
            const held: *@This() = @ptrCast(@alignCast(ptr));
            const p = held.publication;
            p.refreshSnapshotCheckpoint();
            MaintenanceHoldSites.record(&p.owner.publication_stats.maintenance_hold_sites.generated_store, held.held_from);
            // Matches the `Owner.enterMaintenanceWriter` admission `acquireStore`
            // took below -- see that call site's own doc comment.
            Owner.leaveMaintenanceWriter(p.snapshot_root);
            alloc.destroy(held);
        }

        fn guardKey(ptr: *anyopaque, key: []const u8) !void {
            const held: *@This() = @ptrCast(@alignCast(ptr));
            errdefer {
                if (!held.refusal_recorded) {
                    held.refusal_recorded = true;
                    held.publication.owner.publication_stats.refused +|= 1;
                    held.publication.owner.publication_stats.generated_refused +|= 1;
                }
            }
            try held.publication.guardKey(held.database, key);
        }
    };

    fn indexInputsUnchanged(self: *@This(), captured: *native.NativeFile, latest: *native.NativeFile) !bool {
        if (self.shadow_scopes.len == 0) return captured.indexCatalogPrefixUnchanged(latest, self.index_prefix);
        for (self.shadow_scopes) |scope| if (!try captured.indexCatalogPrefixUnchanged(latest, scope.prefix)) return false;
        return true;
    }

    fn refreshSnapshotCheckpoint(self: *@This()) void {
        const snapshot = self.snapshot_root.owned_lite_backend.?.native_docstore.?;
        // Opening the live coordinator can add immutable-reader leases.
        // Rebind primary reads after publication, but remember any changed
        // LSM inputs: adopting a header must never erase that evidence.
        var latest = native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.owner.path, .{ .read_only = true, .internal_reader = true }) catch null;
        if (latest) |*file| {
            defer file.close();
            const live_stat = file.file.stat(file.runtime()) catch null;
            const captured_stat = snapshot.file.file.stat(snapshot.file.runtime()) catch null;
            const same_inode = live_stat != null and captured_stat != null and live_stat.?.inode == captured_stat.?.inode;
            if (same_inode) {
                if (snapshot.file.activeCheckpoint().index_catalog_root_page != file.activeCheckpoint().index_catalog_root_page) {
                    const unchanged = self.indexInputsUnchanged(&snapshot.file, file) catch false;
                    if (!unchanged) self.index_inputs_changed = true;
                }
                snapshot.refreshIndexPublicationCheckpoint(file.header) catch {
                    self.index_inputs_changed = true;
                };
                if (self.owner.current) |live_root| snapshot.file.inheritAllocatorVerification(&live_root.owned_lite_backend.?.native_docstore.?.file) catch {};
            } else self.index_inputs_changed = true;
        } else self.index_inputs_changed = true;
    }

    /// Index publication needs current catalog bytes, not a complete foreground
    /// runtime reopen for every native mutation. The path lease pins these raw
    /// observations through commit; primary producer publication still uses current().
    fn requireCurrentCatalog(self: *@This()) !void {
        const captured = self.source_read orelse return error.EnrichmentWaitCanceled;
        var expected = try captured.openReadScope(alloc);
        defer expected.close();
        var live = try self.snapshot_database.core.store.runtime_store.beginRead();
        defer live.abort();
        var actual = try live.openReadScope(alloc);
        defer actual.close();
        const inventory = @import("../storage/db/artifact_inventory.zig");
        for ([_][]const u8{
            "\x00\x00__metadata__:schema_json",
            @import("../storage/db/table_catalog.zig").key,
            inventory.index_key,
            inventory.enrichment_key,
            inventory.resolver_key,
            &keys.identity_namespace_key,
        }) |key| try sameValue(&expected, &actual, key);
        const root_capture = self.root_catalog_read orelse return error.EnrichmentWaitCanceled;
        var root_expected = try root_capture.openReadScope(alloc);
        defer root_expected.close();
        var root_live = try self.snapshot_root.db.core.store.runtime_store.beginRead();
        defer root_live.abort();
        var root_actual = try root_live.openReadScope(alloc);
        defer root_actual.close();
        try sameValue(&root_expected, &root_actual, @import("tables.zig").catalog_key);
    }

    fn guardDocument(self: *@This(), database: *h.db_mod.DB, doc_key: []const u8) !void {
        if (self.document_guards) |guards| if (guards.keys.contains(doc_key)) return;
        const captured_read = self.source_read orelse return error.EnrichmentWaitCanceled;
        var expected = try captured_read.openReadScope(alloc);
        defer expected.close();
        var actual = try database.core.store.runtime_store.beginRead();
        defer actual.abort();
        const row_key = if (self.relational_rows) try @import("../storage/db/relational_store.zig").keyAlloc(alloc, doc_key) else try keys.documentKeyAlloc(alloc, doc_key);
        defer alloc.free(row_key);
        const ordinal_key = try keys.identityDocToOrdinalKeyAlloc(alloc, doc_key);
        defer alloc.free(ordinal_key);
        for ([_][]const u8{ row_key, ordinal_key }) |key| try sameValue(&expected, &actual, key);
        if (expected.get(ordinal_key)) |ordinal| {
            if (ordinal.len != 4) return error.EnrichmentWaitCanceled;
            const state_key = keys.identityOrdinalStateKey(std.mem.readInt(u32, ordinal[0..4], .big));
            try sameValue(&expected, &actual, &state_key);
        } else |err| if (err != error.NotFound) return err;
        if (self.document_guards) |guards| {
            const a = guards.arena.allocator();
            try guards.keys.put(a, try a.dupe(u8, doc_key), {});
        }
    }

    fn sameValue(expected: anytype, actual: anytype, key: []const u8) !void {
        const before = expected.get(key) catch |err| if (err == error.NotFound) null else return err;
        const after = actual.get(key) catch |err| if (err == error.NotFound) null else return err;
        if ((before == null) != (after == null)) return error.EnrichmentWaitCanceled;
        if (before) |bytes| if (!std.mem.eql(u8, bytes, after.?)) return error.EnrichmentWaitCanceled;
    }

    fn guardKey(self: *@This(), database: *h.db_mod.DB, key: []const u8) !void {
        const doc_key = (try keys.decodeDocumentComponentAlloc(alloc, key)) orelse return;
        defer alloc.free(doc_key);
        try self.guardDocument(database, doc_key);
    }

    fn guardReplayKey(self: *@This(), database: *h.db_mod.DB, key: []const u8, inputs: ?@import("../storage/db/replay_read_set.zig").View) !void {
        try self.guardKey(database, key);
        // Computed resolver outputs were read through the captured logical
        // store and validated above. A prior publication in this turn may
        // have changed them legitimately; comparing them again to the turn's
        // original physical snapshot would reject our own committed output.
        if (inputs) |read_set| if (read_set.hasPoint(key)) return;
        const captured_read = self.source_read orelse return error.EnrichmentWaitCanceled;
        var expected = try captured_read.openReadScope(alloc);
        defer expected.close();
        var actual = try database.core.store.runtime_store.beginRead();
        defer actual.abort();
        try sameValue(&expected, &actual, key);
    }

    fn publishReplay(ptr: *anyopaque, batch: h.db_mod.derived_types.DerivedBatch, artifact_keys: []const []const u8, inputs: ?@import("../storage/db/replay_read_set.zig").View, request: *anyopaque, apply: *const fn (*anyopaque, *anyopaque) anyerror!u64) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const io = h.handleLockIo();
        self.owner.gate.mutex.lockUncancelable(io);
        defer self.owner.gate.mutex.unlock(io);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
        self.owner.publication_stats.attempts +|= 1;
        self.owner.publication_stats.replay_attempts +|= 1;
        errdefer {
            self.owner.publication_stats.refused +|= 1;
            self.owner.publication_stats.replay_refused +|= 1;
        }
        var lease = try self.owner.acquireMaintenancePathLeaseAssumeLocked();
        const held_from = nowNs();
        defer MaintenanceHoldSites.record(&self.owner.publication_stats.maintenance_hold_sites.publish_replay, held_from);
        defer lease.close();
        const database = try self.current();
        // Computed resolver effects must explicitly carry their read set.
        var guards: DocumentGuards = .{ .arena = std.heap.ArenaAllocator.init(alloc) };
        defer guards.arena.deinit();
        self.document_guards = &guards;
        defer self.document_guards = null;
        // Enqueue-only replay has no computed artifact writes to fence.
        if (artifact_keys.len > 0 and inputs == null) return error.EnrichmentWaitCanceled;
        if (inputs) |read_set| {
            var live_inputs = try database.core.store.beginReadTxn();
            defer live_inputs.abort();
            try read_set.validate(&live_inputs);
        }
        // Release read pins before writing. The gate and path lease still
        // fence every input until the commit below has completed.
        const journal = @import("../storage/db/derived/change_journal.zig");
        var record = try journal.recordFromDerivedBatch(alloc, batch, 0);
        defer journal.deinitRecord(alloc, &record);
        // Projection-only documents and direct vectors need their source
        // guards too, even when the journal omits them from changed_doc_keys.
        for (batch.documents) |document| if (document.action != .upsert) try self.guardDocument(database, document.key);
        for (batch.dense_embeddings) |embedding| try self.guardDocument(database, embedding.parent_doc_key orelse embedding.doc_key);
        for (batch.sparse_embeddings) |embedding| try self.guardDocument(database, embedding.doc_key);
        for (record.changed_doc_keys) |key| try self.guardDocument(database, key);
        for (record.deleted_doc_keys) |key| try self.guardDocument(database, key);
        for (record.overwritten_doc_keys) |key| try self.guardDocument(database, key);
        for (record.changed_artifact_keys) |key| try self.guardReplayKey(database, key, inputs);
        for (artifact_keys) |key| try self.guardReplayKey(database, key, inputs);
        const sequence = try apply(request, database);
        // The physical header alone does not update the detached DocStore's
        // committed/reserved sequence caches. Only mirror successful commits.
        self.refreshSnapshotCheckpoint();
        self.snapshot_database.core.store.observeExternalReplayCommit(sequence);
        self.owner.notifyMutation();
        // A changed index input cancels subsequent detached index work, not
        // this already committed primary record. Report its durable success.
        return sequence;
    }

    fn acquireStore(ptr: *anyopaque, fence: ?WriteFence, now_ms: u64) !StoreLease {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        // This lease's whole point is that the caller then writes through
        // the returned store directly (`StoreLease.store.beginWrite`), which
        // independently reenters writer admission for `self.snapshot_root`
        // through the exact same `Owner.enterMaintenanceWriter` below. A
        // separate, uncoordinated `self.owner.gate.mutex.lockUncancelable`
        // here (as this used to do) would hold that non-reentrant queued
        // mutex across the nested call's own attempt to acquire it --
        // deadlocking this call against itself (capi "detached maintenance
        // publishes through native storage admission" regression). Routing
        // through `enterMaintenanceWriter` instead establishes
        // `root.maintenance_writer_references`, which is exactly what lets
        // that nested reentry see admission already held and skip the queue.
        try Owner.enterMaintenanceWriter(self.snapshot_root, .primary);
        errdefer Owner.leaveMaintenanceWriter(self.snapshot_root);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
        self.owner.publication_stats.attempts +|= 1;
        self.owner.publication_stats.generated_attempts +|= 1;
        errdefer {
            self.owner.publication_stats.refused +|= 1;
            self.owner.publication_stats.generated_refused +|= 1;
        }
        const database = try self.current();
        if (fence) |expected| {
            var ownership = try ProducerLease.init(alloc, database.core.store, expected.lease_key);
            defer ownership.deinit();
            const live = (try ownership.load(alloc)) orelse return error.EnrichmentLeaseFenceLost;
            defer alloc.free(live.owner_id);
            if (live.epoch != expected.epoch or live.expires_at_ms <= @max(now_ms, database.backend_runtime.clock().nowRealtimeMs()) or !std.mem.eql(u8, live.owner_id, expected.owner_id)) return error.EnrichmentLeaseFenceLost;
        }
        if (database.enrichment_runtime == null) try database.reconfigureEnrichmentRuntimePaused(.{ .enable_without_producers = true });
        const held = try alloc.create(HeldStore);
        held.* = .{ .publication = self, .database = database, .held_from = nowNs() };
        return .{ .store = database.enrichment_runtime.?.store, .ptr = held, .release = HeldStore.release, .guard_key = HeldStore.guardKey };
    }

    fn heartbeat(ptr: *anyopaque, fence: WriteFence, now_ms: u64, ttl_ms: u64) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const io = h.handleLockIo();
        self.owner.gate.mutex.lockUncancelable(io);
        defer self.owner.gate.mutex.unlock(io);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
        var path_lease = try self.owner.acquireMaintenancePathLeaseAssumeLocked();
        const held_from = nowNs();
        defer MaintenanceHoldSites.record(&self.owner.publication_stats.maintenance_hold_sites.heartbeat, held_from);
        defer path_lease.close();
        const database = try self.current();
        var lease = try ProducerLease.init(alloc, database.core.store, fence.lease_key);
        defer lease.deinit();
        return lease.renewFenced(fence.owner_id, fence.epoch, now_ms, ttl_ms);
    }
};

// Foreign threads can immediately reenter after a read. Ticket ordering keeps
// a stream of short reads from starving queued writers and maintenance.
const QueuedMutex = struct {
    state: std.Io.Mutex = .init,
    ready: @import("antfly_platform").sync.BroadcastCondition = .init,
    next: u64 = 0,
    serving: u64 = 0,
    /// A writer lease handed from one holder to the next queued thread so
    /// back-to-back calls skip the release/reacquire and the queue round trip.
    /// Owned by the current holder. `unlock` closes it once nobody is
    /// waiting, so it can never outlive the threads it was kept for.
    retained: ?native.PathWriterLock = null,
    /// Tickets of queued threads that will take the path lease as soon as
    /// they hold the gate (see `lockForLease`). Bounded; overflow only means
    /// a handoff is skipped.
    lease_tickets: [16]u64 = undefined,
    /// The Store/NativeFile each `lease_tickets` entry's caller will use,
    /// same index (WS2). Two Connections to the same path may each own a
    /// separate Owner/Store, sharing only this `PathGate` for the kernel
    /// lease; a same-process queue position alone does not mean the
    /// waiter's writes funnel through the same native commit queue as the
    /// current holder's, so `retainForWaiter` checks this too before
    /// handing off a lease whose write may not be durable yet. `null`
    /// (identity unknown) never matches.
    lease_ticket_stores: [16]?*anyopaque = undefined,
    lease_ticket_count: usize = 0,

    /// Upper bound on one chain of handoffs. Readers in other processes are
    /// not queued, so this is what bounds their wait behind a busy process.
    pub const retain_quantum_ns: i96 = 50 * std.time.ns_per_ms;
    /// Turn length when a writer in another process is queued. Each handoff
    /// between processes makes the next holder refresh its runtime, so a short
    /// turn amortizes that cost while keeping every queued wait bounded.
    pub const contended_quantum_ns: i96 = 20 * std.time.ns_per_ms;

    /// Caller holds the mutex. Keeps `lease` for the next queued thread when
    /// that is cheaper and fair; otherwise returns false and the caller closes.
    /// `store` identifies the calling side's own Store/NativeFile (WS2):
    /// the handoff only proceeds when the next holder registered
    /// (`lockForLease`) with this same identity, since otherwise its
    /// writes do not funnel through this process's single native commit
    /// queue and may stage against a tip this call's own write has not
    /// yet made durable. Pass `null` only when the identity is genuinely
    /// unavailable; that always refuses the handoff.
    pub fn retainForWaiter(self: *QueuedMutex, io: std.Io, lease: *native.PathWriterLock, store: ?*anyopaque) bool {
        if (self.retained != null) return false;
        const age = std.Io.Clock.awake.now(io).nanoseconds - lease.acquired_ns;
        if (age >= retain_quantum_ns) return false;
        {
            self.state.lockUncancelable(io);
            defer self.state.unlock(io);
            // Hand off only to the very next holder, and only if it is about
            // to take the lease. Anyone else might need the kernel lock free.
            const next_holder = self.serving +% 1;
            if (self.next == next_holder) return false;
            const at = std.mem.indexOfScalar(u64, self.lease_tickets[0..self.lease_ticket_count], next_holder) orelse return false;
            if (store == null or self.lease_ticket_stores[at] != store) return false;
        }
        if (age >= contended_quantum_ns and lease.othersWaiting()) return false;
        if (lease.reader_registration_batch) |batch| batch.end();
        lease.reader_registration_batch = null;
        lease.endHold();
        self.retained = lease.*;
        return true;
    }

    /// Caller holds the mutex.
    pub fn takeRetained(self: *QueuedMutex) ?native.PathWriterLock {
        const lease = self.retained orelse return null;
        self.retained = null;
        return lease;
    }

    /// Caller holds the mutex and needs the kernel lease free for itself
    /// (for example a reader or a nonblocking writer attempt).
    pub fn dropRetained(self: *QueuedMutex) void {
        if (self.takeRetained()) |lease| {
            var owned = lease;
            owned.close();
        }
    }

    pub fn lockUncancelable(self: *QueuedMutex, io: std.Io) void {
        self.state.lockUncancelable(io);
        defer self.state.unlock(io);
        const ticket = self.next;
        self.next +%= 1;
        while (ticket != self.serving) self.ready.waitUncancelable(io, &self.state);
    }

    /// Like `lockUncancelable`, for a caller that takes the path lease next,
    /// so the current holder may hand its lease over directly. `store`
    /// identifies this caller's own Store/NativeFile (WS2): see
    /// `retainForWaiter`. Pass `null` if genuinely unavailable -- that
    /// Store can still be handed off to when nobody is marked for lease
    /// handoff at all, just never through the early, possibly-undurable
    /// `retainForWaiter` path.
    pub fn lockForLease(self: *QueuedMutex, io: std.Io, store: ?*anyopaque) void {
        self.state.lockUncancelable(io);
        defer self.state.unlock(io);
        const ticket = self.next;
        self.next +%= 1;
        const marked = self.lease_ticket_count < self.lease_tickets.len;
        if (marked) {
            self.lease_tickets[self.lease_ticket_count] = ticket;
            self.lease_ticket_stores[self.lease_ticket_count] = store;
            self.lease_ticket_count += 1;
        }
        while (ticket != self.serving) self.ready.waitUncancelable(io, &self.state);
        if (marked) {
            const tickets = self.lease_tickets[0..self.lease_ticket_count];
            const at = std.mem.indexOfScalar(u64, tickets, ticket).?;
            tickets[at] = tickets[tickets.len - 1];
            self.lease_ticket_stores[at] = self.lease_ticket_stores[self.lease_ticket_count - 1];
            self.lease_ticket_count -= 1;
        }
    }

    pub fn tryLock(self: *QueuedMutex) bool {
        if (!self.state.tryLock()) return false;
        defer self.state.unlock(h.handleLockIo());
        if (self.next != self.serving) return false;
        self.next +%= 1;
        return true;
    }

    pub fn unlock(self: *QueuedMutex, io: std.Io) void {
        self.state.lockUncancelable(io);
        self.serving +%= 1;
        // Close before any new holder can run, so it finds the sidecar free.
        if (self.next == self.serving) self.dropRetained();
        self.ready.broadcast(io);
        self.state.unlock(io);
    }
};

fn acquireMaintenanceCapture(gate: *QueuedMutex, misses: *u8) bool {
    if (!gate.tryLock()) {
        misses.* +|= 1;
        if (misses.* < 3) return false;
        gate.lockUncancelable(h.handleLockIo());
    }
    misses.* = 0;
    return true;
}

pub fn testMaintenanceCaptureAdmission() !void {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const io = h.handleLockIo();
    var gate: QueuedMutex = .{};
    var misses: u8 = 0;
    gate.lockUncancelable(io);
    var held = true;
    defer if (held) gate.unlock(io);
    try std.testing.expect(!acquireMaintenanceCapture(&gate, &misses));
    try std.testing.expect(!acquireMaintenanceCapture(&gate, &misses));
    const Background = struct {
        acquired: std.Io.Event = .unset,
        fn run(self: *@This(), mutex: *QueuedMutex, count: *u8) void {
            if (acquireMaintenanceCapture(mutex, count)) {
                self.acquired.set(h.handleLockIo());
                mutex.unlock(h.handleLockIo());
            }
        }
    };
    var background: Background = .{};
    const thread = try std.Thread.spawn(.{}, Background.run, .{ &background, &gate, &misses });
    defer thread.join();
    // Observe the queued ticket, rather than relying on a timing sleep. Keep
    // the initial caller active until maintenance has joined its queue.
    while (true) {
        gate.state.lockUncancelable(io);
        const queued = gate.next == 2;
        gate.state.unlock(io);
        if (queued) break;
        std.Thread.yield() catch {};
    }
    gate.unlock(io);
    held = false;
    // An immediately reentering foreground caller must follow maintenance.
    gate.lockUncancelable(io);
    defer gate.unlock(io);
    try std.testing.expect(background.acquired.isSet());
    try std.testing.expectEqual(@as(u8, 0), misses);
}

test "capi path gate hands a writer lease to the next queued thread and drops it when idle" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threaded.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/handoff.aflite", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const io = h.handleLockIo();
    var gate: QueuedMutex = .{};
    // This test exercises the generic handoff mechanism, not cross-Store
    // identity (WS2), so every side here shares one dummy identity.
    var dummy_store: u8 = 0;
    const same_store: ?*anyopaque = &dummy_store;

    // Nobody queued: the lease is not kept.
    gate.lockUncancelable(io);
    var lone = try native.fairWriterLockWithIo(alloc, io, threaded.io(), path, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s);
    try std.testing.expect(!gate.retainForWaiter(io, &lone, same_store));
    lone.close();
    gate.unlock(io);

    const Plain = struct {
        fn run(g: *QueuedMutex) void {
            const wio = h.handleLockIo();
            g.lockUncancelable(wio);
            g.unlock(wio);
        }
    };
    {
        // The next holder will not take a lease: no handoff.
        gate.lockUncancelable(io);
        var lease = try native.fairWriterLockWithIo(alloc, io, threaded.io(), path, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s);
        const plain = try std.Thread.spawn(.{}, Plain.run, .{&gate});
        while (true) {
            gate.state.lockUncancelable(io);
            const queued = gate.next -% gate.serving;
            gate.state.unlock(io);
            if (queued > 1) break;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expect(!gate.retainForWaiter(io, &lease, same_store));
        lease.close();
        gate.unlock(io);
        plain.join();
    }

    const Waiter = struct {
        gate: *QueuedMutex,
        store: ?*anyopaque,
        took: bool = false,
        fn run(self: *@This()) void {
            const wio = h.handleLockIo();
            self.gate.lockForLease(wio, self.store);
            defer self.gate.unlock(wio);
            if (self.gate.takeRetained()) |kept| {
                var lease = kept;
                self.took = true;
                lease.close();
            }
        }
    };
    gate.lockUncancelable(io);
    var lease = try native.fairWriterLockWithIo(alloc, io, threaded.io(), path, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s);
    var waiter: Waiter = .{ .gate = &gate, .store = same_store };
    const thread = try std.Thread.spawn(.{}, Waiter.run, .{&waiter});
    while (true) {
        gate.state.lockUncancelable(io);
        const queued = gate.next -% gate.serving;
        gate.state.unlock(io);
        if (queued > 1) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(gate.retainForWaiter(io, &lease, same_store));
    gate.unlock(io);
    thread.join();
    try std.testing.expect(waiter.took);
    try std.testing.expect(gate.retained == null);

    // A kept lease nobody claims is closed by the last unlock.
    gate.lockUncancelable(io);
    gate.retained = try native.fairWriterLockWithIo(alloc, io, threaded.io(), path, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s);
    gate.unlock(io);
    try std.testing.expect(gate.retained == null);
    var probe = try native.tryFairWriterLockWithIo(alloc, io, path);
    probe.close();
}

test "capi path gate never hands an early lease to a waiter on a different Store" {
    // WS2 regression: two Connections to the same path can each own a
    // separate Owner/Store (the "separate handles" scenario), sharing only
    // this PathGate for the kernel lease. A same-process queue position on
    // gate.mutex alone used to be treated as proof the waiter's writes
    // funnel through the handing-off side's native commit queue -- true
    // only when they share a Store. retainForWaiter must refuse the early
    // handoff whenever the identities differ (or either is unknown), even
    // though the generic "someone is queued for the lease" condition that
    // retainForWaiter otherwise checks is satisfied.
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threaded.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/handoff-cross-store.aflite", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const io = h.handleLockIo();
    var gate: QueuedMutex = .{};
    var holder_store: u8 = 0;
    var waiter_store: u8 = 0;

    const Waiter = struct {
        gate: *QueuedMutex,
        store: ?*anyopaque,
        took: bool = false,
        fn run(self: *@This()) void {
            const wio = h.handleLockIo();
            self.gate.lockForLease(wio, self.store);
            defer self.gate.unlock(wio);
            if (self.gate.takeRetained()) |kept| {
                var lease = kept;
                self.took = true;
                lease.close();
            }
        }
    };
    for ([_]?*anyopaque{ &waiter_store, null }) |store_case| {
        gate.lockUncancelable(io);
        var lease = try native.fairWriterLockWithIo(alloc, io, threaded.io(), path, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s);
        var waiter: Waiter = .{ .gate = &gate, .store = store_case };
        const thread = try std.Thread.spawn(.{}, Waiter.run, .{&waiter});
        while (true) {
            gate.state.lockUncancelable(io);
            const queued = gate.next -% gate.serving;
            gate.state.unlock(io);
            if (queued > 1) break;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        // The waiter is genuinely queued and marked for lease handoff, but
        // its identity (&waiter_store, or unknown) never matches the
        // holder's (&holder_store): no early handoff.
        try std.testing.expect(!gate.retainForWaiter(io, &lease, &holder_store));
        lease.close();
        gate.unlock(io);
        thread.join();
        try std.testing.expect(!waiter.took);
    }
}

// Connections in one process queue on a canonical file gate. The kernel
// sidecar remains the authority between processes and with CLI/server owners.
const PathGate = struct {
    path: []u8,
    mutex: QueuedMutex = .{},
    sql_preparation: @import("../sql/preparation_admission.zig").PreparationAdmission = .{},
    references: usize = 1,
    owners: std.ArrayList(*Owner) = .empty,

    /// Finds a writer-mode `Owner` sharing this gate (any `Connection` to
    /// the same path -- including a different Owner than the caller's
    /// own, per `compatible`'s own open-mode split) whose `WriterLane`
    /// currently holds this process's one cross-process writer lease
    /// (actively checked out, or merely parked between writes --
    /// lite-writer-authority.md W2 keeps that lease's kernel lock open
    /// while parked, so back-to-back writers skip the kernel round
    /// trip), and pins it there for the lifetime of one synthetic read.
    ///
    /// Lets a reader's legacy (non-region) kernel lock attempt -- taken
    /// only when `shared_lock_region.Region.open` itself reports the
    /// region unusable for this call, e.g. a stale `db_inode` stamp left
    /// by a vacuum's generation swap racing this process's own still-live
    /// region peer (`ensureNoLivePeer`'s own doc comment) -- tell a
    /// same-process self-collision apart from a genuinely foreign
    /// writer's. Only the kernel can see the former as a lock conflict;
    /// in-process state is the only way to know it is not one.
    ///
    /// A pin, not just a point-in-time check, matters: see
    /// `WriterLane.local_reader_activity`'s own doc comment for why
    /// nothing would otherwise stop the lease moving to a foreign writer
    /// (or this lane being freed outright) while the synthetic read is
    /// still in flight.
    ///
    /// Caller holds `self.mutex` (`acquirePathLeaseAssumeLocked`'s own
    /// precondition); `Connection.close()`'s own last-connection teardown
    /// removes an `Owner` from `self.owners` under the same mutex before
    /// ever calling `Owner.deinit` (which is what actually frees it), so
    /// every candidate this iterates is guaranteed live for the whole
    /// call -- a reader whose writer owner is already gone never finds
    /// it here. This takes each candidate's own `writer_lane.mutex` in
    /// turn, never nested with another one, so it cannot cycle against
    /// `WriterLane.checkout`'s own sequential (never nested) hold of the
    /// two mutexes.
    fn pinLocalWriterLeaseForRead(self: *PathGate) ?*WriterLane {
        for (self.owners.items) |owner| {
            if (!h.liteOpenModeCanWrite(owner.options.open_mode)) continue;
            if (owner.writer_lane.pinForLocalRead()) return &owner.writer_lane;
        }
        return null;
    }
};
var registry_mutex: std.Io.Mutex = .init;
var gates: std.StringHashMapUnmanaged(*PathGate) = .empty;

/// File replacement must join the same fair queue as foreground operations
/// and maintenance before acquiring the cross-process writer lease. Retaining
/// the gate also keeps its identity stable when the last connection closes.
pub const PathReplacementGuard = struct {
    gate: *PathGate,

    pub fn release(self: PathReplacementGuard) void {
        self.gate.mutex.unlock(h.handleLockIo());
        releaseGate(self.gate);
    }
};

pub fn acquirePathReplacement(path: []const u8) !PathReplacementGuard {
    const io = h.handleLockIo();
    const canonical = native.realPathAlloc(alloc, io, path) catch |err| switch (err) {
        error.FileNotFound => blk: {
            const parent = try native.realPathAlloc(alloc, io, std.fs.path.dirname(path) orelse ".");
            defer alloc.free(parent);
            break :blk try std.fs.path.join(alloc, &.{ parent, std.fs.path.basename(path) });
        },
        else => return err,
    };
    defer alloc.free(canonical);
    const gate = try retainGate(canonical);
    gate.mutex.lockUncancelable(io);
    // Replacement takes the kernel lease itself; a lease kept for this
    // thread would otherwise block it.
    gate.mutex.dropRetained();
    return .{ .gate = gate };
}

pub fn testPathReplacementAdmission() !void {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replacement.aflite", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const io = h.handleLockIo();
    // Also exercise a destination that does not exist yet: creation and later
    // replacement must use one canonical queue, including relative aliases.
    const held = try acquirePathReplacement(path);
    var held_live = true;
    defer if (held_live) held.release();
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    file.close(io);
    const canonical = try native.realPathAlloc(alloc, io, path);
    defer alloc.free(canonical);
    const Replacement = struct {
        acquired: bool = false,
        failure: ?anyerror = null,
        finished: std.Io.Event = .unset,
        fn run(self: *@This(), target: []const u8, expected: *PathGate) void {
            defer self.finished.set(h.handleLockIo());
            const replacement = acquirePathReplacement(target) catch |err| {
                self.failure = err;
                return;
            };
            defer replacement.release();
            self.acquired = replacement.gate == expected;
        }
    };
    var replacement: Replacement = .{};
    const thread = try std.Thread.spawn(.{}, Replacement.run, .{ &replacement, canonical, held.gate });
    defer thread.join();
    while (true) {
        held.gate.mutex.state.lockUncancelable(io);
        const queued = held.gate.mutex.next == 2;
        held.gate.mutex.state.unlock(io);
        if (queued) break;
        // A failed registration or a different canonical gate must fail the
        // regression instead of leaving it waiting for an impossible ticket.
        try std.testing.expect(!replacement.finished.isSet());
        std.Thread.yield() catch {};
    }
    held.release();
    held_live = false;
    // The following foreground operation must queue behind replacement.
    const next = try acquirePathReplacement(path);
    defer next.release();
    try std.testing.expect(replacement.failure == null);
    try std.testing.expect(replacement.acquired);
}

fn retainGate(path: []const u8) !*PathGate {
    const io = h.handleLockIo();
    registry_mutex.lockUncancelable(io);
    defer registry_mutex.unlock(io);
    if (gates.get(path)) |gate| {
        gate.references += 1;
        return gate;
    }
    const gate = try alloc.create(PathGate);
    errdefer alloc.destroy(gate);
    gate.* = .{ .path = try alloc.dupe(u8, path) };
    errdefer alloc.free(gate.path);
    try gates.put(alloc, gate.path, gate);
    return gate;
}
fn releaseGate(gate: *PathGate) void {
    const io = h.handleLockIo();
    registry_mutex.lockUncancelable(io);
    defer registry_mutex.unlock(io);
    gate.references -= 1;
    if (gate.references != 0) return;
    _ = gates.remove(gate.path);
    std.debug.assert(gate.owners.items.len == 0);
    gate.owners.deinit(alloc);
    alloc.free(gate.path);
    alloc.destroy(gate);
    if (gates.count() == 0) {
        gates.deinit(alloc);
        gates = .empty;
    }
}

// Compatible connections borrow one persistent generation and one maintenance
// worker. SQL sessions, cursor IDs, and child handles remain connection-local.
/// Where a background turn holds the writer lease.
pub const MaintenanceHoldSites = struct {
    pub const Site = struct { count: u64 = 0, ns: u64 = 0, max_ns: u64 = 0 };
    writer_primary: Site = .{},
    writer_index: Site = .{},
    writer_reclamation: Site = .{},
    writer_artifact_lease: Site = .{},
    writer_allocator_service: Site = .{},
    publish_replay: Site = .{},
    generated_commit: Site = .{},
    generated_store: Site = .{},
    heartbeat: Site = .{},
    /// A maintenance turn's capture under the lease, until it is released.
    turn_capture: Site = .{},
    /// Time inside `turn_capture`, by step (total ns).
    capture_steps: CaptureSteps = .{},
    storage_turn: Site = .{},
    idle_adopt: Site = .{},

    pub fn record(site: *Site, started_ns: i96) void {
        const elapsed: u64 = @intCast(@max(0, std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds - started_ns));
        site.count +|= 1;
        site.ns +|= elapsed;
        site.max_ns = @max(site.max_ns, elapsed);
    }

    pub fn writer(self: *MaintenanceHoldSites, scope: PublicationScope) *Site {
        return switch (scope) {
            .primary => &self.writer_primary,
            .index => &self.writer_index,
            .reclamation => &self.writer_reclamation,
            .artifact_lease => &self.writer_artifact_lease,
            .allocator_service => &self.writer_allocator_service,
        };
    }
};

pub const CaptureSteps = struct {
    refresh_ns: u64 = 0,
    marker_sweep_ns: u64 = 0,
    recover_ns: u64 = 0,
    catalog_ns: u64 = 0,
    debt_checks_ns: u64 = 0,
    schedule_ns: u64 = 0,
    snapshot_ns: u64 = 0,
    publication_catalog_ns: u64 = 0,
};

/// Splits one held interval into consecutive steps.
const StepClock = struct {
    last: i96,
    fn mark(self: *StepClock, total: *u64) void {
        const now = nowNs();
        total.* +|= @intCast(@max(0, now - self.last));
        self.last = now;
    }
};

fn nowNs() i96 {
    return std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds;
}

/// Which durable-consumer kinds a background turn could currently do
/// anything useful for. See docs/design/lite-performance/lite-single-commit-write-path.md
/// §3.6 "No-consumer path". `registeredDemand()` computes this cheaply
/// (O(1) per table) from the catalog; once the commit path (write-path
/// rewrite, WS2/WS3) threads a write-specific mask through, `notifyDemand`
/// can be called with a precise mask instead of the conservative registry
/// snapshot `notifyMutation()` uses today.
pub const Demand = packed struct(u8) {
    index: bool = false,
    enrichment: bool = false,
    group_source: bool = false,
    ttl: bool = false,
    resolver: bool = false,
    /// Relational maintenance sweep (index build/repair, index GC, CHECK
    /// validation; `independent_maintenance.zig`'s `.index` Work) has a
    /// bounded page it could run right now. Lite-relational-one §3.3c/task 3:
    /// without this bit, a no-other-consumer table whose only backlog is
    /// relational maintenance never satisfies `registeredDemand().any()`,
    /// so `maintenanceTurn`'s no-consumer gating (see its "changed_matters"
    /// comment) could starve that backlog instead of just this owner's
    /// own separately-scheduled `independent_maintenance.Owner` noticing it.
    relational: bool = false,
    _reserved: u2 = 0,

    pub const none: Demand = .{};

    pub fn any(self: Demand) bool {
        return @as(u8, @bitCast(self)) != 0;
    }

    pub fn merge(self: Demand, other: Demand) Demand {
        return @bitCast(@as(u8, @bitCast(self)) | @as(u8, @bitCast(other)));
    }
};

/// WS3 writer lane (design doc §3.2/§3.2a): in-process, per-Store admission
/// for a writer-mode connection's mutation-intent calls. `PathGate`'s kernel
/// lease exists to exclude *other* processes (and, within one process,
/// other incompatible `Owner`s sharing this path); it buys same-process
/// callers sharing this `Owner`'s `Store` nothing, since their actual mutual
/// exclusion is already enforced by the DB apply lock and the Store's own
/// group-commit queue. This lane keeps the kernel lease parked for the next
/// same-Store writer -- unconditionally when one is *already* polling for
/// it (`checkin`'s `waiting != 0` case, zero cost), otherwise until a
/// genuinely foreign waiter (a different process, or a different
/// in-process `Owner`) has been queued past a short fairness quantum --
/// and releases it, always after a `Store.sync()` so the next holder sees
/// a durable tip. See `checkin`'s doc comment for why the foreign check
/// uses `PathWriterLock.othersWaitingAcrossProcesses`, not the plain
/// `othersWaiting`: this process's own maintenance worker competes for the
/// same kernel lease through the same region ticket ring as a genuinely
/// foreign waiter, and the two are indistinguishable from `othersWaiting`
/// alone -- `othersWaitingAcrossProcesses` tells them apart by the
/// process id the region's own ticket ring stamps on every ticket.
///
/// At most one writer is ever "checked out" of the lane at a time
/// (`checked_out`): `checkout`/`checkin` replace *when* the kernel lease
/// and `PathGate` are touched, not the DB-level exclusivity those already
/// provide. This removes the original register-before-release race: the
/// old design decided, in a single uncancellable instant right after
/// dropping the connection's own locks, whether a same-Store waiter was
/// already registered on `PathGate` -- and in practice almost always lost
/// that race, since a waiter typically had not yet gotten far enough to
/// register by then. Here, a new writer that finds nothing parked simply
/// polls (bounded by its own busy-timeout deadline) instead of needing to
/// have registered at one exact instant; the current holder's `checkin`
/// always parks for it as long as it is already polling (`waiting != 0`),
/// no matter when it started.
pub const WriterLane = struct {
    mutex: std.Io.Mutex = .init,
    /// The kernel lease, parked between writers: held by this process,
    /// not currently in use by any call. `null` either because nobody has
    /// ever established the lane yet, or because it was just handed off
    /// to a remote waiter.
    parked: ?native.PathWriterLock = null,
    /// True while exactly one writer is between `checkout` and the
    /// matching `checkin` -- establishing the lane from cold (its first
    /// `connection.acquire()`/`refresh()`), refreshing a parked lease, or
    /// (rarely) handing the parked lease off to a remote waiter.
    /// Serializes those cases against each other; it is not the DB-level
    /// write lock (the apply lock already provides that), only this
    /// lane's own bookkeeping.
    checked_out: bool = false,
    /// The lease belonging to the current `checked_out` holder, moved
    /// here between `checkout`'s success and the matching `checkin` --
    /// the two calls run from different points in the mutation commit
    /// path (WS3 releases the connection-level locks, this lane included,
    /// well before the call's own durability wait). Valid only while
    /// `checked_out` is true.
    held_lease: ?native.PathWriterLock = null,
    /// Same-process callers currently polling in `checkout`, waiting for
    /// `checked_out` to go false. Read by `checkin` to decide whether a
    /// local writer gets the parked lease unconditionally (zero cost, no
    /// `othersWaiting`/fairness check needed) rather than racing a remote
    /// handoff decision against them.
    waiting: usize = 0,
    /// Bumped (and futex-woken) whenever the lane becomes available:
    /// `checked_out` cleared or a lease parked. Waiters read it under
    /// `mutex` before sleeping on it, so a release between their check and
    /// their sleep cannot be missed.
    release_epoch: std.atomic.Value(u32) = .init(0),
    /// The root this lane handed out last, so a warm checkout can tell
    /// "nothing has replaced the generation since" by pointer identity
    /// alone -- see `checkout`'s fast path.
    last_root: ?*h.Handle = null,
    /// When `parked` was last set (by `checkinImpl`), so `maintainParkedLease`
    /// can tell how long it has sat with nobody checking it out at all.
    /// Refreshed on every park, including a same-process reuse's own
    /// checkout+checkin round trip -- back-to-back local writers keep
    /// resetting this exactly like they keep the lease itself warm.
    /// Meaningless while `parked` is `null`.
    parked_since_ns: i96 = 0,
    /// The exact `NativeStore` `checkinImpl` was given when it parked
    /// `parked` -- the same value `checkin`'s own immediate foreign/idle
    /// release already trusted unconditionally, with no root comparison
    /// at all. `maintainParkedLease` must sync through this store, not
    /// re-derive one from `last_root`/`Owner.current`: those can lag a
    /// same-process generation swap that happens while this lease sits
    /// parked, and syncing
    /// the *wrong* store -- or skipping the sync because the comparison
    /// came up stale -- leaves this write's buffered pages never pwritten
    /// before the lease is handed to the next holder, which reads a file
    /// missing them (observed live as `test_cross_process_writers_do_not
    /// _lose_each_others_rows` turning a resolved `FileBusy` into a
    /// silent `NotFound` once the lease could actually change hands).
    /// Meaningless while `parked` is `null`.
    parked_store: ?*NativeStore = null,
    /// `checkin` found a foreign process waiting past its fairness quantum
    /// but parked the lease instead of closing it: the checking-in writer
    /// is about to drive its own staged group, so the lease must outlive
    /// that commit. The next local checkout, or `maintainParkedLease`,
    /// drains the store and hands the lease over.
    foreign_release_due: bool = false,
    /// Set once `Owner.deinit` starts: nothing will hand a parked lease
    /// off again, so a lent lease's `close` releases it instead.
    closing: bool = false,
    /// lite-writer-authority.md W6: a plain read is using the current root
    /// without the writer lease. Writers wait for it, the same as for a
    /// checked-out writer; the read takes no kernel lock and leaves a
    /// parked lease parked.
    reading: bool = false,
    /// Thread that last checked the lane out; meaningful only while
    /// `checked_out`. Lets the commit leader tell a different writer still
    /// staging (worth waiting for) from its own unreleased checkout.
    holder: std.Thread.Id = 0,
    /// Same-process readers currently relying on this lane's kernel
    /// lease staying held through a *synthetic* (kernel-lock-free) lease
    /// of their own -- see `PathGate.pinLocalWriterLeaseForRead` and
    /// `Connection.acquirePathLeaseAssumeLocked`'s reader branch, which
    /// grants one only when the legacy (non-region) kernel lock attempt
    /// collides with exactly this lane's own held lease, never a foreign
    /// writer's. A point-in-time check that the lease was held at grant
    /// time is not enough: without a pin, nothing would stop
    /// `maintainParkedLease`'s idle/foreign release, `checkinImpl`'s own
    /// force-release, or `Owner.deinit`'s final teardown from giving the
    /// kernel lease to a foreign writer (or freeing this lane outright)
    /// while that synthetic read is still in flight -- letting a foreign
    /// writer reuse pages the read is still looking at. While this is
    /// nonzero: `maintainParkedLease` defers its release and retries
    /// shortly instead; `checkinImpl` parks instead of closing even on a
    /// forced release; and `Owner.deinit` waits for it to drain (under
    /// `self.mutex`, with `closing` already set so no new pin can start)
    /// before closing the parked/held lease and freeing this lane.
    local_reader_activity: @import("maintenance_activity.zig").Activity = .{},

    /// How long `maintainParkedLease` waits before checking again while
    /// `local_reader_activity` is nonzero -- short because a synthetic
    /// read is expected to be brief (a snapshot copy, not a long scan),
    /// and this only ever delays a release that was going to happen
    /// anyway, never blocks a reader's own progress.
    const local_reader_pin_retry_ns: i96 = 1 * std.time.ns_per_ms;

    const poll_initial_ns: i96 = 50 * std.time.ns_per_us;
    const poll_max_ns: i96 = 2 * std.time.ns_per_ms;
    /// Floor under any *bounded* wait for *this same process's* own
    /// `checked_out` span to end (another local caller mid-staging, about
    /// to `checkin`), for a caller with an explicit nonzero
    /// `busy_timeout_ms`: `@max(deadline, now + min_local_wait_ns)` never
    /// shortens that caller's own budget, it only ever extends a smaller
    /// one. See `checkout`'s own `unconditional_wait` for the `0` case --
    /// this floor by itself is not a substitute for that: the staging
    /// window it is sized for (a few `poll_max_ns` ticks, not hundreds of
    /// milliseconds -- an uncapped-rate caller retrying this same path in a
    /// tight loop turns every extra millisecond here into real, same-sized
    /// slowdown across every such call site) is routinely exceeded under
    /// genuine same-process write concurrency (issue1015: 32 concurrent
    /// writers plus background full-text catch-up measured ordinary
    /// `checkin`s 4-9ms away, an order of magnitude past this floor, and
    /// occasionally far more), so relying on it alone to bound the `0`
    /// case turns ordinary contention into a spurious `FileBusy` for a
    /// condition that is never actually externally contended.
    const min_local_wait_ns: i96 = 4 * std.time.ns_per_ms;

    /// Whether `connection.refresh(root, intent)` might actually need to
    /// do something (the detached allocator-proof admission branch) for
    /// `root`, cheaply (no disk I/O beyond an in-memory allocator-stats
    /// read) -- the same condition `Connection.refresh` itself gates that
    /// branch on. Used to decide whether `checkout`'s fast path may skip
    /// calling `refresh` at all.
    fn mightNeedRefreshAdmission(connection: *Connection, root: *h.Handle, intent: api.StorageIntent) bool {
        if (connection.options.profile != .native or connection.options.open_mode != .writer) return false;
        if (Connection.usesCapacityAdmission(root)) return true;
        if (intent != .mutation) return false;
        return connection.needsReuseAdmission(root) catch true;
    }

    /// Check `connection` into the lane, returning the root to use, with
    /// `connection` already attached to it and that root's `lane_pins`
    /// incremented (the caller -- `enterCapturedHandleWithDeadline` --
    /// must decrement it exactly once, in `leaveWithRegistry`, once this
    /// call's own durability wait has resolved).
    ///
    /// Design doc §3.2/§3.2a "no self-replacement": while this lane holds
    /// the kernel lease continuously, no other *process* can have
    /// written, and `connection.owner.current` not having moved means no
    /// same-process replacement (`Owner.refresh`'s slow path, the only
    /// thing that ever reassigns it) has happened either -- by
    /// definition, not by inference, so there is nothing a fresh disk
    /// probe could tell this call that it does not already know. Skipping
    /// `connection.refresh` entirely in that case (keeping only its cheap,
    /// in-memory admission check -- `mightNeedRefreshAdmission` -- as a
    /// safety valve for capacity-limited or reclaiming stores) is what
    /// stops back-to-back same-Store commits from ever reaching
    /// `Owner.refresh`'s probe-based "unchanged" comparison at all, which
    /// is what was spuriously taking the slow replacement path under
    /// genuine concurrent overlap (a commit's on-disk header can lag its
    /// own in-memory `current.owned_lite_backend.?.native_docstore.?.file`
    /// by the time a concurrently-admitted writer's probe runs, even
    /// though nothing external changed) -- observed empirically as a
    /// `metadata_pool`/`source_certificates` crash from a generation torn
    /// down while another writer was still pinned on it.
    pub fn checkout(self: *WriterLane, connection: *Connection, intent: api.StorageIntent) !*h.Handle {
        const io = h.handleLockIo();
        const deadline = connection.admissionDeadline();
        // See `min_local_wait_ns`'s doc comment: the "someone else is
        // checked out" poll below only ever waits on this same process's
        // own lane, never a foreign holder, so it floors `deadline` rather
        // than using it directly.
        const local_wait_deadline = @max(deadline, std.Io.Clock.awake.now(io).nanoseconds + min_local_wait_ns);
        // No busy timeout was actually configured for this call (the
        // ordinary default): `connection.admissionDeadline()` still
        // synthesizes one (`now + 0`), but `checked_out`/`parked` are pure
        // in-process bookkeeping -- nothing external ever contends for
        // them (same invariant `Connection.refresh`'s allocator-proof wait
        // already relies on for its own `self.owner.allocator_proof_mutex.
        // lockUncancelable` when `admission_deadline_ns == null and
        // busy_timeout_ms == 0`) -- so there is no genuinely foreign
        // holder to fail fast against here. Wait this same process's own
        // imminent `checkin` out unconditionally instead of failing once
        // `min_local_wait_ns`'s fixed floor elapses: that floor is sized
        // for a staging window measured in microseconds and is routinely
        // exceeded under real concurrent write load (issue1015), which
        // turned ordinary same-process contention into a spurious
        // `FileBusy` no caller's `busy_timeout_ms` could have prevented
        // (`0` already means "no timeout", not "a 4ms one"). A caller that
        // *did* set an explicit nonzero `busy_timeout_ms` is unaffected:
        // `local_wait_deadline` still governs that case exactly as before.
        const unconditional_wait = connection.admission_deadline_ns == null and connection.options.busy_timeout_ms == 0;
        var backoff_ns: i96 = poll_initial_ns;
        self.mutex.lockUncancelable(io);
        while (true) {
            // A same-process synthetic reader (`pinForLocalRead`) is
            // relying on this exact parked lease staying held -- see
            // `local_reader_activity`'s own doc comment. Defer the
            // foreign release until it drains; fall through to the
            // ordinary "take the parked lease" branch below instead,
            // which keeps the kernel lease open (just reused by this
            // local writer) rather than closing it.
            if (!self.reading and self.parked != null and self.foreign_release_due and self.local_reader_activity.count == 0) {
                var overdue = self.parked.?;
                const overdue_store = self.parked_store;
                self.parked = null;
                self.parked_store = null;
                self.foreign_release_due = false;
                self.mutex.unlock(io);
                if (overdue_store) |held_store| {
                    held_store.ensureDurableForReuse();
                    held_store.sync() catch {};
                }
                overdue.close();
                self.mutex.lockUncancelable(io);
                continue;
            }
            if (if (self.reading) null else self.parked) |parked| {
                self.parked = null;
                self.checked_out = true;
                self.holder = std.Thread.getCurrentId();
                self.holder = std.Thread.getCurrentId();
                const likely_root = self.last_root;
                self.mutex.unlock(io);
                const take_fast_path = likely_root != null and likely_root == connection.owner.current and
                    !mightNeedRefreshAdmission(connection, likely_root.?, intent);
                if (take_fast_path) {
                    const root = likely_root.?;
                    connection.current = root;
                    connection.attach(root);
                    _ = root.lane_pins.fetchAdd(1, .release);
                    self.mutex.lockUncancelable(io);
                    self.held_lease = parked;
                    self.mutex.unlock(io);
                    return root;
                }
                // Slow path: either this is the first warm checkout after
                // a cold start whose own `connection.refresh` never ran
                // yet for *this* connection (a different connection just
                // parked the lease), something (almost certainly
                // same-process maintenance) replaced the generation since
                // this lease was last used, or admission might be needed.
                // `refresh` can still briefly need `connection.gate.mutex`
                // itself (its detached allocator-proof wait toggles it);
                // hold it only for this call, not across the lease's whole
                // parked lifetime.
                connection.gate.mutex.lockUncancelable(io);
                var lease: ?native.PathWriterLock = parked;
                const root = connection.refresh(&lease, intent, true) catch |err| {
                    connection.gate.mutex.unlock(io);
                    if (lease) |*held| held.close();
                    self.mutex.lockUncancelable(io);
                    self.checked_out = false;
                    self.mutex.unlock(io);
                    self.signalReleased(io);
                    return err;
                };
                connection.gate.mutex.unlock(io);
                _ = root.lane_pins.fetchAdd(1, .release);
                self.mutex.lockUncancelable(io);
                self.held_lease = lease;
                self.last_root = root;
                self.mutex.unlock(io);
                return root;
            }
            if (!self.checked_out and !self.reading) {
                self.checked_out = true;
                self.holder = std.Thread.getCurrentId();
                self.holder = std.Thread.getCurrentId();
                self.mutex.unlock(io);
                // Cold path: nothing parked yet. Do the full admission
                // dance exactly as a non-lane call would, then keep the
                // result in the lane instead of handing it back per-call.
                // `acquireForOwnCheckout`, not plain `acquire`: this call
                // already holds `checked_out` for *this* checkout (just
                // set, above) and already knows `self.parked` is `null`
                // (the lane's own `while` loop above took the
                // `self.parked != null` branch instead whenever it was
                // not) -- going through the ordinary `acquire` path would
                // borrow from this lane (`borrowLease`) and wait on the
                // very checkout only its own return can ever end.
                var lease: ?native.PathWriterLock = connection.acquireForOwnCheckout() catch |err| {
                    self.mutex.lockUncancelable(io);
                    self.checked_out = false;
                    self.mutex.unlock(io);
                    self.signalReleased(io);
                    return err;
                };
                const root = connection.refresh(&lease, intent, true) catch |err| {
                    if (lease) |*held| held.close();
                    connection.gate.mutex.unlock(io);
                    self.mutex.lockUncancelable(io);
                    self.checked_out = false;
                    self.mutex.unlock(io);
                    self.signalReleased(io);
                    return err;
                };
                connection.gate.mutex.unlock(io);
                _ = root.lane_pins.fetchAdd(1, .release);
                self.mutex.lockUncancelable(io);
                self.held_lease = lease;
                self.last_root = root;
                self.mutex.unlock(io);
                return root;
            }
            // Someone else is checked out (or mid cold-start/hand-off).
            // Poll, bounded by this call's own admission deadline, rather
            // than needing to have registered at one exact instant --
            // `checkin` checks `waiting` fresh every time, so arriving
            // late here is never fatal to getting the fast path.
            self.waiting += 1;
            const epoch = self.release_epoch.load(.acquire);
            self.mutex.unlock(io);
            const remaining = local_wait_deadline - std.Io.Clock.awake.now(io).nanoseconds;
            if (!unconditional_wait and remaining <= 0) {
                self.mutex.lockUncancelable(io);
                self.waiting -= 1;
                self.mutex.unlock(io);
                return error.FileBusy;
            }
            // Woken by `signalReleased`; the timeout only bounds a path
            // that frees the lane without signalling. With no busy timeout
            // configured (`unconditional_wait`), `remaining` is meaningless
            // (floored to `min_local_wait_ns`, already elapsed) -- just cap
            // the poll at `poll_max_ns` and keep going; see
            // `unconditional_wait`'s own doc comment for why that can never
            // wait on anything but this same process's own imminent
            // `checkin`.
            io.futexWaitTimeout(u32, &self.release_epoch.raw, epoch, .{ .duration = .{ .raw = .fromNanoseconds(@intCast(if (unconditional_wait) @min(backoff_ns, poll_max_ns) else @min(backoff_ns, remaining))), .clock = .awake } }) catch {};
            backoff_ns = @min(backoff_ns * 2, poll_max_ns);
            self.mutex.lockUncancelable(io);
            self.waiting -= 1;
        }
    }

    pub const MaintenanceCheckout = union(enum) {
        /// Another holder has the lane checked out.
        busy,
        /// The lane is now checked out to the caller; no lease is parked,
        /// so the caller acquires one and lends it via `lendLease`.
        fresh,
        /// The lane is checked out to the caller with its parked lease.
        parked: native.PathWriterLock,
        /// Like `parked`, but another process has waited past its fairness
        /// quantum: the caller drains, releases the lease, and acquires
        /// a fresh one through the fair queue.
        overdue: native.PathWriterLock,
    };

    /// lite-writer-authority.md W2: maintenance checks the lane out like
    /// any other writer instead of taking its lease around it.
    pub fn beginMaintenanceCheckout(self: *WriterLane) MaintenanceCheckout {
        const io = h.handleLockIo();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.checked_out or self.reading) return .busy;
        self.checked_out = true;
        self.holder = std.Thread.getCurrentId();
        const lease = self.parked orelse return .fresh;
        self.parked = null;
        self.parked_store = null;
        // A same-process synthetic reader is relying on this exact
        // parked lease staying held -- see `local_reader_activity`'s own
        // doc comment. Defer the foreign release until it drains: hand
        // this same lease to the local maintenance borrower instead
        // (`.parked`, never `.overdue`), which keeps it open rather than
        // closing it. `foreign_release_due` is left set so a later
        // checkout can still act on it once the pin drains.
        if (self.foreign_release_due and self.local_reader_activity.count == 0) {
            self.foreign_release_due = false;
            return .{ .overdue = lease };
        }
        return .{ .parked = lease };
    }

    /// Marks a lease this lane checked out so its `close` parks it back here.
    pub fn lendLease(self: *WriterLane, lease: *native.PathWriterLock) void {
        lease.return_to = .{ .ptr = self, .give_back = returnLease };
    }

    /// lite-writer-authority.md W6: take the lane's reader slot. Waits for
    /// an in-process writer (or another plain read) to finish, without a
    /// kernel lease; releases `gate` while waiting, as `borrowLease` does.
    pub fn beginRead(self: *WriterLane, owner: *Owner, gate: *QueuedMutex, deadline: i96, unconditional: bool) !void {
        const io = h.handleLockIo();
        var backoff_ns: i96 = poll_initial_ns;
        while (true) {
            self.mutex.lockUncancelable(io);
            if (!self.checked_out and !self.reading) {
                self.reading = true;
                self.mutex.unlock(io);
                return;
            }
            const epoch = self.release_epoch.load(.acquire);
            self.mutex.unlock(io);
            const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
            if (!unconditional and remaining <= 0) return error.FileBusy;
            const wait_ns = if (unconditional) @min(backoff_ns, poll_max_ns) else @min(backoff_ns, remaining);
            gate.unlock(io);
            io.futexWaitTimeout(u32, &self.release_epoch.raw, epoch, .{ .duration = .{ .raw = .fromNanoseconds(@intCast(wait_ns)), .clock = .awake } }) catch {};
            gate.lockUncancelable(io);
            backoff_ns = @min(backoff_ns * 2, poll_max_ns);
            if (owner.stopping.load(.acquire)) return error.Canceled;
        }
    }

    pub fn endRead(self: *WriterLane) void {
        const io = h.handleLockIo();
        self.mutex.lockUncancelable(io);
        std.debug.assert(self.reading);
        self.reading = false;
        self.mutex.unlock(io);
        self.signalReleased(io);
    }

    /// Pins this lane's kernel lease against release for the lifetime of
    /// one synthetic (kernel-lock-free) reader lease -- see
    /// `local_reader_activity`'s own doc comment. Returns `false` (no pin
    /// taken) unless the lease is actually held right now (checked out or
    /// parked) and this lane is not already closing: a lane with neither
    /// has no lease to protect, and `Owner.deinit` setting `closing`
    /// before it waits for existing pins to drain means a caller must
    /// never start a new one afterward, or that wait could complete while
    /// this one is still in flight.
    pub fn pinForLocalRead(self: *WriterLane) bool {
        const io = h.handleLockIo();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.closing or (!self.checked_out and self.parked == null)) return false;
        self.local_reader_activity.begin();
        return true;
    }

    /// Ends one `pinForLocalRead` pin.
    fn unpinLocalRead(self: *WriterLane) void {
        const io = h.handleLockIo();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.local_reader_activity.finish(io);
    }

    /// `PathWriterLock.ReturnTo.give_back` for a synthetic reader lease
    /// granted by `pinForLocalRead`: the lease itself carries no real
    /// resource (`file`/`region` both `null`), so there is nothing to
    /// release here beyond the pin itself.
    fn giveBackLocalReadPin(ptr: *anyopaque, lease: native.PathWriterLock) void {
        _ = lease;
        const self: *WriterLane = @ptrCast(@alignCast(ptr));
        self.unpinLocalRead();
    }

    /// Marks a synthetic reader lease so its `close` ends the pin that
    /// granted it. Caller already holds that pin (a successful
    /// `pinForLocalRead`).
    pub fn lendLocalReadPin(self: *WriterLane, lease: *native.PathWriterLock) void {
        lease.return_to = .{ .ptr = self, .give_back = giveBackLocalReadPin };
    }

    pub const Borrowed = struct {
        lease: native.PathWriterLock,
        source: enum { handoff, uncontended, queued },
    };

    /// lite-writer-authority.md W2: the one way a caller outside `checkout`
    /// gets this process's writer lease. Checks the lane out and lends its
    /// lease, reusing a parked or gate-retained one and acquiring through
    /// the fair queue only when this process holds none. Caller holds
    /// `gate`; it is released while another local holder has the lane,
    /// because a holder on `checkout`'s cold path takes the gate before it
    /// checks in. `unconditional` waits out a local holder with no
    /// deadline (see `checkout`); it never bounds a foreign wait.
    pub fn borrowLease(self: *WriterLane, owner: *Owner, gate: *QueuedMutex, deadline: i96, unconditional: bool, connection: ?*Connection) !Borrowed {
        const io = h.handleLockIo();
        var backoff_ns: i96 = poll_initial_ns;
        while (true) {
            if (gate.takeRetained()) |held| {
                var lease = held;
                // A retained lease is this process's only one. One already
                // lent keeps its lane checkout; otherwise take the lane.
                if (lease.return_to == null) switch (self.beginMaintenanceCheckout()) {
                    .fresh => self.lendLease(&lease),
                    .busy => {},
                    .parked, .overdue => |parked| {
                        var extra = parked;
                        extra.close();
                        self.lendLease(&lease);
                    },
                };
                return .{ .lease = lease, .source = .handoff };
            }
            switch (self.beginMaintenanceCheckout()) {
                .parked => |held| {
                    var lease = held;
                    self.lendLease(&lease);
                    owner.drainForReuse();
                    return .{ .lease = lease, .source = .handoff };
                },
                .overdue => |held| {
                    var lease = held;
                    // Drain only; see `releaseBorrowed`.
                    owner.drainForReuse();
                    lease.close();
                    break;
                },
                .fresh => break,
                .busy => {
                    const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
                    if (!unconditional and remaining <= 0) return error.FileBusy;
                    const wait_ns = if (unconditional) @min(backoff_ns, poll_max_ns) else @min(backoff_ns, remaining);
                    const epoch = self.release_epoch.load(.acquire);
                    gate.unlock(io);
                    io.futexWaitTimeout(u32, &self.release_epoch.raw, epoch, .{ .duration = .{ .raw = .fromNanoseconds(@intCast(wait_ns)), .clock = .awake } }) catch {};
                    gate.lockUncancelable(io);
                    backoff_ns = @min(backoff_ns * 2, poll_max_ns);
                    if (owner.stopping.load(.acquire)) return error.Canceled;
                },
            }
        }
        // The lane is checked out to this call and the process holds no
        // lease: acquire one through the fair queue and lend it.
        std.debug.assert(self.parked == null);
        if (comptime h.builtin.is_test) if (connection) |c| if (c.test_path_lease_busy) |hook| {
            // Tests observe the point where admission would start waiting:
            // only after a failed attempt with budget left.
            if (native.lockWriterPathWithIo(alloc, io, c.path)) |got| {
                var lease = got;
                owner.drainForReuse();
                self.lendLease(&lease);
                return .{ .lease = lease, .source = .uncontended };
            } else |err| {
                if ((err != error.WouldBlock and err != error.FileBusy) or deadline - std.Io.Clock.awake.now(io).nanoseconds <= 0) {
                    self.cancelCheckout();
                    return if (err == error.WouldBlock) error.FileBusy else err;
                }
                hook.notify(hook.ptr);
            }
        };
        var held = native.fairWriterLockWithIo(alloc, io, owner.path_lock_io.io(), owner.path, deadline) catch |err| {
            self.cancelCheckout();
            return err;
        };
        owner.drainForReuse();
        self.lendLease(&held);
        return .{ .lease = held, .source = if (held.contended) .queued else .uncontended };
    }

    /// Gives a borrowed lease up for good instead of parking it (W5: drained
    /// first), for a holder about to run long work that must not keep
    /// other processes out, such as the detached allocator proof walk.
    /// A lease this lane did not lend is just closed.
    pub fn releaseBorrowed(self: *WriterLane, owner: *Owner, lease: *native.PathWriterLock) void {
        const lent = if (lease.return_to) |to| to.ptr == @as(*anyopaque, self) else false;
        if (!lent) return lease.close();
        // A same-process synthetic reader needs this lease held: return it
        // to the lane (parked) instead of giving it up.
        {
            const io = h.handleLockIo();
            self.mutex.lockUncancelable(io);
            const pinned = self.local_reader_activity.count != 0;
            self.mutex.unlock(io);
            if (pinned) return lease.close();
        }
        // Drain only: a full `sync` can enter maintenance writer admission,
        // which takes the gate this caller already holds.
        if (owner.currentStore()) |store| store.ensureDurableForReuse();
        lease.return_to = null;
        lease.close();
        const io = h.handleLockIo();
        self.mutex.lockUncancelable(io);
        self.checked_out = false;
        self.mutex.unlock(io);
        self.signalReleased(io);
    }

    /// `PathWriterLock.ReturnTo.give_back`: a lent lease's `close` parks it
    /// and checks the lane back in, with the same foreign-fairness rule as
    /// a foreground `checkin`.
    fn returnLease(ptr: *anyopaque, lease: native.PathWriterLock) void {
        const self: *WriterLane = @ptrCast(@alignCast(ptr));
        const owner: *Owner = @fieldParentPtr("writer_lane", self);
        const io = h.handleLockIo();
        const own_pid = antfly_platform.process.currentId() orelse 0;
        self.mutex.lockUncancelable(io);
        if (self.closing) {
            self.checked_out = false;
            self.mutex.unlock(io);
            var released = lease;
            released.close();
            self.signalReleased(io);
            return;
        }
        const now = std.Io.Clock.awake.now(io).nanoseconds;
        if (lease.othersWaitingAcrossProcesses(own_pid) and now - lease.acquired_ns >= foreign_fairness_quantum_ns) self.foreign_release_due = true;
        // W2: this process holds one lease, so nothing else can be parked.
        std.debug.assert(self.parked == null);
        self.parked = lease;
        self.parked_since_ns = now;
        self.parked_store = if (owner.current) |current| current.owned_lite_backend.?.native_docstore else null;
        // A borrower may have released and reacquired the lease, or written
        // through another instance: the next checkout must refresh.
        self.last_root = null;
        self.checked_out = false;
        self.mutex.unlock(io);
        self.signalReleased(io);
        // A timed loop wait re-reads the parked-lease timer on its own; a
        // lease returned by a maintenance turn must not wake another turn.
        if (owner.loop_may_park.load(.seq_cst)) self.wakeOwnerForParkedLease(io);
    }

    /// Ends a checkout that never got (or never lent) a lease.
    pub fn cancelCheckout(self: *WriterLane) void {
        const io = h.handleLockIo();
        self.mutex.lockUncancelable(io);
        self.checked_out = false;
        self.mutex.unlock(io);
        self.signalReleased(io);
    }

    fn signalReleased(self: *WriterLane, io: std.Io) void {
        _ = self.release_epoch.fetchAdd(1, .release);
        io.futexWake(u32, &self.release_epoch.raw, std.math.maxInt(u32));
    }

    /// How long a lease must have been held before a *genuinely foreign*
    /// waiter (`PathWriterLock.othersWaitingAcrossProcesses`, never this
    /// process's own maintenance worker -- see that method's doc comment)
    /// is allowed to end the lane's parking. Avoids releasing on a
    /// transient queue blip from a remote caller about to be served by
    /// someone else regardless; mirrors `QueuedMutex.contended_quantum_ns`,
    /// the fairness window the old per-call handoff used.
    const foreign_fairness_quantum_ns: i96 = 20 * std.time.ns_per_ms;

    /// How long a lease may sit parked with nobody ever checking it out
    /// again before this process gives it up even with no foreign waiter
    /// in sight -- mirrors `NativeFile.OpenOptions.redo_idle_checkpoint_ns`'s
    /// default (`Store.redo_idle_checkpoint_ns`, 1s): an idle process
    /// should hold no cross-process writer lease. Parking itself is only
    /// ever an optimization for *back-to-back* same-process writers;
    /// without this bound, a process that writes once and then goes idle
    /// would hold the lease forever, since nothing else (`checkin`'s own
    /// foreign-waiter check included) ever runs again to let it go --
    /// `maintainParkedLease`, driven by `Owner.maintenanceLoop`, is what
    /// actually enforces this bound, since an idle holder makes no more
    /// `checkin` calls of its own for `checkin` itself to react on. A
    /// named constant of its own, not a shared one, because the two
    /// concepts (redo-log checkpoint cadence and this lane's idle-park
    /// timeout) are free to diverge; they just happen to agree today.
    const parked_idle_release_ns: i96 = 1 * std.time.ns_per_s;

    /// Release `connection`'s hold on this lane once it no longer needs
    /// `root` (it has staged its write, or failed before staging): detach
    /// `connection`, then either hand the lease straight to an
    /// already-polling local writer (zero cost: no sync, no kernel
    /// release), keep it parked for the next one, or -- only once nobody
    /// local is waiting and a genuinely foreign (cross-process, or a
    /// different in-process `Owner`) waiter has outlasted the fairness
    /// quantum -- sync and give the kernel lease up for good.
    ///
    /// Deliberately keys the remote check on `PathWriterLock.
    /// othersWaitingAcrossProcesses`, not the plain `othersWaiting`: the
    /// latter cannot tell a genuinely foreign waiter apart from this
    /// *same* process's own maintenance worker, which competes for the
    /// same kernel lease through the same file-based fair queue. Treating
    /// that as "remote" starves the maintenance worker for as long as
    /// local writers keep arriving (it only ever shows up as "someone is
    /// queued" and never actually goes away on its own), and was observed
    /// empirically as repeated, unproductive sync/close/reacquire cycling
    /// under sustained local load. Excluding same-process tickets by
    /// their recorded pid avoids that interaction while still giving a
    /// truly remote process (or a different in-process `Owner`) a bounded
    /// wait, not an indefinite one, even under continuous local traffic.
    pub fn checkin(self: *WriterLane, connection: *Connection, store: *NativeStore) void {
        self.checkinImpl(connection, store, false);
    }

    /// Like `checkin`, but for a suspend of unbounded duration (an inline
    /// provider call during a lane-managed write -- `HandleGuard.
    /// suspendQuery`), not the bounded durability wait `checkin` itself
    /// guards. `checkin`'s fairness-quantum grace period before giving the
    /// lease up for a foreign waiter is sized for an fsync, not for
    /// however long this call's suspend lasts, so never park speculatively
    /// here on "nobody is waiting on it yet": only a same-process writer
    /// already polling (`waiting != 0`, zero cost either way, exactly as
    /// `checkin`) gets the warm handoff; otherwise release the kernel
    /// lease now, so a foreign waiter that arrives during the suspend --
    /// however long it runs -- is never stuck behind a lease this process
    /// is not even using.
    pub fn checkinForSuspend(self: *WriterLane, connection: *Connection, store: *NativeStore) void {
        self.checkinImpl(connection, store, true);
    }

    fn checkinImpl(self: *WriterLane, connection: *Connection, store: *NativeStore, force_release: bool) void {
        const io = h.handleLockIo();
        connection.detach();
        self.mutex.lockUncancelable(io);
        var lease = self.held_lease.?;
        self.held_lease = null;
        // Mirror `QueuedMutex.retainForWaiter`'s own cleanup before parking:
        // `withReadRegistrationBatch` (used by both `checkout`'s own cold
        // start and a `borrowLease` caller that reuses this parked
        // lease) asserts no batch is already
        // registered. This lease may carry one from whichever earlier
        // `acquirePathLeaseAssumeLocked` call originally acquired it.
        if (lease.reader_registration_batch) |batch| batch.end();
        lease.reader_registration_batch = null;
        const now = std.Io.Clock.awake.now(io).nanoseconds;
        if (self.waiting != 0) {
            self.parked = lease;
            self.parked_since_ns = now;
            self.parked_store = store;
            self.checked_out = false;
            self.mutex.unlock(io);
            self.signalReleased(io);
            // A local waiter takes this immediately (its own poll, not
            // `maintainParkedLease`), so no wake is needed for its sake --
            // but see the doc comment below for why one still happens.
            self.wakeOwnerForParkedLease(io);
            return;
        }
        const own_pid = antfly_platform.process.currentId() orelse 0;
        const foreign_overdue = lease.othersWaitingAcrossProcesses(own_pid) and now - lease.acquired_ns >= foreign_fairness_quantum_ns;
        // Only an explicit release (a suspend, which never has a staged
        // commit of its own) closes the lease here. A writer checking in
        // before its durability wait still has to drive its own group under
        // this lease, and draining for it from here waits on itself.
        // A nonzero `local_reader_activity` overrides even an explicit
        // release: a same-process synthetic reader is relying on this
        // exact lease staying held (see its own doc comment), so park
        // instead of closing regardless of why this call asked to.
        if (!force_release or self.local_reader_activity.count != 0) {
            if (foreign_overdue) self.foreign_release_due = true;
            self.parked = lease;
            self.parked_since_ns = now;
            self.parked_store = store;
            self.checked_out = false;
            self.mutex.unlock(io);
            self.signalReleased(io);
            // Bare `self.wake.set(io)` isn't enough: `Owner.maintenanceLoop`
            // may currently be parked indefinitely (no durable consumer
            // registered -- `no_scheduled_wait`), and nothing else ever
            // wakes it to start the bounded poll `maintainParkedLease`
            // needs to eventually release this lease on its own. See
            // `parked_idle_release_ns`'s own doc comment: without this,
            // a process that writes once and then goes idle keeps the
            // cross-process writer lease forever, since this exact
            // `checkin` call -- the only place that ever reacted to the
            // foreign-waiter/idle conditions before -- never runs again.
            self.wakeOwnerForParkedLease(io);
            return;
        }
        self.checked_out = false;
        self.mutex.unlock(io);
        self.signalReleased(io);
        store.ensureDurableForReuse();
        store.sync() catch {};
        lease.close();
    }

    /// `WriterLane` is embedded by value in `Owner` (`owner.writer_lane`),
    /// so its address recovers the owning `Owner` without a stored
    /// back-pointer (see `metadata_pool.zig`'s `Node.payload` for the same
    /// pattern). Called right after parking, outside `self.mutex`, so
    /// `Owner.maintenanceLoop` wakes promptly to run `maintainParkedLease`
    /// on this lane.
    fn wakeOwnerForParkedLease(self: *WriterLane, io: std.Io) void {
        const owner: *Owner = @fieldParentPtr("writer_lane", self);
        owner.wake.set(io);
    }

    /// Bounded-release check for the lease `checkin`/`checkinForSuspend`
    /// park (see `checkin`'s own doc comment): called every tick of
    /// `Owner.maintenanceLoop` so a parked lease can never outlive
    /// `foreign_fairness_quantum_ns` once a genuinely foreign waiter
    /// queues, or `parked_idle_release_ns` with nobody ever checking it
    /// out again -- `checkin` itself only reacts to those conditions on
    /// its *own* next call, which an idle holder never makes.
    ///
    /// Uses `parked_store` for the sync, never `Owner.current`/`last_root`:
    /// those can lag a same-process generation swap that lands while this
    /// lease sits parked (a root-keyed lookup would either skip the sync
    /// it must do, or run it against the wrong store). `checkin`'s own immediate
    /// foreign/idle release already trusts its `store` parameter
    /// unconditionally, with no root comparison at all; this mirrors that
    /// exactly, just reading the store back from where `checkinImpl`
    /// stashed it instead of having it handed in fresh.
    ///
    /// Returns the nanoseconds until this should be called again (bounding
    /// `Owner.maintenanceLoop`'s own sleep), or `null` once there is
    /// nothing parked left to check -- including right after this call
    /// releases it, so a released lease stops the loop from polling at
    /// all rather than spinning on an empty `parked`.
    pub fn maintainParkedLease(self: *WriterLane) ?i96 {
        const io = h.handleLockIo();
        self.mutex.lockUncancelable(io);
        var lease = self.parked orelse {
            self.mutex.unlock(io);
            return null;
        };
        // Releasing drains the store, which a W6 read may be using; a
        // same-process synthetic reader (`pinForLocalRead`) needs the lease
        // held. Retry shortly in either case.
        if (self.reading or self.local_reader_activity.count != 0) {
            self.mutex.unlock(io);
            return local_reader_pin_retry_ns;
        }

        const own_pid = antfly_platform.process.currentId() orelse 0;
        const now = std.Io.Clock.awake.now(io).nanoseconds;
        const foreign_waiting = lease.othersWaitingAcrossProcesses(own_pid);
        const foreign_overdue = foreign_waiting and now - lease.acquired_ns >= foreign_fairness_quantum_ns;
        const idle_overdue = now - self.parked_since_ns >= parked_idle_release_ns;
        if (!foreign_overdue and !idle_overdue) {
            self.mutex.unlock(io);
            const idle_remaining = self.parked_since_ns + parked_idle_release_ns - now;
            // No foreign waiter to be fair to right now: `lease.acquired_ns`
            // is this lease's original kernel acquisition, which a
            // same-process warm handoff (`checkout`'s own "no self-replacement"
            // fast path, and this same lane's own `checkin` park) never
            // refreshes, so after many back-to-back same-process writes it
            // can be arbitrarily older than `foreign_fairness_quantum_ns`.
            // Bounding the wait by that stale timestamp regardless made this
            // return the 1ms floor on every call for as long as writes kept
            // landing, for a fairness window protecting nobody -- the perf
            // regression perf/lwa-xproc-writers item 5 traced "no-consumer
            // path 1037"'s 27-41 unwanted background turns to. Only let the
            // quantum bound the wait once a foreign waiter actually exists
            // to need it.
            if (!foreign_waiting) return @max(1 * std.time.ns_per_ms, idle_remaining);
            const quantum_remaining = lease.acquired_ns + foreign_fairness_quantum_ns - now;
            return @max(1 * std.time.ns_per_ms, @min(quantum_remaining, idle_remaining));
        }
        self.parked = null;
        self.foreign_release_due = false;
        self.checked_out = true;
        self.holder = std.Thread.getCurrentId();
        const store = self.parked_store;
        self.parked_store = null;
        self.mutex.unlock(io);
        // Commits staged under this lease (ride-along groups, or a group a
        // writer drives after checking the lane in) must finish before the
        // lease goes. An idle release also folds the redo log, since the
        // store's own idle turn never writes (lite-writer-authority.md
        // §4.3); a foreign handoff skips the fold to keep its turn short.
        if (store) |held_store| {
            held_store.ensureDurableForReuse();
            if (!foreign_overdue) held_store.foldIdleRedoLog();
            held_store.sync() catch {};
        }
        lease.close();
        self.mutex.lockUncancelable(io);
        self.checked_out = false;
        self.mutex.unlock(io);
        self.signalReleased(io);
        return null;
    }
};

pub const Owner = struct {
    // Only bootstrap small files at close. Large files must finish their
    // existing worker proof; shutdown never starts another whole-file walk.
    const close_allocator_proof_max_bytes = 4 * 1024 * 1024;
    // notifyMutation only raises allocator_certificate_bootstrap_worthwhile
    // once the checkpoint has grown this much since the last baseline, not
    // on every write (design doc §3.6). Modest relative to
    // close_allocator_proof_max_bytes: large enough that a handful of small
    // writes (the no-consumer path's common case) never trips it, small
    // enough that a real write burst still gets certified at close.
    const allocator_certificate_threshold_bytes: u64 = 256 * 1024;
    // Sentinel for maintenanceWaitMillis/maintenanceLoop: no durable
    // consumer is registered and nothing is pending from the last turn, so
    // the loop parks on `wake` without a timer instead of polling.
    const no_scheduled_wait: i64 = -1;
    const MaintenanceKey = struct { table_id: u64, incarnation: u128 };
    const MaintenanceSchedule = struct {
        shadow_next: bool = true,
        startup_replay_pending: bool = true,
        source_recovery_next: bool = true,
        group_source_next: bool = true,
        // Separately allocated: table admission can grow the schedule map
        // while a visibility/idle drain borrows this state.
        group_sources: ?*h.db_mod.DB.GroupSourceMaintenanceState = null,
        next_source_recovery_ns: i96 = 0,
        ttl: h.db_mod.types.TTLCleanupStats = .{},
        last_ttl_error: ?anyerror = null,
        next_ttl_ns: i96 = 0,
    };
    const ttl_counter_fields = .{ "acquisition_count", "runs", "scanned_timestamps", "deleted_docs", "scanned_graph_candidates", "expired_graph_sources", "expired_graph_artifacts", "error_count", "lease_acquire_failures", "lost_leases" };

    fn recordTtlTurn(total: *h.db_mod.types.TTLCleanupStats, before: h.db_mod.types.TTLCleanupStats, after: h.db_mod.types.TTLCleanupStats) void {
        inline for (ttl_counter_fields) |field| @field(total, field) +|= @field(after, field) -| @field(before, field);
        total.last_run_ns = @max(total.last_run_ns, after.last_run_ns);
        total.last_acquired_ms = @max(total.last_acquired_ms, after.last_acquired_ms);
    }

    /// Native TTL is driven under this owner's gate. Count completed turns at
    /// that lifetime, rather than losing them when a serving DB view refreshes.
    /// Configuration and current lease state still come from the live runtime.
    pub fn overlayTtlStats(self: *Owner, database: *h.db_mod.DB, stats: *h.db_mod.types.TTLCleanupStats) void {
        const schedule = self.maintenance_schedules.get(.{ .table_id = database.core.identity_namespace.table_id, .incarnation = database.root_incarnation }) orelse return;
        inline for (ttl_counter_fields) |field| @field(stats, field) = @max(@field(stats, field), @field(schedule.ttl, field));
        stats.last_run_ns = @max(stats.last_run_ns, schedule.ttl.last_run_ns);
        stats.last_acquired_ms = @max(stats.last_acquired_ms, schedule.ttl.last_acquired_ms);
    }

    path: []const u8,
    options: api.LiteResolvedOpenOptions,
    owned_ttl_owner_id: ?[]u8 = null,
    owned_models_dir: ?[]u8 = null,
    gate: *PathGate,
    /// WS3 writer lane: see `WriterLane`'s doc comment. One per Owner (one
    /// per Store), shared by every compatible `Connection` to this path in
    /// this process.
    writer_lane: WriterLane = .{},
    /// lite-single-commit-write-path.md §3.3c: the most recent ambiguous
    /// outcome `commitCoordinatedLocal` (`capi/sql.zig`) retired a marker
    /// for, across any table on this path. `DB.outcomeMarkerFallbackStatus`
    /// uses this to resolve `antfly_db_get_transaction_status` for an id
    /// that is not a two-phase decision -- kept here, not on the
    /// per-generation `*DB` the ambiguous commit actually touched,
    /// because an uncertain outcome's own reconciliation on the next call
    /// can replace that `DB` instance before a client ever gets to query
    /// status, which would silently lose an in-memory field living there.
    /// Bounded to exactly one outstanding ambiguous receipt per path at a
    /// time (overwritten by the next one, across every table): an older,
    /// still-unresolved `transaction_id` stops resolving once a newer
    /// ambiguous commit replaces it here, same as any bounded receipt
    /// scheme.
    last_ambiguous_fused_marker: ?@import("../storage/db/apply_receipts.zig").OutcomeMarker = null,
    connections: std.ArrayList(*Connection) = .empty,
    worker: ?std.Thread = null,
    // The path gate permits one foreground lease wait per owner. The global
    // debug Io cannot spawn tasks; this lazy pool supplies exactly the lock
    // waiter and its deadline timer, and is shared by compatible connections.
    path_lock_io: std.Io.Threaded,
    test_maintenance_wait_ns: if (h.builtin.is_test) ?i96 else void = if (h.builtin.is_test) null else {},
    stopping: std.atomic.Value(bool) = .init(false),
    /// Set while `maintenanceLoop` may sleep with no timeout. Raised before
    /// the loop reads the lane's parked-lease timer, so a lease parked after
    /// that read (`WriterLane.returnLease`) sees it and wakes the loop.
    loop_may_park: std.atomic.Value(bool) = .init(false),
    // One graph walk per owner. Admission waits without the foreground gate;
    // the worker only try-locks under that gate and yields to an active proof.
    allocator_proof_mutex: std.Io.Mutex = .init,
    /// A proof that completed but could not be transferred because the
    /// writer lease was busy. Guarded by the proof mutex.
    allocator_proof_receipt: ?native.NativeFile.AllocatorProofReceipt = null,
    verification_cancel: @FieldType(NativeStore, "maintenance_cancel") = .{},
    allocator_certificate_pending: bool = false,
    /// Threshold-gated sibling of `allocator_certificate_pending` consulted
    /// only by certifyAllocatorOnClose's bootstrap decision (starting a
    /// brand new verification walk at close) -- see notifyMutation's doc
    /// comment for why this is a separate flag from
    /// `allocator_certificate_pending` itself.
    allocator_certificate_bootstrap_worthwhile: bool = false,
    /// Checkpoint bytes observed the last time notifyMutation() considered
    /// `allocator_certificate_bootstrap_worthwhile`; null until the first
    /// call establishes a baseline. Lets notifyMutation flip that flag only
    /// once the checkpoint has grown past
    /// `allocator_certificate_threshold_bytes` since then, instead of on
    /// every write (design doc §3.6: "allocator certificates run at close
    /// or past a size threshold, not on every write").
    allocator_certificate_baseline_bytes: ?u64 = null,
    /// WS5/WS7 (not yet wired into maintenanceTurn, see the "NOT YET WIRED"
    /// comment there): intended to hold the highest watermark this owner
    /// has fully drained via DocStore.serviceRetirement
    /// (RetirementStats.done was true), compared against
    /// DocStore.retirement_queue.requested() to decide whether there is new
    /// retirement demand.
    retirement_drained_through: u64 = 0,
    // Historical attempts suppress close-time retries; only active work is
    // allowed to finish when shutdown cancels the remaining maintenance.
    allocator_verification_started: bool = false,
    allocator_verification_active: bool = false,
    // Directory progress belongs to the path owner, not a serving generation.
    artifact_marker_sweep: @FieldType(NativeStore, "artifact_marker_sweep") = .{},
    reader_coordinator: *native.ReaderCoordinator = undefined,
    maintenance_pending: bool = false,
    // Protected by the path gate. Nested work on this owner shares its turn;
    // other processes cannot open unverified index runtimes during its proof.
    maintenance_turn_lease: ?std.Io.File = null,
    maintenance_turn_references: usize = 0,
    maintenance_turn_deferred: bool = false,
    // Replay can move the checkpoint on every turn. Give physical storage a
    // fresh capture between such turns instead of always servicing a stale
    // pre-replay snapshot that reclamation must correctly refuse.
    maintenance_storage_pending: bool = false,
    // Owner lifetime, rather than DB-runtime lifetime: snapshot refreshes must
    // not reset a table's replay/construction alternation.
    maintenance_schedules: std.AutoHashMapUnmanaged(MaintenanceKey, MaintenanceSchedule) = .empty,
    maintenance_root_incarnation: ?u128 = null,
    wake: std.Io.Event = .unset,
    maintenance_activity: @import("maintenance_activity.zig").Activity = .{},
    current: ?*h.Handle = null,
    retired: std.ArrayList(*h.Handle) = .empty,
    maintenance_snapshot: ?*h.Handle = null,
    /// Non-null for the duration of a hosted/non-independent `maintenanceTurn`
    /// (one whose `root` is `self.current` itself, not a separate detached
    /// snapshot): the generation whose `maintenance_references` this very
    /// turn incremented. A generated producer's publication callback
    /// (`PreparedPublication.current`/`.acquireStore`) reenters `refresh`
    /// from inside that same turn, under the same `gate.mutex`, so it can
    /// never race its own in-flight work -- the nonzero reference it sees on
    /// `current` is its own, not a concurrent user's. `refreshOrTurnBusy`'s
    /// reuse fast paths compare against this to tell that case apart from a
    /// genuinely different, still-active maintenance reference that must
    /// still force a reopen.
    maintenance_turn_root: ?*h.Handle = null,
    /// Set by `enterMaintenanceWriter`, under `gate.mutex`, only while its
    /// own nested `acquireMaintenancePathLeaseAssumeLocked`/
    /// `tryWriterLeaseAssumeLocked` call is admitting a write for a root
    /// that *is* `self.current` (hosted/non-independent `maintenanceTurn`,
    /// where they are the same object, not a detached snapshot). That
    /// nested call's own lease handoff would otherwise call
    /// `ensureDurableForReuse` on `self.current`'s store to wait out a
    /// drive "on another thread" (`ensureDurableForReuse`'s own doc
    /// comment) -- but here the in-flight drive is THIS SAME call's own
    /// outer frame, on this same thread, not a foreign one (e.g. a derived
    /// index catch-up's `Store.applyMutationGroup`, itself mid-`commit_
    /// draining`, reentering writer admission for the index artifacts it
    /// is producing). Waiting on it is a self-deadlock, not a race to
    /// guard against: there is only one `NativeFile` instance here, and
    /// its own commit cannot be "concurrent" with the call it is nested
    /// inside of. `WriterLane.borrowLease`/`tryWriterLeaseAssumeLocked`
    /// check this to skip exactly (and only) that one redundant wait.
    skip_reuse_drain: bool = false,
    maintenance_table_cursor: usize = 0,
    maintenance_round_pending: bool = false,
    /// A maintenance turn's writes landed on the current root's store
    /// (lite-writer-authority.md W1/W3: snapshots forward their writes).
    /// The file is current, but the foreground DB's in-memory runtime
    /// (catalogs, index manager, derived progress) still reflects the state
    /// before them, so the next `refresh` replaces the generation instead of
    /// taking a fast path.
    foreground_runtime_stale: bool = false,
    maintenance_round_ttl_pending: bool = false,
    maintenance_capture_misses: u8 = 0,
    // Connection-owner lifetime totals, protected by the path gate. They survive
    // foreground runtime refreshes and distinguish conflicts from a moving tip.
    publication_stats: struct {
        /// Namespace rounds reaching replay/idle completion (errors below).
        /// Lets observers distinguish quiescence from a worker that stopped.
        maintenance_rounds: u64 = 0,
        retirement_work_pages: usize = 128,
        replay_budget_table_id: u64 = 0,
        replay_budget: @import("../storage/db/derived/replay_budget.zig").Snapshot = .{},
        attempts: u64 = 0,
        refused: u64 = 0,
        rebased: u64 = 0,
        generated_attempts: u64 = 0,
        generated_refused: u64 = 0,
        replay_attempts: u64 = 0,
        replay_refused: u64 = 0,
        maintenance_errors: u64 = 0,
        maintenance_queue_timeouts: u64 = 0,
        allocator_refusals: u64 = 0,
        allocator_service_admissions: u64 = 0,
        index_input_refusals: u64 = 0,
        // Detached graph-walk totals, including foreground and maintenance.
        // These survive runtime refresh and exclude lease/queue wait time.
        allocator_proof_walks: u64 = 0,
        /// Proofs completed on an earlier attempt that yielded busy, reused
        /// without repeating the walk.
        allocator_proof_receipt_reuses: u64 = 0,
        allocator_proof_failures: u64 = 0,
        allocator_proof_ns: u64 = 0,
        allocator_proof_max_ns: u64 = 0,
        // Successful positive-timeout mutation admission, before its body.
        // Log2 nanosecond buckets give bounded percentile ranges, not exact
        // sample quantiles. Diagnostics/read admission cannot pollute them.
        mutation_admission_count: u64 = 0,
        mutation_admission_ns: u64 = 0,
        mutation_admission_max_ns: u64 = 0,
        mutation_admission_log2_ns: [64]u64 = @splat(0),
        // Cross-process writer lease: wait from the start of acquisition
        // (FIFO queue plus kernel lock, excluding the in-process gate) and
        // hold until release or handoff. Foreground covers exported calls;
        // maintenance covers background publication, replay and reclamation.
        foreground_lease: native.LeaseTiming = .{},
        maintenance_lease: native.LeaseTiming = .{},
        // Time foreground calls spent queued on this process's path gate.
        gate_wait_ns: u64 = 0,
        gate_wait_max_ns: u64 = 0,
        /// Background writer-lease holds by site (count, total and max ns),
        /// to attribute `maintenance_lease` hold time.
        maintenance_hold_sites: MaintenanceHoldSites = .{},
        last_maintenance_error: ?[]const u8 = null,
    } = .{},
    test_runtime_snapshot_opens: if (h.builtin.is_test) u64 else void = if (h.builtin.is_test) 0 else {},
    embedding_memo: ?memo_mod.QueryEmbeddingCache = null,
    embedding_memo_budget: cache_budget.CacheBudget = .init(embedding_memo_bytes),
    test_after_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *h.Handle) void } else void = if (h.builtin.is_test) null else {},
    test_after_allocator_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *native.NativeFile) void } else void = if (h.builtin.is_test) null else {},
    test_before_allocator_reader_close: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *native.NativeFile) void } else void = if (h.builtin.is_test) null else {},
    test_allocator_wait: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},
    test_after_stop: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},
    test_before_storage_service: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *h.Handle) void } else void = if (h.builtin.is_test) null else {},
    test_before_maintenance_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},
    /// Per-owner, test-local count of background turns the maintenance
    /// loop actually ran (as opposed to waking and finding nothing to do).
    /// Unlike the diagnostic-only write_amplification_trace counters, this
    /// is scoped to one Owner instance, so concurrent tests never share it.
    test_background_turn_count: if (h.builtin.is_test) std.atomic.Value(u64) else void = if (h.builtin.is_test) .init(0) else {},
    /// Per-owner, test-local count of every `refresh()` call (foreground
    /// capi entries and background turns both). Combined with
    /// `test_background_turn_count == 0`, a delta here is wholly
    /// attributable to the calling writes themselves.
    test_refresh_count: if (h.builtin.is_test) std.atomic.Value(u64) else void = if (h.builtin.is_test) .init(0) else {},
    /// Per-owner, test-local running total of `native.NativeFile.
    /// test_sync_calls` across every generation this owner has ever had as
    /// `current`. `refresh`'s slow path replaces `current` (and therefore
    /// the whole `NativeFile`, with a fresh, zeroed `test_sync_calls`) on a
    /// vacuum/compaction-triggered reopen or any of its other reasons; a
    /// test that samples the live file's counter at two points in time reads
    /// a meaningless (and, under a plain saturating subtraction, silently
    /// wrong) number if a swap happened in between -- see `capi/db.zig`'s
    /// `ws3WriterLaneTestSyncCalls` doc comment for a confirmed case. Reading
    /// `test_cumulative_sync_calls + current's own test_sync_calls` is
    /// stable across any number of swaps.
    test_cumulative_sync_calls: if (h.builtin.is_test) std.atomic.Value(u64) else void = if (h.builtin.is_test) .init(0) else {},
    /// The same bridge for the outgoing generation's `Store.test_groups_total`
    /// and `Store.test_group_members_total` group-commit counters.
    test_cumulative_groups: if (h.builtin.is_test) std.atomic.Value(u64) else void = if (h.builtin.is_test) .init(0) else {},
    test_cumulative_group_members: if (h.builtin.is_test) std.atomic.Value(u64) else void = if (h.builtin.is_test) .init(0) else {},

    /// A retiring generation rejects further publication once nothing else
    /// still needs it writable -- except a `WriterLane`-admitted call still
    /// mid-flight on it. `WriterLane.checkout`'s own doc comment: a pinned
    /// root's `lane_pins` is kept nonzero "for as long as this call might
    /// still touch it, including while its own ticket is still
    /// outstanding" -- that outstanding ticket is exactly a
    /// `sync_level: .enrichments`/`.full_index` write's own derived-
    /// visibility wait (WS3's `releaseWriterLaneBeforeWait`/`leave`), which
    /// keeps driving further writes against this same table (group-source
    /// slices, enrichment production) for as long as it runs, with none of
    /// the connection-level locks held across that wait. A concurrent
    /// `refresh` -- a sibling write, or the maintenance owner's own
    /// republish via `PreparedPublication.acquireStore`/`.current()`,
    /// entirely unrelated to this call's own progress -- can retire and
    /// mark read-only the exact generation that pinned write is still
    /// using. That is not just a flaky retry: a write that lands in the gap
    /// between the retirement decision and the store actually flipping
    /// read-only hits `error.ReadOnly` well-behaved (see
    /// `group_source_owner.Owner.step`'s own tolerance for it), but one
    /// that free-rides on this instance's now-superseded writer-base
    /// bookkeeping can reach `NativeFile.publishCheckpoint`'s backstop --
    /// "stale writer base reached publishCheckpoint" -- which is a hard
    /// panic, not a retryable error, by design (its own doc comment: by the
    /// time a stale instance reaches publication it may have already reused
    /// pages out of its own stale free map). `collectRetired` already treats
    /// a nonzero `lane_pins` as a reason this generation cannot be destroyed
    /// yet; extend that same deferral to the read-only marking that
    /// precedes destruction. Called again from `collectRetired` once the
    /// matching pin clears, in case this generation is still sitting in
    /// `self.retired` by then.
    pub fn markRetiredReadOnlyIfIdle(retired: *h.Handle) void {
        // Only a generation `refresh`'s slow path has already replaced
        // (`previous.lease_snapshot = true`, set at the same point it is
        // pushed onto `self.retired`) is ever eligible: the live
        // `self.current` must never be marked read-only here, including
        // when this is called again from a later sweep that finds the
        // pin's own root was never actually retired in the first place.
        if (!retired.lease_snapshot) return;
        if (retired.maintenance_references != 0 or retired.immutable_query_borrows != 0 or retired.lane_pins.load(.acquire) != 0) return;
        const store = retired.owned_lite_backend.?.native_docstore.?;
        if (store.read_only) return;
        store.read_only = true;
        store.file.read_only = true;
    }

    /// Request a retiring generation's resident background workers --
    /// `DB.startResidentBackgroundWorkersIfNeeded`'s own `group_source_owner`
    /// scheduler registration and each table's `enrichment_runtime` worker
    /// -- to stop driving new writes against it, without blocking this
    /// `refresh` call on either.
    ///
    /// Why non-blocking is the only safe option here: both workers are
    /// `.producer`-class tasks on the *same* `maintenanceScheduler` this
    /// refresh may itself be running under (a resident worker's own pass
    /// can call `Owner.refresh` again, e.g. via `PreparedPublication.
    /// acquireStore`/`.current()`, to republish against the live
    /// generation). `group_source_owner.Owner.stop`/`EnrichmentRuntime.stop`
    /// both join the in-flight task (`MaintenanceScheduler.Handle.await`/
    /// `Io.Future.await`) before returning. If this function's caller holds
    /// `self.gate.mutex` (every `refresh` caller does -- see this struct's
    /// own doc comment) and that join is waiting on a task that is itself
    /// blocked re-entering `Owner.refresh` for the *same* mutex, this
    /// deadlocks: the join never returns because the task it is waiting on
    /// can never acquire the lock the joiner already holds. (An earlier,
    /// fully-blocking version of this fix hit exactly that, observed as the
    /// test run stalling for tens of seconds inside this exact call chain.)
    /// A bare, atomic "please stop" store has no such hazard: it only ever
    /// touches memory already safely synchronized by the task's own
    /// `step`/`workerStep` re-checking it at the top of its *next* turn --
    /// never this thread blocking on that check happening.
    ///
    /// The actual join -- the one that frees `self.registration`/`self.future`
    /// and is safe to block on -- happens later, from `DB.close` (via
    /// `tables.closeAll`/`closeHandle`), reached only once `collectRetired`
    /// decides nothing else references this generation at all (not even a
    /// pinned cursor or an in-flight `lane_pins` write): by then, the worker
    /// this function asked to stop has long since seen `stopping`/
    /// `shutdown_requested` at the top of its own turn and gone idle, so
    /// that later join has nothing live left to wait on and cannot
    /// recursively call `refresh` either. A fresh registration starts the
    /// next time `replacement`'s own table is (re)opened
    /// (`tables.get`/`openTable`), so no caller is ever left without a
    /// resident worker once one is actually needed again.
    fn stopResidentBackgroundWorkersNonBlocking(database: *h.db_mod.DB) void {
        database.group_source_owner.stopping.store(true, .release);
        if (database.enrichment_runtime) |runtime| runtime.beginTeardown();
    }

    /// Caller holds the path lease, so the checkpoint and catalog cannot race
    /// a different process's publication. Cursor continuation deliberately
    /// uses its pinned generation without reopening the connection.
    pub fn refresh(self: *Owner) !*h.Handle {
        return self.refreshOrTurnBusy() catch |err| switch (err) {
            error.MaintenanceTurnBusy => error.FileBusy,
            else => err,
        };
    }

    /// `refresh`, except that a replacement open which needs the
    /// cross-process maintenance turn while another writer holds it fails
    /// with `error.MaintenanceTurnBusy` instead of `error.FileBusy`, so a
    /// caller holding the writer lease (`Connection.refresh`) can give the
    /// lease back and wait for the turn instead of failing at once.
    fn refreshOrTurnBusy(self: *Owner) !*h.Handle {
        return (try self.refreshImpl(false)).?;
    }

    /// lite-writer-authority.md W6: the serving root for a plain read that
    /// holds no writer lease, or null when only a replacement open would
    /// serve it. A writer-mode open can run recovery writes, so that case
    /// goes back through the writer path.
    pub fn refreshForRead(self: *Owner) !?*h.Handle {
        return self.refreshImpl(true);
    }

    fn refreshImpl(self: *Owner, fast_only: bool) !?*h.Handle {
        wal_trace.recordOp(.serving_runtime_refresh);
        if (comptime h.builtin.is_test) _ = self.test_refresh_count.fetchAdd(1, .release);
        var known_maintenance: ?*h.Handle = null;
        if (self.current) |current| {
            if (current.lite_scheduling_owner == null) try self.bindGroupSourceState(current);
            var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
            defer probe.close();
            const file = &current.owned_lite_backend.?.native_docstore.?.file;
            const probe_inode = (try probe.file.stat(probe.runtime())).inode;
            // Establish the fenced tip before opening any new read pins.
            // Existing readers of an unknown peer publication remain poisoned.
            self.reader_coordinator.observeCurrent(probe_inode, probe.headerFingerprint());
            const same_inode = probe_inode == (try file.file.stat(file.runtime())).inode;
            // Under the redo log, an ordinary commit advances the logical
            // commit sequence (`NativeFile.committedSequence()`, whose own
            // doc comment warns about exactly this) without ever touching
            // the physical checkpoint slot or header until the next
            // `checkpointRedoLog` fold. `probe` is a fresh open -- its own
            // `recoverRedoLog` replay always reflects the latest durable
            // sequence this inode has on disk -- but `file` (this Owner's
            // own, possibly long-idle serving instance) only reflects what
            // it last observed. Neither the raw header comparison below nor
            // `runtimeMetadataUnchanged`/`loadedIndexInputsUnchanged` (both
            // physical-root-only comparisons) can see a redo-only commit on
            // a sibling connection on their own: every fast path here must
            // also agree on this logical watermark before reusing `current`.
            const sequence_unchanged = probe.committedSequence() == file.committedSequence();
            // A hosted/non-independent `maintenanceTurn` sets `root` to
            // `self.current` itself and bumps *its own* `maintenance_references`
            // for the turn's duration (see that field's own doc comment). A
            // generated producer's publication callback reenters `refresh`
            // from inside that same turn, under the same `gate.mutex`, to
            // republish against the live generation -- never concurrently
            // with the reference it is itself holding. Treat that one
            // self-held reference as if it were absent; a reference any
            // other caller holds on `current` still forces a reopen below.
            const maintenance_references_clear = current.maintenance_references == 0 or
                (current == self.maintenance_turn_root and current.maintenance_references == 1);
            // An uncertain publication can fail before writing anything, so an
            // unchanged header does not prove the runtime is reusable: reopen
            // to reconcile from durable state.
            if (!self.foreground_runtime_stale and current.immutable_query_borrows == 0 and maintenance_references_clear and same_inode and !needsOutcomeReconciliation(current) and sequence_unchanged and std.meta.eql(probe.header, file.header)) {
                if (!file.free_pages_verified) if (self.maintenance_snapshot) |snapshot|
                    try file.inheritAllocatorVerification(&snapshot.owned_lite_backend.?.native_docstore.?.file);
                return current;
            }
            // `sequence_unchanged` exists to catch a redo-only commit that
            // the header/metadata comparisons below cannot see on their own
            // (the comment above). Without a redo log, the physical
            // checkpoint *is* the only commit record: every commit,
            // including a lease-only one, bumps `committedSequence()` by
            // bumping the checkpoint's own `commit_sequence`, so requiring
            // it unchanged here would force a full reopen for exactly the
            // lease bookkeeping this fast path exists to adopt cheaply.
            // `runtimeMetadataUnchanged`/`loadedIndexInputsUnchanged` already
            // prove no primary/index root moved; that is sufficient on a
            // non-redo file, which has no logical watermark separate from
            // the physical one for `sequence_unchanged` to protect.
            const fast2_metadata_unchanged = try file.runtimeMetadataUnchanged(&probe);
            const fast2_index_unchanged = try loadedIndexInputsUnchanged(current, file, &probe);
            if (!self.foreground_runtime_stale and current.immutable_query_borrows == 0 and maintenance_references_clear and same_inode and !needsOutcomeReconciliation(current) and (sequence_unchanged or file.redo == null) and
                fast2_metadata_unchanged and fast2_index_unchanged)
            {
                // Lease bookkeeping and unpublished shadow files do not change
                // a serving runtime. Reopening it would publish another lease
                // for every immutable input while foreground traffic waits.
                try current.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(probe.header);
                if (self.maintenance_snapshot) |snapshot| try file.inheritAllocatorVerification(&snapshot.owned_lite_backend.?.native_docstore.?.file);
                if (self.options.profile == .native and self.options.open_mode == .writer and !file.free_pages_verified) {
                    _ = try self.copyIdleSnapshotLedger(file);
                    _ = try file.inheritAllocatorCertificate();
                }
                return current;
            }
            if (self.maintenance_snapshot) |snapshot| {
                const captured = &snapshot.owned_lite_backend.?.native_docstore.?.file;
                if (std.meta.eql(probe.header, captured.header) and
                    (try probe.file.stat(probe.runtime())).inode == (try captured.file.stat(captured.runtime())).inode) known_maintenance = snapshot;
            }
            // perf/lwa-xproc-refresh: neither fast path above fired, so a
            // reopen is already decided -- `file` (this Owner's own,
            // possibly long-idle serving instance) is about to be
            // discarded for `replacement`, a brand-new `NativeFile` on the
            // same inode. That reopen's own `recoverRedoLog` trusts an
            // already-populated, same-pid `SharedRedoOverlay` verbatim and
            // never re-populates it (`inode_lock.ensureRedoOverlay`'s own
            // doc comment): correct for a same-process sibling, whose own
            // commits already wrote straight into that one shared
            // instance, but not for a commit made by a genuinely different
            // process -- that overlay is this process's own, and nothing
            // in this process ever applied a foreign process's logical
            // mutations to it, so `replacement` would silently inherit the
            // exact same, still-incomplete instance and gain nothing from
            // reopening. Calling the one primitive that actually walks the
            // log past this process's own last-applied sequence (via the
            // shared lock region's published watermark, §9.3b) and decodes
            // whatever is new directly into that shared overlay closes
            // that gap -- called here, on the *old* `file`, while its own
            // bound is still genuinely behind (this is the only place
            // where that is true: `replacement`'s own cold open would
            // already have advanced a fresh instance's bound via its
            // independent physical log scan before any caller could
            // intervene, making this same call on `replacement` itself a
            // guaranteed no-op). A cheap no-op here too whenever nothing
            // new was actually published since this process's own overlay
            // was last caught up.
            if (file.redo != null) try file.refreshCrossProcessRedoOverlay();
        }
        if (fast_only) return null;
        var options = self.options;
        options.externally_locked = true;
        if (known_maintenance orelse self.current) |verified| options.allocator_verification_owner = &verified.owned_lite_backend.?.native_docstore.?.file;
        // Certify before opening index sources, whose lease publications can
        // advance the header. The probe is borrowed only by synchronous open.
        var certified: ?native.NativeFile = null;
        defer if (certified) |*file| file.close();
        var retained_turn = false;
        defer if (retained_turn) self.releaseMaintenanceTurnAssumeLocked();
        if (self.options.profile == .native and self.options.open_mode == .writer) {
            certified = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
            certified.?.allocator_cancel_token = &self.verification_cancel;
            if (self.current) |previous| {
                // Only an idle serving wrapper can donate. Active cursors,
                // detached work, proof readers, and a lane writer still
                // mid-flight on this generation (whose own commit may still
                // be changing the allocator ledger) retain independent caches.
                if (previous.query_borrows == 0 and previous.maintenance_references == 0 and
                    previous.allocator_verification_references == 0 and previous.sql_cursors.count() == 0 and
                    previous.lane_pins.load(.acquire) == 0)
                    _ = try certified.?.takeAllocatorRebaseBase(&previous.owned_lite_backend.?.native_docstore.?.file);
            }
            _ = try self.copyIdleSnapshotLedger(&certified.?);
            if (try certified.?.inheritAllocatorCertificate()) {
                options.allocator_verification_owner = &certified.?;
                options.allocator_state_donor = &certified.?;
            } else if (certified.?.header.indexed_reclamation and !try self.unindexedRuntimeOpen(&certified.?)) {
                retained_turn = try self.retainMaintenanceTurnAssumeLocked();
                if (!retained_turn) return error.MaintenanceTurnBusy;
            }
        }
        const replacement = try api.openLiteHandleAllocWithSharedInference(alloc, self.path, options, false, h.handleLockIo(), null, self.current);
        errdefer h.closeHandle(replacement);
        try self.bindGroupSourceState(replacement);
        if (self.current) |previous| {
            // A clean memo is meaningful only at its logical metadata cut.
            // Copy the witness; recover() compares it with the replacement's
            // current identity before skipping work.
            if (previous.owned_lite_backend.?.native_docstore.?.file.header.indexed_reclamation and
                replacement.owned_lite_backend.?.native_docstore.?.file.header.indexed_reclamation)
                replacement.sql_ddl_recovery_needed = previous.sql_ddl_recovery_needed;
            replacement.sql_ddl_recovery_generation = previous.sql_ddl_recovery_generation;
            replacement.sql_ddl_agenda_after = previous.sql_ddl_agenda_after;
            replacement.sql_ddl_agenda_after_len = previous.sql_ddl_agenda_after_len;
            replacement.sql_ddl_agenda_scan_generation = previous.sql_ddl_agenda_scan_generation;
            replacement.sql_ddl_agenda_scan_started = previous.sql_ddl_agenda_scan_started;
            replacement.sql_ddl_agenda_scan_complete = previous.sql_ddl_agenda_scan_complete;
            if (known_maintenance) |snapshot| {
                try replacement.owned_lite_backend.?.native_docstore.?.file.inheritAllocatorVerification(&snapshot.owned_lite_backend.?.native_docstore.?.file);
                replacement.sql_commit_recovery_needed = previous.sql_commit_recovery_needed or snapshot.sql_commit_recovery_needed;
                replacement.sql_ddl_recovery_needed = previous.sql_ddl_recovery_needed or snapshot.sql_ddl_recovery_needed;
            }
            try replacement.owned_lite_backend.?.native_docstore.?.file.inheritAllocatorVerification(&previous.owned_lite_backend.?.native_docstore.?.file);
            inheritReclamationTelemetry(replacement.owned_lite_backend.?.native_docstore.?, previous.owned_lite_backend.?.native_docstore.?);
            // Group continuation belongs to this connection owner and was
            // bound above; retiring a DB wrapper cannot move or reset it.
            try self.retired.ensureUnusedCapacity(alloc, 1);
            self.retired.appendAssumeCapacity(previous);
            previous.lease_snapshot = true;
            // Cursor continuation can only read this generation. Make every
            // namespace's shared native store reject publication immediately
            // -- except for a write this same retirement must not pull the
            // floor out from under: `markRetiredReadOnlyIfIdle`'s own doc
            // comment.
            markRetiredReadOnlyIfIdle(previous);
            // Likewise, tell every resident background worker this
            // generation ever started that it is retiring, so it stops
            // attempting new writes against it instead of racing the
            // store's own read-only transition or this generation's
            // superseded writer-base bookkeeping -- see
            // `stopResidentBackgroundWorkersNonBlocking`'s own doc comment
            // for why this must stay non-blocking.
            stopResidentBackgroundWorkersNonBlocking(&previous.db);
            var retiring_tables = previous.embedded_tables.valueIterator();
            while (retiring_tables.next()) |table| stopResidentBackgroundWorkersNonBlocking(&table.*.db);
        }
        // Child table handles outlive snapshots. Drop borrowed producers before
        // collectRetired releases their old provider (even if addresses are reused).
        for (self.connections.items) |connection| {
            for (connection.table_handles.items) |id| {
                const child, const slot = api.handleRegistry().enter(id) orelse continue;
                defer api.HandleRegistry.leave(slot);
                h.invalidateQueryProducer(child);
            }
        }
        // Carry the outgoing generation's sync count forward before it is
        // replaced (see `test_cumulative_sync_calls`'s doc comment): this is
        // the one place `self.current` is ever reassigned.
        if (comptime h.builtin.is_test) if (self.current) |outgoing| {
            const outgoing_store = outgoing.owned_lite_backend.?.native_docstore.?;
            _ = self.test_cumulative_sync_calls.fetchAdd(outgoing_store.file.test_sync_calls.load(.monotonic), .monotonic);
            _ = self.test_cumulative_groups.fetchAdd(outgoing_store.test_groups_total.load(.acquire), .monotonic);
            _ = self.test_cumulative_group_members.fetchAdd(outgoing_store.test_group_members_total.load(.acquire), .monotonic);
            // A test's simulated fsync latency belongs to the path, not to
            // one generation's file.
            replacement.owned_lite_backend.?.native_docstore.?.file.test_sync_delay_ns.store(outgoing_store.file.test_sync_delay_ns.load(.monotonic), .monotonic);
        };
        // Marks the new generation subject to the `active_writer` backstop
        // (native.zig's `publishCheckpoint`/inode_lock.zig's own doc
        // comment): this is `self.current`'s only other reassignment point
        // besides `Owner.create`'s own initial open.
        replacement.owned_lite_backend.?.native_docstore.?.file.lease_managed = true;
        // The outgoing root's store is the writer (W1): nothing staged on it
        // may still be draining when it is replaced (W5).
        if (self.current) |outgoing| if (outgoing.owned_lite_backend.?.native_docstore) |outgoing_store| outgoing_store.ensureDurableForReuse();
        self.foreground_runtime_stale = false;
        self.current = replacement;
        for (self.connections.items) |connection| {
            connection.current = replacement;
            var sessions = connection.sql_sessions.valueIterator();
            while (sessions.next()) |session| session.*.handle = replacement;
        }
        self.collectRetired();
        return replacement;
    }

    /// Unverified index opens must not publish reader leases into a peer's
    /// proof. An explicitly empty index catalog opens no such readers. Inspect
    /// it through a read-only store so diagnostics need not serialize behind
    /// every owner's full graph walk after allocator-only housekeeping.
    fn unindexedRuntimeOpen(self: *Owner, tip: *native.NativeFile) !bool {
        var backend = try h.lite_backend.Handle.open(alloc, self.path, .{
            .read_only = true,
            .externally_locked = true,
            .io = h.handleLockIo(),
        });
        defer backend.deinit();
        const store = backend.native_docstore orelse return false;
        if (!std.meta.eql(store.file.header, tip.header) or
            (try store.file.file.stat(store.file.runtime())).inode != (try tip.file.stat(tip.runtime())).inode) return false;
        // Initializing a missing root is a mutation, even with no indexes.
        if (try backend.existingRootIncarnation("root") == null) return false;
        var read = try store.beginRead();
        defer read.abort();
        // API entry runs SQL recovery after opening the root. That recovery
        // can open every named namespace and publish immutable-reader leases,
        // even when the root's own index catalog is empty. Keep the exception
        // limited to root-only databases, without opening any child indexes.
        const directory = @import("../storage/lite/embedded_catalog.zig");
        const directory_bytes = read.getOwned(alloc, directory.key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (directory_bytes) |bytes| {
            defer alloc.free(bytes);
            const parsed = std.json.parseFromSlice(directory.Catalog, alloc, bytes, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return false,
            };
            defer parsed.deinit();
            if (parsed.value.tables.len != 0) return false;
        }
        // A root-only read can also mutate while reconciling a pending SQL
        // decision or DDL/FK job. Its durable markers require the same turn.
        inline for (.{
            "\x00antfly/embedded/sql-commit/v1",
            "\x00antfly/embedded/sql-ddl",
            "\x00antfly/embedded/sql-fk-publication",
        }) |key| if (try read.valueIdentity(key) != null) return false;
        const indexes = @import("../storage/db/catalog/index_manager.zig");
        inline for (.{
            .{ "\x00\x00__metadata__:indexes", indexes.deserializeCatalog, true },
            .{ "\x00\x00__metadata__:enrichments", @import("../storage/db/catalog/enrichment_catalog.zig").deserializeCatalog, false },
            .{ "\x00\x00__metadata__:resolvers", @import("../storage/db/catalog/resolver_catalog.zig").deserializeCatalog, false },
        }) |catalog| {
            const data = read.getOwned(alloc, catalog[0]) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (data) |bytes| {
                defer alloc.free(bytes);
                const configs = catalog[1](alloc, bytes) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => return false,
                };
                defer {
                    for (configs) |*config| config.deinit(alloc);
                    alloc.free(configs);
                }
                if (configs.len != 0) return false;
            } else if (catalog[2]) return false;
        }
        // Recovering an unfinished graph build can write cleanup metadata at
        // open even after its index has been removed. Keep that path guarded.
        var cursor = try read.openCursor();
        defer cursor.close();
        const row = cursor.seekAtOrAfter(indexes.graph_replacement_build_prefix) catch |err| switch (err) {
            error.NotFound => return true,
            else => return err,
        };
        return !std.mem.startsWith(u8, row.key, indexes.graph_replacement_build_prefix);
    }

    fn loadedIndexInputsUnchanged(root: *h.Handle, before: *native.NativeFile, after: *native.NativeFile) !bool {
        if (before.header.indexed_reclamation and after.header.indexed_reclamation) {
            if (std.mem.eql(u8, &before.activeCheckpoint().logical.indexes, &after.activeCheckpoint().logical.indexes)) return true;
            // Prefix comparisons dereference the old root. Its page numbers
            // are meaningful only while that checkpoint remains protected.
            const retained = for (after.header.checkpoints) |slot| {
                if (std.meta.eql(before.activeCheckpoint(), slot)) break true;
            } else false;
            if (!retained) return false;
        } else if (before.activeCheckpoint().index_catalog_root_page == after.activeCheckpoint().index_catalog_root_page) return true;
        if (!try databaseIndexInputsUnchanged(&root.db, before, after)) return false;
        var tables = root.embedded_tables.valueIterator();
        while (tables.next()) |table| if (!try databaseIndexInputsUnchanged(&table.*.db, before, after)) return false;
        return true;
    }

    fn databaseIndexInputsUnchanged(database: *h.db_mod.DB, before: *native.NativeFile, after: *native.NativeFile) !bool {
        const indexes = try database.core.listIndexes(alloc);
        defer {
            for (indexes) |*index| index.deinit(alloc);
            alloc.free(indexes);
        }
        for (indexes) |index| {
            const prefix = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}/", .{ database.core.index_manager.base_path, index.name });
            defer alloc.free(prefix);
            if (!try before.indexCatalogPrefixUnchanged(after, prefix)) return false;
        }
        return true;
    }

    pub fn collectRetired(self: *Owner) void {
        if (self.current == null) return;
        var i: usize = 0;
        while (i < self.retired.items.len) {
            const retired = self.retired.items[i];
            const retained = retained: {
                if (retired.query_borrows > 0 or retired.maintenance_references != 0 or retired.allocator_verification_references != 0 or retired.lane_pins.load(.acquire) != 0) break :retained true;
                for (self.connections.items) |connection| {
                    // attach moves this connection's registry onto the current
                    // root. Inspect its actual owner during query resume too.
                    var cursors = if (connection.attached)
                        connection.current.?.sql_cursors.valueIterator()
                    else
                        connection.sql_cursors.valueIterator();
                    while (cursors.next()) |cursor| {
                        if (cursor.*.adapter.handle == retired) break :retained true;
                    }
                }
                break :retained false;
            };
            if (retained) {
                // Still kept alive for some other reason (a lingering
                // cursor, an allocator-verification reference, ...), but a
                // `lane_pins` deferral that was the only thing blocking
                // `markRetiredReadOnlyIfIdle` at retirement time may have
                // cleared since: every `collectRetired` call (reached from
                // `refresh`, itself called under the same lock this
                // generation was retired under) is a correct place to
                // recheck it, without a separate, independently-locked call
                // site at pin-release time.
                markRetiredReadOnlyIfIdle(retired);
                i += 1;
            } else {
                _ = self.retired.swapRemove(i);
                h.closeHandle(retired);
            }
        }
    }

    fn bindGroupSourceState(self: *Owner, root: *h.Handle) !void {
        root.owned_lite_backend.?.native_docstore.?.file.defer_allocator_verification = self.options.profile == .native;
        self.configureRetirementBudget(root.owned_lite_backend.?.native_docstore.?);
        if (self.maintenance_root_incarnation != root.db.root_incarnation) {
            self.deinitMaintenanceSchedules();
            self.maintenance_schedules = .empty;
            self.maintenance_root_incarnation = root.db.root_incarnation;
        }
        root.db.borrowed_group_source_state = try self.groupSourceState(&root.db);
        var tables = root.embedded_tables.valueIterator();
        while (tables.next()) |table|
            table.*.db.borrowed_group_source_state = try self.groupSourceState(&table.*.db);
        root.lite_scheduling_owner = self;
    }

    /// Called under the connection gate by explicit maintenance as well as the
    /// background scheduler. The state outlives every refreshed DB wrapper.
    pub fn groupSourceState(self: *Owner, database: *h.db_mod.DB) !*h.db_mod.DB.GroupSourceMaintenanceState {
        const entry = try self.maintenance_schedules.getOrPut(alloc, .{
            .table_id = database.core.identity_namespace.table_id,
            .incarnation = database.root_incarnation,
        });
        if (!entry.found_existing) entry.value_ptr.* = .{};
        if (entry.value_ptr.group_sources == null) {
            const state = try alloc.create(h.db_mod.DB.GroupSourceMaintenanceState);
            state.* = .{ .allocator = alloc };
            entry.value_ptr.group_sources = state;
        }
        return entry.value_ptr.group_sources.?;
    }

    /// Cheap (O(1) per table), read-only snapshot of which durable-consumer
    /// kinds are currently registered on the live root and every embedded
    /// table under it. A zero result means a background turn has nothing
    /// registered to do: no index, enrichment, group/replication source,
    /// TTL or resolver. Durable obligations that do not yet have a live
    /// in-memory registry here (outbox, repair pins, hot standby) are left
    /// for `lite/recovery_agenda.zig` per the design doc §3.6; see the
    /// final report for why that wiring isn't included in this change.
    fn registeredDemand(self: *Owner) Demand {
        const root = self.current orelse return .none;
        var demand = tableDemand(&root.db);
        var tables = root.embedded_tables.valueIterator();
        while (tables.next()) |table| demand = demand.merge(tableDemand(&table.*.db));
        return demand;
    }

    /// `registeredDemand()` can only read the catalog off an established
    /// `self.current`; with none yet (the opportunistic lock attempt in
    /// `open()`'s `!create` path lost its race and deferred establishing
    /// one -- "initialize this runtime lazily", that call site's own doc
    /// comment), it answers `.none`. That is a true "unknown", not a true
    /// "nothing to do": a foreground call still establishes `self.current`
    /// the first time it needs one (`Connection.refresh()`/`Owner.refresh()`),
    /// but does so without ever calling `notifyMutation()`/`notifyDemand()`,
    /// so nothing wakes a maintenance loop that already decided "no demand,
    /// park indefinitely" from this same unknown state. Both of
    /// `maintenanceLoop`'s own scheduling decisions (the initial wait and
    /// `maintenanceWaitMillis`'s idle-turn fallback) must treat an
    /// unestablished `self.current` as "recheck soon", never as "nothing
    /// registered, wait forever" -- the next bounded wait's own
    /// `maintenanceTurn()` call establishes `self.current` via its own
    /// `self.refresh()` and learns the real answer.
    fn registeredDemandOrUnknown(self: *Owner) bool {
        return self.current == null or self.registeredDemand().any();
    }

    /// `registeredDemandOrUnknown` without clock-driven demand: what a
    /// write or a lane checkin can make worth an immediate turn.
    fn mutationDemandOrUnknown(self: *Owner) bool {
        return self.current == null or mutationDemand(self.registeredDemand()).any();
    }

    /// How often clock-driven demand alone (TTL cleanup) gets a turn.
    /// Matches `maintenanceWaitMillis`'s idle poll.
    const periodic_turn_interval_ns: i96 = std.time.ns_per_s;

    fn tableDemand(database: *h.db_mod.DB) Demand {
        return .{
            .index = database.core.hasManagedIndexes(),
            // `index_manager.enrichments` only ever holds a *standalone*
            // enrichment (chunk/asset, or one listed for symmetry). A
            // dense/sparse index's own embedded `generator` config carries
            // no entry there at all (see `liteEnrichmentCatalogEntryJsonAlloc`'s
            // own doc comment on `capi/db.zig`) -- `hasGeneratedEnrichmentTargets()`
            // is the one check that already walks every index kind for
            // either shape (`computeGeneratedEnrichmentTargetCacheExcluding`),
            // so without it a durable embedding backlog on a plain
            // `dense_vector`/`sparse_vector` index never registers any
            // demand at all.
            .enrichment = database.core.index_manager.enrichments.items.len != 0 or database.core.hasGeneratedEnrichmentTargets(),
            .group_source = database.core.index_manager.hasGroupSources(),
            .ttl = database.ttl_runtime != null,
            .resolver = database.core.index_manager.resolvers.items.len != 0,
            .relational = database.local_execution.relational_index_maintenance_sweep.isPending(),
        };
    }

    pub fn registeredDemandForTest(self: *Owner) Demand {
        std.debug.assert(h.builtin.is_test);
        return self.registeredDemand();
    }

    /// Wakes the maintenance worker only for a non-zero demand mask. A
    /// zero mask (nothing a background turn could help with is currently
    /// registered) leaves the worker parked and untouched: no lease
    /// acquisition, no serving-runtime refresh, no turn.
    pub fn notifyDemand(self: *Owner, mask: Demand) void {
        if (!mask.any()) return;
        if (self.worker == null) return;
        self.maintenance_pending = true;
        self.wake.set(h.handleLockIo());
    }

    /// Conservative fallback for call sites (capi/db.zig's post-commit
    /// notify sites, among others) that have not yet threaded a
    /// write-specific Demand mask through the commit path: wake for
    /// whatever this owner's registry currently reports as live.
    ///
    /// `allocator_certificate_pending` itself still follows every call:
    /// Connection.close()'s `finish_proof` reads it to decide whether an
    /// *already in-flight* worker-driven verification walk is worth letting
    /// finish rather than canceling mid-walk, which must stay keyed on "did
    /// anything mutate since that walk started", not a size threshold --
    /// the walk itself (not this write) is the expensive part to waste.
    /// `maybeRaiseAllocatorCertificateBootstrapWorthwhile` below is the
    /// separate, threshold-gated signal for certifyAllocatorOnClose's own
    /// *bootstrap* decision (starting a brand new walk at close, design doc
    /// §3.6: "allocator certificates run at close or past a size
    /// threshold, not on every write").
    pub fn notifyMutation(self: *Owner) void {
        self.allocator_certificate_pending = true;
        self.maybeRaiseAllocatorCertificateBootstrapWorthwhile();
        self.notifyDemand(mutationDemand(self.registeredDemand()));
    }

    /// The part of `registered` a write can make urgent. TTL cleanup is
    /// clock-driven (`ttl_runtime.Config.interval_ms`), and the idle
    /// worker's periodic poll (`maintenanceWaitMillis`, 1s while any demand
    /// is registered) already services it. Every Lite database outside
    /// tests runs a TTL runtime, so counting it here made every write wake a
    /// full maintenance turn that took the writer lease, and the next write
    /// queued behind that turn even with no index, enrichment or resolver.
    fn mutationDemand(registered: Demand) Demand {
        var mask = registered;
        mask.ttl = false;
        return mask;
    }

    /// Cheap and lock-free (reads fields already resident on the current
    /// root's Store, no I/O), so it costs nothing close to a real demand
    /// check, unlike certifyAllocatorOnClose's own bootstrap walk which
    /// this exists to skip on an idle/trivial-activity close.
    fn maybeRaiseAllocatorCertificateBootstrapWorthwhile(self: *Owner) void {
        if (self.allocator_certificate_bootstrap_worthwhile) return;
        // Matches certifyAllocatorOnClose's own early return: certificates
        // are a native-writer-only optimization.
        if (self.options.profile != .native or self.options.open_mode != .writer) return;
        const root = self.current orelse return;
        const backend = root.owned_lite_backend orelse return;
        const store = backend.native_docstore orelse return;
        const bytes = store.file.activeCheckpoint().page_count *| store.file.header.page_size;
        const baseline = self.allocator_certificate_baseline_bytes orelse {
            self.allocator_certificate_baseline_bytes = bytes;
            return;
        };
        if (bytes -| baseline >= allocator_certificate_threshold_bytes) self.allocator_certificate_bootstrap_worthwhile = true;
    }

    /// Scope registration reuse to admission; closing the path lease also
    /// releases idle anchors before another process can publish.
    fn withReadRegistrationBatch(self: *Owner, lease: native.PathWriterLock) native.PathWriterLock {
        var held = lease;
        if (self.options.reader_coordinator) |coordinator| {
            std.debug.assert(held.reader_registration_batch == null);
            held.reader_registration_batch = coordinator.beginRegistrationBatch();
        }
        return held;
    }

    /// Finish the current store's staged commits before its lease serves
    /// another instance. Skipped for an admission on the current root itself
    /// (`skip_reuse_drain`, hosted turns): the lease stays with the same
    /// instance, and that admission can be nested inside the very commit
    /// drive the drain would wait for.
    fn drainForReuse(self: *Owner) void {
        if (self.skip_reuse_drain) return;
        if (self.currentStore()) |store| store.ensureDurableForReuse();
    }

    fn currentStore(self: *Owner) ?*NativeStore {
        const current = self.current orelse return null;
        return current.owned_lite_backend.?.native_docstore;
    }

    /// Caller holds the owner gate. Captures and validated publications need
    /// turns alongside foreground writers, otherwise a continuously occupied
    /// kernel queue prevents index replay from ever acquiring its lease.
    /// Reclamation and detached-proof reacquisition remain nonblocking.
    /// The lease is borrowed from the writer lane (`WriterLane.borrowLease`,
    /// lite-writer-authority.md W2), so closing it parks it back there.
    fn acquireMaintenancePathLeaseAssumeLocked(self: *Owner) !native.PathWriterLock {
        if (self.stopping.load(.acquire)) return error.Canceled;
        const io = h.handleLockIo();
        const stats = &self.publication_stats.maintenance_lease;
        const started = std.Io.Clock.awake.now(io).nanoseconds;
        const default_budget = 5 * std.time.ns_per_s;
        const budget = if (h.builtin.is_test) self.test_maintenance_wait_ns orelse default_budget else default_budget;
        var borrowed = self.writer_lane.borrowLease(self, &self.gate.mutex, started + budget, false, null) catch |err| {
            if (err == error.FileBusy) {
                self.publication_stats.maintenance_queue_timeouts +|= 1;
                stats.recordBusy(@intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started)));
            }
            return err;
        };
        errdefer borrowed.lease.close();
        stats.recordAcquired(switch (borrowed.source) {
            .handoff => .handoff,
            .uncontended => .uncontended,
            .queued => .queued,
        }, if (borrowed.source == .handoff) 0 else @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started)));
        wal_trace.recordOp(.maintenance_lease_acquired);
        borrowed.lease.startHold(stats);
        if (self.stopping.load(.acquire)) return error.Canceled;
        return self.withReadRegistrationBatch(borrowed.lease);
    }

    /// Caller holds the gate. Nonblocking: reuses a lease kept for a queued
    /// thread or parked on the lane, otherwise refuses while another
    /// process is queued. Like `borrowLease`, the lease is lent by the lane.
    fn tryWriterLeaseAssumeLocked(self: *Owner) !native.PathWriterLock {
        const stats = &self.publication_stats.maintenance_lease;
        // lite-writer-authority.md W2: like the blocking acquisition, check
        // the lane out and lend its lease; never take a lease around it.
        var lease = if (self.gate.mutex.takeRetained()) |held| blk: {
            var retained = held;
            switch (self.writer_lane.beginMaintenanceCheckout()) {
                .fresh => self.writer_lane.lendLease(&retained),
                .busy => {},
                .parked, .overdue => |parked| {
                    var extra = parked;
                    extra.close();
                    self.writer_lane.lendLease(&retained);
                },
            }
            stats.recordAcquired(.handoff, 0);
            break :blk retained;
        } else switch (self.writer_lane.beginMaintenanceCheckout()) {
            .busy => return error.FileBusy,
            .parked => |held| blk: {
                var parked = held;
                self.writer_lane.lendLease(&parked);
                self.drainForReuse();
                stats.recordAcquired(.handoff, 0);
                break :blk parked;
            },
            .overdue => |held| {
                // Another process is overdue: hand the lease over instead
                // of holding it for nonblocking reclamation.
                var overdue = held;
                // Drain only; see `WriterLane.releaseBorrowed`.
                self.drainForReuse();
                overdue.close();
                self.writer_lane.cancelCheckout();
                return error.FileBusy;
            },
            .fresh => blk: {
                var fresh = native.tryFairWriterLockWithIo(alloc, h.handleLockIo(), self.path) catch |err| {
                    self.writer_lane.cancelCheckout();
                    return err;
                };
                self.drainForReuse();
                self.writer_lane.lendLease(&fresh);
                stats.recordAcquired(.uncontended, 0);
                break :blk fresh;
            },
        };
        lease.startHold(stats);
        return lease;
    }

    fn hasPendingReplay(database: *h.db_mod.DB) !bool {
        if (database.hasPendingGraphReplacements()) return true;
        const target = database.core.nextDerivedSequence();
        const indexes = try database.listDerivedReplayDebt(alloc);
        defer {
            for (indexes) |*index| index.deinit(alloc);
            alloc.free(indexes);
        }
        for (indexes) |index| if (index.catch_up_required) return true;
        const state = h.db_mod.enrichment_state;
        if (database.enrichment_runtime) |runtime| {
            const stats = runtime.stats();
            // Durable retry/notification targets also need reconciliation.
            // The runtime may know a newer target than this view's cached
            // journal watermark; ignoring it strands debt across reopen.
            if (stats.retrying or stats.applied_sequence < @max(target, stats.target_sequence)) return true;
        } else if (database.core.hasGeneratedEnrichmentTargets()) {
            const applied = try state.loadAppliedSequence(alloc, database.core.store, "generated");
            const status = try state.loadRuntimeStatus(alloc, database.core.store, "generated");
            if (status.retrying or applied < @max(target, status.target_sequence)) return true;
        }
        // Producer completion does not prove resolver/promotion completion.
        // Foreground native views need not own these runtimes, so inspect the
        // two durable watermarks instead of constructing providers on idle polls.
        if (database.core.index_manager.resolvers.items.len != 0) {
            if (database.resolution_runtime) |runtime| {
                const stats = runtime.stats();
                if (stats.applied_sequence < @max(target, stats.target_sequence)) return true;
            } else if (try state.loadAppliedSequence(alloc, database.core.store, "resolution") < target) return true;
            if (database.promotion_runtime) |runtime| {
                const stats = runtime.stats();
                if (stats.applied_sequence < @max(target, stats.target_sequence)) return true;
            } else if (try state.loadAppliedSequence(alloc, database.core.store, "promotion") < target) return true;
            // Catalog backfills also advance durable cursors without adding
            // replay records. Inspect both keys in one committed view only
            // after the cheap watermark checks have ruled out other debt.
            if (try @import("../storage/db/resolution_runtime.zig").hasDurableReresolveBacklog(database.core.store)) return true;
        }
        return false;
    }

    fn lookupEmbedding(ptr: *anyopaque, a: std.mem.Allocator, key: [32]u8) !?[]f32 {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        return self.embedding_memo.?.lookup(&self.embedding_memo_budget, a, key);
    }

    fn rememberEmbedding(ptr: *anyopaque, key: [32]u8, vector: []const f32) void {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        self.embedding_memo.?.remember(&self.embedding_memo_budget, key, vector);
    }

    // Primary transactions and index publications join the fair writer queue.
    // Their DB/index locks belong to the detached runtime; foreground refresh
    // opens a separate runtime. Reclamation admission yields on contention.
    fn enterReplacementWriter(ptr: *anyopaque) !void {
        try enterMaintenanceWriter(ptr, .index);
    }

    fn enterMaintenanceWriter(ptr: *anyopaque, scope: PublicationScope) !void {
        const root: *h.Handle = @ptrCast(@alignCast(ptr));
        const self = root.maintenance_owner.?;
        root.maintenance_admission_mutex.lockUncancelable(h.handleLockIo());
        defer root.maintenance_admission_mutex.unlock(h.handleLockIo());
        if (root.maintenance_writer_references != 0) {
            root.maintenance_writer_references += 1;
            return;
        }
        if (self.stopping.load(.acquire)) return error.Canceled;
        if (scope != .reclamation) self.gate.mutex.lockUncancelable(h.handleLockIo()) else if (!self.gate.mutex.tryLock()) return error.EnrichmentWaitCanceled;
        errdefer self.gate.mutex.unlock(h.handleLockIo());
        if (self.stopping.load(.acquire)) return error.Canceled;
        self.publication_stats.attempts +|= 1;
        errdefer self.publication_stats.refused +|= 1;
        // Bounded allocator service runs after detached publication/catalog
        // pins have been released and computes no document/index output.
        const requires_publication = scope != .reclamation and scope != .allocator_service;
        if (requires_publication and root.maintenance_publication == null) return error.Canceled;
        // A resolver publication can change index inputs before its replay
        // watermark is saved. Fence subsequent immutable index work, while
        // allowing primary transactions to read the rebased checkpoint and
        // persist that completed progress. Otherwise replay repeats its own
        // output forever without advancing its durable watermark.
        if (scope == .index) if (root.maintenance_publication) |p| if (p.index_inputs_changed) {
            self.publication_stats.index_input_refusals +|= 1;
            return error.EnrichmentWaitCanceled;
        };
        if (self.current) |current| if (needsOutcomeReconciliation(current)) return error.EnrichmentWaitCanceled;
        // See `skip_reuse_drain`'s own doc comment: this admission is for
        // `self.current` itself only in the hosted/non-independent case.
        self.skip_reuse_drain = self.current == root;
        defer self.skip_reuse_drain = false;
        var lease = (if (scope == .reclamation) self.tryWriterLeaseAssumeLocked() else self.acquireMaintenancePathLeaseAssumeLocked()) catch |err| switch (err) {
            error.FileBusy, error.WouldBlock => return error.EnrichmentWaitCanceled,
            else => return err,
        };
        errdefer lease.close();
        // The lease is lent by the lane (`lendLease`), which marked the lane
        // checked out; closing it checks the lane back in.
        const maintenance_store = root.owned_lite_backend.?.native_docstore.?;
        // The lease may have come from a path that did not drain the
        // foreground store (a gate handoff, another root's park, a fresh
        // lock). Its commits must finish before this second NativeFile on
        // the same inode starts writing; new background ones are now
        // refused because the lane is checked out.
        if (self.current) |current| if (current.owned_lite_backend.?.native_docstore) |foreground_store| {
            if (foreground_store != maintenance_store) foreground_store.ensureDurableForReuse();
        };
        maintenance_store.writer_held_by_maintenance.store(true, .release);
        errdefer maintenance_store.writer_held_by_maintenance.store(false, .release);
        // The hold starts here: admission checks and rebases run under it.
        const held_from = nowNs();
        var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
        defer probe.close();
        const file = &root.owned_lite_backend.?.native_docstore.?.file;
        if ((try probe.file.stat(probe.runtime())).inode != (try file.file.stat(file.runtime())).inode) return error.EnrichmentWaitCanceled;
        if (!std.meta.eql(probe.header, file.header) and scope == .allocator_service) {
            // Bounded allocator/orphan service computes no detached output.
            // Start it on the current ledger under the fair writer lease;
            // vacuum assessment/copy publication still rejects a moved tip.
            try root.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(probe.header);
            if (self.current) |current| try file.inheritAllocatorVerification(&current.owned_lite_backend.?.native_docstore.?.file);
            if (!file.free_pages_verified) _ = try file.inheritAllocatorCertificate();
            self.publication_stats.rebased +|= 1;
        } else if (!std.meta.eql(probe.header, file.header)) {
            if (scope == .reclamation) return error.EnrichmentWaitCanceled;
            const publication = root.maintenance_publication orelse return error.EnrichmentWaitCanceled;
            try publication.validateStorageCurrent();
            // Rebase before opening a primary transaction as well as before
            // publishing immutable index bytes. Lease acquisition and replay
            // checkpoints must not require a quiet interval between capture
            // and publication. Unchanged physical index inputs and catalog
            // identity keep detached computations valid; transactions opened
            // after this admission read the current primary checkpoint.
            // Validation can itself commit under this lease (runtime refresh,
            // SQL recovery, immutable-reader leases), so the probe above may
            // be stale. Compare and rebase against the header it produced.
            var latest = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
            defer latest.close();
            if ((try latest.file.stat(latest.runtime())).inode != (try file.file.stat(file.runtime())).inode) return error.EnrichmentWaitCanceled;
            if (file.activeCheckpoint().index_catalog_root_page != latest.activeCheckpoint().index_catalog_root_page) {
                if (!try publication.indexInputsUnchanged(file, &latest)) {
                    publication.index_inputs_changed = true;
                    if (scope == .index) {
                        self.publication_stats.index_input_refusals +|= 1;
                        return error.EnrichmentWaitCanceled;
                    }
                }
            }
            try root.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(latest.header);
            if (self.current) |current| try file.inheritAllocatorVerification(&current.owned_lite_backend.?.native_docstore.?.file);
            self.publication_stats.rebased +|= 1;
        }
        if (requires_publication) {
            const publication = root.maintenance_publication orelse return error.Canceled;
            publication.requireCurrentCatalog() catch |err| {
                publication.index_inputs_changed = true;
                return err;
            };
        }
        // A changed tip invalidates a detached proof. Retry outside the gate
        // instead of falling back to a whole-file scan in this publication.
        // Pages are allocated by the writer (the current root), never by this
        // snapshot (lite-writer-authority.md W1), so it is the writer's
        // allocator that must be proven.
        const writer_file = if (self.current) |current| &current.owned_lite_backend.?.native_docstore.?.file else file;
        if ((scope == .primary or scope == .index) and !writer_file.free_pages_verified) {
            self.publication_stats.allocator_refusals +|= 1;
            return error.EnrichmentWaitCanceled;
        }
        if (scope == .allocator_service) self.publication_stats.allocator_service_admissions +|= 1;
        root.maintenance_writer_lease = lease;
        root.maintenance_writer_references = 1;
        root.maintenance_writer_scope = @backingInt(scope);
        root.maintenance_writer_held_from = held_from;
    }

    fn leaveMaintenanceWriter(ptr: *anyopaque) void {
        const root: *h.Handle = @ptrCast(@alignCast(ptr));
        const self = root.maintenance_owner.?;
        root.maintenance_admission_mutex.lockUncancelable(h.handleLockIo());
        defer root.maintenance_admission_mutex.unlock(h.handleLockIo());
        std.debug.assert(root.maintenance_writer_references != 0);
        root.maintenance_writer_references -= 1;
        if (root.maintenance_writer_references != 0) return;
        var lease = root.maintenance_writer_lease.?;
        root.maintenance_writer_lease = null;
        // Admission alone is not a mutation. Reclamation probes also take
        // this lease; waking replay here makes each idle turn schedule itself.
        // Generated and replay publications notify at their commit boundary.
        lease.close();
        // The lease was lent by the writer lane, so closing it above parked
        // it there and checked the lane back in (lite-writer-authority.md W2).
        root.owned_lite_backend.?.native_docstore.?.writer_held_by_maintenance.store(false, .release);
        // Status readers serialize publication_stats under the gate; record
        // before releasing it.
        MaintenanceHoldSites.record(self.publication_stats.maintenance_hold_sites.writer(@fromBackingInt(@intCast(root.maintenance_writer_scope))), root.maintenance_writer_held_from);
        self.gate.mutex.unlock(h.handleLockIo());
    }

    fn closeMaintenanceSnapshot(root: *h.Handle) void {
        root.lease_snapshot = true;
        const store = root.owned_lite_backend.?.native_docstore.?;
        store.read_only = true;
        store.file.read_only = true;
        h.closeHandle(root);
    }

    pub fn needsOutcomeReconciliation(root: *h.Handle) bool {
        const store = root.owned_lite_backend.?.native_docstore.?;
        return root.sql_decision_uncertain or store.file.checkpoint_publication_uncertain or store.secret_store_uncertain;
    }

    fn inheritReclamationTelemetry(destination: anytype, source: anytype) void {
        destination.file.allocator_work.absorb(&source.file.allocator_work);
        const stats = &destination.maintenance_policy.status;
        const previous = source.maintenance_policy.status;
        if (previous.rewrite_count > stats.rewrite_count) stats.last_reclaimed_bytes = previous.last_reclaimed_bytes;
        stats.assessment_count = @max(stats.assessment_count, previous.assessment_count);
        stats.rewrite_count = @max(stats.rewrite_count, previous.rewrite_count);
    }

    fn configureRetirementBudget(self: *Owner, store: *NativeStore) void {
        store.file.retirement_work_pages = self.publication_stats.retirement_work_pages;
        store.maintenance_policy.options.retirement_work_pages = self.publication_stats.retirement_work_pages;
    }

    /// A serving runtime whose allocator cache no other thread can touch.
    fn idleServingRuntime(handle: *h.Handle) bool {
        return handle.query_borrows == 0 and handle.maintenance_references == 0 and
            handle.allocator_verification_references == 0 and handle.sql_cursors.count() == 0 and
            handle.lane_pins.load(.acquire) == 0;
    }

    /// Copy an idle maintenance snapshot's warm ledger into a cold `file` as
    /// a rebase base. Detached work mutates a referenced snapshot's ledger
    /// without the gate, so only an unreferenced snapshot may donate.
    fn copyIdleSnapshotLedger(self: *Owner, file: *native.NativeFile) !bool {
        const snapshot = self.maintenance_snapshot orelse return false;
        if (snapshot.maintenance_references != 0 or snapshot.allocator_verification_references != 0) return false;
        return file.copyAllocatorRebaseBase(&snapshot.owned_lite_backend.?.native_docstore.?.file);
    }

    /// `NativeStore.WriteTarget.resolve` for maintenance snapshots: the
    /// owner's current root store, the one writable instance on this inode.
    fn resolveWriterStore(ptr: *anyopaque) ?*NativeStore {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        const current = self.current orelse return null;
        return current.owned_lite_backend.?.native_docstore;
    }

    fn refreshMaintenanceSnapshot(self: *Owner, foreground: *h.Handle, need_database: bool) !*h.Handle {
        const file = &foreground.owned_lite_backend.?.native_docstore.?.file;
        if (self.maintenance_snapshot) |snapshot| {
            const captured = &snapshot.owned_lite_backend.?.native_docstore.?.file;
            // Header/inode equality alone is not enough: this snapshot is a
            // separate NativeFile instance from `foreground`'s own, with its
            // own in-memory allocator/ledger state, never updated by writes
            // the *foreground* instance makes. Cross-check its own last-seen
            // commit sequence against the per-inode registry's shared,
            // process-wide latest -- published by every NativeFile instance
            // on this inode after each durable commit (see
            // `NativeFile.publishCommittedSequenceToInode`) -- so a snapshot
            // another writer has since advanced past is never reused as
            // though it were current: that is exactly the self-deadlock's
            // physical-maintenance sibling, a maintenance reclaim staged
            // against pages a later foreground commit already reused.
            const captured_is_current = if (captured.inode_lock_ref.load(.acquire)) |entry|
                captured.activeCheckpoint().commit_sequence >= inode_lock.committedSequence(entry)
            else
                true;
            if (captured_is_current and (!need_database or snapshot.db_live) and std.meta.eql(file.header, captured.header) and
                (try file.file.stat(file.runtime())).inode == (try captured.file.stat(captured.runtime())).inode)
            {
                self.configureRetirementBudget(snapshot.owned_lite_backend.?.native_docstore.?);
                return snapshot;
            }
        }
        var options = self.options;
        options.externally_locked = true;
        // Immutable artifact leases protect the exact input trees. A coarse
        // inode reader here would force every peer writer to append throughout
        // detached computation, even when it touches unrelated pages.
        options.artifact_reader_fence = false;
        options.allocator_verification_owner = file;
        // Opening a database runtime commits (root incarnation, index
        // admissions), which loads the allocator. Hand it a certified warm
        // ledger first, as the serving refresh does, so the open rebases
        // instead of loading and hashing the whole ledger under the lease.
        var certified: ?native.NativeFile = null;
        defer if (certified) |*donor| donor.close();
        if (need_database and file.header.indexed_reclamation) {
            certified = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
            certified.?.allocator_cancel_token = &self.verification_cancel;
            certified.?.allocator_certificate_on_commit = true;
            if (self.maintenance_snapshot) |old| if (old.maintenance_references == 0 and old.allocator_verification_references == 0) {
                _ = try certified.?.takeAllocatorRebaseBase(&old.owned_lite_backend.?.native_docstore.?.file);
            };
            if (certified.?.ledger == null and idleServingRuntime(foreground))
                _ = try certified.?.copyAllocatorRebaseBase(&foreground.owned_lite_backend.?.native_docstore.?.file);
            if (certified.?.ledger != null and try certified.?.inheritAllocatorCertificate()) {
                options.allocator_verification_owner = &certified.?;
                options.allocator_state_donor = &certified.?;
            }
        }
        const snapshot = if (need_database) blk: {
            const opened = try api.openLiteHandleAllocWithSharedInference(alloc, self.path, options, false, h.handleLockIo(), null, foreground);
            if (comptime h.builtin.is_test) self.test_runtime_snapshot_opens += 1;
            break :blk opened;
        } else blk: {
            // Reclamation needs the physical store, never a DB/catalog/provider
            // runtime. Keep this cheap owner across idle polls of the same tip.
            var backend = try h.lite_backend.Handle.open(alloc, self.path, .{
                .externally_locked = true,
                .no_sync = options.no_sync,
                .io = h.handleLockIo(),
            });
            errdefer backend.deinit();
            backend.native_docstore.?.file.allocator_certificate_on_commit = true;
            const opened = try alloc.create(h.Handle);
            opened.* = .{ .alloc = alloc, .db = undefined, .db_live = false, .owned_lite_backend = backend, .open_mode = options.open_mode };
            break :blk opened;
        };
        snapshot.lease_snapshot = true;
        snapshot.sql_commit_recovery_needed = foreground.sql_commit_recovery_needed;
        snapshot.sql_ddl_recovery_needed = foreground.sql_ddl_recovery_needed;
        snapshot.sql_ddl_recovery_generation = foreground.sql_ddl_recovery_generation;
        const store = snapshot.owned_lite_backend.?.native_docstore.?;
        store.file.reader_coordinator = self.reader_coordinator;
        // lite-writer-authority.md W1: the snapshot computes on its own
        // pinned state; its writes are applied by the current root's store.
        store.write_target = .{ .ptr = self, .resolve = resolveWriterStore };
        store.file.forwarding_only = true;
        // W1: the snapshot reads, proves and donates allocator state, but
        // never writes its own file; its store forwards every write.
        store.file.read_only = true;
        if (self.maintenance_snapshot) |old| {
            // Carry the old snapshot's warm, verified ledger as a rebase base,
            // as the serving-runtime refresh does. Otherwise every replacement
            // starts cold, and the first allocator use under the lease pays a
            // full ledger load and digest. The base grants no authority: the
            // certificate check authenticates the rebased ledger. Take it
            // before inheriting verification flags, which it resets.
            if (old.maintenance_references == 0 and old.allocator_verification_references == 0) {
                if (try store.file.takeAllocatorRebaseBase(&old.owned_lite_backend.?.native_docstore.?.file))
                    _ = try store.file.inheritAllocatorCertificate();
            }
        }
        if (store.file.ledger == null and !store.file.free_pages_verified and idleServingRuntime(foreground)) {
            if (try store.file.copyAllocatorRebaseBase(&foreground.owned_lite_backend.?.native_docstore.?.file))
                _ = try store.file.inheritAllocatorCertificate();
        }
        try store.file.inheritAllocatorVerification(file);
        if (self.maintenance_snapshot) |old| {
            const old_file = &old.owned_lite_backend.?.native_docstore.?.file;
            try store.file.inheritAllocatorVerification(old_file);
            // Policy scheduling and telemetry belong to this connection;
            // assessment caches and page roots belong to the new generation.
            store.maintenance_policy = old.owned_lite_backend.?.native_docstore.?.maintenance_policy;
            const previous_store = old.owned_lite_backend.?.native_docstore.?;
            // These cursors belong to the path, not its checkpoint. Transfer
            // ownership before closing the old snapshot so bounded sweeps
            // make progress across continuously changing replay snapshots.
            store.artifact_cleanup_cursor = previous_store.artifact_cleanup_cursor;
            store.artifact_marker_sweep = previous_store.artifact_marker_sweep;
            previous_store.artifact_marker_sweep = .{};
            closeMaintenanceSnapshot(old);
        }
        // Carry scheduling state first, then apply the owner's current bound.
        self.configureRetirementBudget(store);
        if (comptime h.builtin.is_test) {
            const source = foreground.owned_lite_backend.?.native_docstore.?;
            if (source.test_maintenance_hook != null) {
                store.maintenance_policy = source.maintenance_policy;
                store.test_maintenance_hook = source.test_maintenance_hook;
            }
        }
        // Marks this detached snapshot subject to the `active_writer`
        // backstop (native.zig's `publishCheckpoint`/inode_lock.zig's own
        // doc comment): it is exactly the "other" instance the writer-lease
        // admission gate must keep from racing the foreground's own live
        // root (`Owner.current`) on the same inode.
        store.file.lease_managed = true;
        self.maintenance_snapshot = snapshot;
        return snapshot;
    }

    // An idle turn can publish allocator/lease cleanup after the serving
    // checkpoint has left the retained slots. Its old page IDs are then unsafe
    // to traverse. Instead adopt only this turn's fenced housekeeping result,
    // while the serving handle and its entry header are still unchanged.
    fn adoptIdleMaintenanceCheckpoint(self: *Owner, foreground: *h.Handle, entry_header: native.Header, maintenance_entry_header: native.Header, maintenance: *NativeStore) !void {
        if (!std.meta.eql(entry_header, maintenance_entry_header)) return;
        if (self.current != foreground or foreground.maintenance_references != 0 or needsOutcomeReconciliation(foreground)) return;
        const store = foreground.owned_lite_backend.?.native_docstore.?;
        if (!std.meta.eql(store.file.header, entry_header)) return;
        var lease = self.tryWriterLeaseAssumeLocked() catch |err| switch (err) {
            error.FileBusy, error.WouldBlock => return,
            else => return err,
        };
        const held_from = nowNs();
        defer MaintenanceHoldSites.record(&self.publication_stats.maintenance_hold_sites.idle_adopt, held_from);
        defer lease.close();
        var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
        defer probe.close();
        if (!std.meta.eql(probe.header, maintenance.file.header) or
            (try probe.file.stat(probe.runtime())).inode != (try store.file.file.stat(store.file.runtime())).inode or
            (try probe.file.stat(probe.runtime())).inode != (try maintenance.file.file.stat(maintenance.file.runtime())).inode) return;
        var before = store.file.activeCheckpoint();
        var after = probe.activeCheckpoint();
        before.commit_sequence = 0;
        after.commit_sequence = 0;
        before.free_map_root_page = 0;
        after.free_map_root_page = 0;
        before.page_count = 0;
        after.page_count = 0;
        before.catalog_root_page = 0;
        after.catalog_root_page = 0;
        before.index_catalog_root_page = 0;
        after.index_catalog_root_page = 0;
        // This turn ran no DB work. Reclamation admission never rebases over
        // external commits, so only lease metadata, its allocator certificate,
        // and allocator state may differ. A certificate updates the physical
        // index catalog even though no immutable index bytes changed. Requiring
        // that catalog root to match would discard our own fenced cleanup,
        // reopen the runtime, and create another set of artifact leases.
        // Primary and namespace roots must still match. The inode and exact
        // maintenance-tip checks above reject every external publication.
        if (!std.meta.eql(before, after) or store.file.header.page_size != probe.header.page_size or
            store.file.header.packed_records != probe.header.packed_records or
            store.file.header.indexed_reclamation != probe.header.indexed_reclamation) return;
        try store.refreshIndexPublicationCheckpoint(probe.header);
        try store.file.inheritAllocatorVerification(&maintenance.file);
    }

    fn acquireMaintenanceTurnGuard(self: *Owner) !?std.Io.File {
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(self.path, &digest, .{});
        const name = try std.fmt.allocPrint(alloc, ".aflite-maintenance-{s}.lock", .{std.fmt.bytesToHex(digest, .lower)});
        defer alloc.free(name);
        const path = try std.fs.path.join(alloc, &.{ std.fs.path.dirname(self.path) orelse ".", name });
        defer alloc.free(path);
        // Keep the coordination inode in place across turns and closes: unlink
        // could give concurrent owners different locks. Process death releases
        // the lease automatically, without a stale persistent leader record.
        return std.Io.Dir.cwd().createFile(h.handleLockIo(), path, .{
            .read = true,
            .truncate = false,
            .lock = .exclusive,
            .lock_nonblocking = true,
        }) catch |err| switch (err) {
            error.WouldBlock, error.FileBusy => null,
            else => return err,
        };
    }

    fn retainMaintenanceTurnAssumeLocked(self: *Owner) !bool {
        if (self.maintenance_turn_references == 0)
            self.maintenance_turn_lease = (try self.acquireMaintenanceTurnGuard()) orelse return false;
        self.maintenance_turn_references += 1;
        return true;
    }

    fn releaseMaintenanceTurnAssumeLocked(self: *Owner) void {
        std.debug.assert(self.maintenance_turn_references != 0);
        self.maintenance_turn_references -= 1;
        if (self.maintenance_turn_references == 0) {
            self.maintenance_turn_lease.?.close(h.handleLockIo());
            self.maintenance_turn_lease = null;
        }
    }

    /// Captures one table under the connection fence. The pinned runtime owns
    /// all provider/catalog state while computation runs outside that fence.
    /// Every short publication revalidates its inode and checkpoint.
    fn maintenanceTurn(self: *Owner) !bool {
        if (self.stopping.load(.acquire)) return false;
        // Independent background snapshots must not repeatedly invalidate each
        // other's index inputs between data publication and replay progress.
        // This lease coordinates background turns only; foreground callers
        // continue using their short path-writer leases during computation.
        const turn_io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(turn_io);
        self.maintenance_turn_deferred = false;
        const retained = self.retainMaintenanceTurnAssumeLocked() catch |err| {
            self.maintenance_turn_deferred = true;
            self.gate.mutex.unlock(turn_io);
            return err;
        };
        if (!retained) self.maintenance_turn_deferred = true;
        self.gate.mutex.unlock(turn_io);
        if (!retained) return true;
        defer {
            self.gate.mutex.lockUncancelable(turn_io);
            self.releaseMaintenanceTurnAssumeLocked();
            self.gate.mutex.unlock(turn_io);
        }
        if (self.options.profile == .native and self.maintenance_storage_pending) {
            // A physical maintenance error must remain observable without
            // preventing subsequent replay turns from making progress.
            self.maintenance_storage_pending = false;
            self.maintenance_storage_pending = !try self.storageMaintenanceTurn();
            return true;
        }
        const io = h.handleLockIo();
        if (self.stopping.load(.acquire)) return false;
        if (comptime h.builtin.is_test) if (self.test_before_maintenance_capture) |hook| hook.notify(hook.ptr);
        // Yield briefly to foreground work, then join the fair queue. Capture
        // stays short; queued traffic must not indefinitely postpone replay/TTL.
        if (!acquireMaintenanceCapture(&self.gate.mutex, &self.maintenance_capture_misses)) return true;
        var gate_locked = true;
        defer if (gate_locked) self.gate.mutex.unlock(io);
        if (self.stopping.load(.acquire)) return false;
        // A foreground proof owns the admission lane. Refresh can open index
        // sources and publish lease metadata, so yield before even capturing
        // a runtime. The gate protects this flag through the capture boundary.
        if (self.allocator_verification_active) return true;
        // A turn reached with no established `self.current` yet (the
        // opportunistic lock attempt in `open()`'s `!create` path lost its
        // race -- "initialize this runtime lazily", that call site's own
        // doc comment -- and `registeredDemandOrUnknown()` now schedules a
        // prompt recheck instead of parking the loop forever on that
        // unknown state) must establish one before touching it: every
        // step below, starting with `acquireMaintenancePathLeaseAssumeLocked`'s
        // own `self.current.?`, assumes it already exists. `self.gate.mutex`
        // is already held here (`acquireMaintenanceCapture` above), the
        // same precondition the later, ordinary `self.refresh()` call
        // below relies on.
        if (self.current == null) _ = try self.refresh();
        var lease = try self.acquireMaintenancePathLeaseAssumeLocked();
        const turn_held_from = nowNs();
        var clock: StepClock = .{ .last = turn_held_from };
        var lease_held = true;
        defer if (lease_held) {
            lease.close();
            MaintenanceHoldSites.record(&self.publication_stats.maintenance_hold_sites.turn_capture, turn_held_from);
        };
        const foreground = try self.refresh();
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.refresh_ns);
        if (needsOutcomeReconciliation(foreground)) return false;
        const foreground_file = &foreground.owned_lite_backend.?.native_docstore.?.file;
        // Marker service is directory-bounded and needs no allocator proof.
        // Reap crash/normal-exit leftovers even if this short-lived owner is
        // canceled before its detached graph walk can finish.
        if (self.options.open_mode == .writer)
            try self.artifact_marker_sweep.run(foreground_file);
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.marker_sweep_ns);
        if (self.options.open_mode == .writer and !foreground_file.free_pages_verified) {
            if (!self.allocator_proof_mutex.tryLock()) return true;
            defer self.allocator_proof_mutex.unlock(io);
            var proof_lease: ?native.PathWriterLock = lease;
            lease_held = false;
            defer if (proof_lease) |*held| held.close();
            try self.verifyAllocatorAssumeLocked(&proof_lease, null, false);
            return true;
        }
        try @import("sql_commit.zig").recover(foreground);
        try @import("sql_ddl.zig").recover(foreground);
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.recover_ns);
        // Catalog identities suffice for rotation and dropped-table pruning.
        // Opening every runtime here makes each bounded turn O(all indexes),
        // delaying the next group lease observation behind unrelated opens.
        try @import("tables.zig").loadCatalog(foreground);
        var live_ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer live_ids.deinit(alloc);
        try live_ids.put(alloc, foreground.db.core.identity_namespace.table_id, {});
        var ids = foreground.embedded_table_records.valueIterator();
        while (ids.next()) |id| try live_ids.put(alloc, id.*, {});
        var stale = self.maintenance_schedules.iterator();
        while (stale.next()) |entry| if (!live_ids.contains(entry.key_ptr.table_id)) {
            if (entry.value_ptr.group_sources) |state| {
                state.deinit();
                alloc.destroy(state);
            }
            self.maintenance_schedules.removeByPtr(entry.key_ptr);
        };
        const count = foreground.embedded_table_records.count() + 1;
        const ordinal = self.maintenance_table_cursor % count;
        // Advance even if this namespace cannot open, preserving sibling
        // progress and the per-table error recorded by tables.get.
        self.maintenance_table_cursor = (ordinal + 1) % count;
        var database = &foreground.db;
        var selected: ?*@import("tables.zig").Table = null;
        if (ordinal != 0) {
            var names = foreground.embedded_table_records.keyIterator();
            var name: []const u8 = undefined;
            for (0..ordinal) |_| name = names.next().?.*;
            database = @import("tables.zig").get(foreground, name) catch return true;
            selected = foreground.embedded_tables.get(name).?;
        }
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.catalog_ns);
        if (self.options.open_mode == .writer and try hasPendingReplay(database)) self.maintenance_pending = true;
        var authority_arena = std.heap.ArenaAllocator.init(alloc);
        defer authority_arena.deinit();
        if (self.options.open_mode == .writer and try @import("resolver_authority.zig").hasProjectionDebt(foreground, database, authority_arena.allocator())) self.maintenance_pending = true;
        const group_state = try self.groupSourceState(database);
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.debt_checks_ns);
        if (foreground_file.ledger) |ledger| {
            group_state.replay_budget.updatePressure(ledger.data_pending, ledger.free_pages, foreground_file.allocator_work.reuse_blocked_append_pages);
            // Bound each service publication independently, increasing useful
            // work when foreground retirement is outpacing the baseline turn.
            const work_pages: usize = @intCast(@min(512, 128 + ledger.data_pending / 8));
            self.publication_stats.retirement_work_pages = work_pages;
            self.configureRetirementBudget(foreground.owned_lite_backend.?.native_docstore.?);
        }
        // WS5/WS7: service owner-queued journal retirement here, demand-gated
        // on the watermark foreground/ack callers raised via
        // DocStore.requestRetirement -- not on a timer, so a no-consumer
        // owner with nothing ever requested never pays for this check past
        // one atomic load. Only the active owner (the path-lease holder,
        // open_mode == .writer) services retirement; a detached/read-only
        // runtime must not attempt it. Commits ride along (serviceRetirement
        // -> commitRideAlong), so this never forces its own fsync.
        //
        // (Verification note: this call initially appeared to reproduce
        // "crash sweep: full text with retirement turn"
        // (storage/lite/crash_consistency_test.zig) failing its uncrashed
        // run with OutcomeUnknown/OutcomeReconciliationRequired. Re-running
        // that exact test against the unmodified merge base (ca8c5eed8d, no
        // WS7-round-3 changes at all) reproduces the identical failure, so
        // it is pre-existing and unrelated to this call site -- not
        // something to fix here.)
        if (self.options.open_mode == .writer) {
            const store = foreground.owned_lite_backend.?.native_docstore.?;
            const requested = store.retirement_queue.requested();
            if (requested > self.retirement_drained_through) {
                if (store.serviceRetirement(true, .{})) |stats| {
                    if (stats.done) self.retirement_drained_through = requested else self.maintenance_pending = true;
                } else |_| {
                    // Transient (e.g. busy/canceled); retry on a later turn
                    // rather than losing the demand signal.
                    self.maintenance_pending = true;
                }
            }
        }
        self.publication_stats.replay_budget_table_id = database.core.identity_namespace.table_id;
        self.publication_stats.replay_budget = group_state.replay_budget.snapshot();
        const schedule_entry = try self.maintenance_schedules.getOrPut(alloc, .{
            .table_id = database.core.identity_namespace.table_id,
            .incarnation = database.root_incarnation,
        });
        if (!schedule_entry.found_existing) schedule_entry.value_ptr.* = .{};
        const schedule = schedule_entry.value_ptr;
        // Reconcile deferred opening-time index/LSM work once per namespace
        // incarnation. Foreground LSM scores also include instance-local
        // retired objects; a fresh detached runtime cannot service those and
        // must not reopen forever on their behalf.
        if (self.options.open_mode == .writer and schedule.startup_replay_pending and
            database.core.hasManagedIndexes() and database.core.nextDerivedSequence() != 0) self.maintenance_pending = true;
        // TTL is a short publication for the selected table, never a drain of
        // every namespace under one file lease.
        const now = std.Io.Clock.awake.now(io).nanoseconds;
        // Legacy receipt recovery owns durable source-replay cursors even
        // after every ordinary journal watermark is caught up. Advance one
        // bounded control page on the live coordinator, where the connection
        // and path fences already serialize journal/cursor publication. Never
        // run providers or construct replacement generations under this gate.
        if (self.options.open_mode == .writer and schedule.source_recovery_next and now >= schedule.next_source_recovery_ns) {
            const recovered = try database.repairRecoverableStartupIndexFailures(alloc, 1, .{ .source_replay_only = true });
            // A completed source cursor may still await an idle vector
            // certificate. Keep detached maintenance eligible to publish it;
            // source control itself never builds files under this fence.
            if (recovered.remaining != 0) self.maintenance_pending = true;
            if (recovered.attempted != 0 or recovered.repaired != 0) {
                schedule.source_recovery_next = false;
                self.notifyMutation();
                return true;
            }
            // Paused/future work must not turn idle connections into a hot
            // polling loop. The next selected namespace still audits it.
            schedule.next_source_recovery_ns = now + std.time.ns_per_s;
        }
        schedule.source_recovery_next = true;
        // Group fan-in is bounded control work on the current coordinator.
        // Alternate it with detached provider replay per namespace so a busy
        // root or a long backfill cannot starve a child or its consumers.
        if (self.options.open_mode == .writer and schedule.group_source_next) {
            if (try database.runGroupSourceMaintenanceSlice(group_state)) {
                schedule.group_source_next = false;
                self.notifyMutation();
                return true;
            }
        }
        schedule.group_source_next = true;
        var ttl_more = false;
        if (now >= schedule.next_ttl_ns) {
            // Each namespace owns its cleanup cadence. Failure still consumes
            // this slot and propagates for authority reconciliation; bounded
            // successful pages yield briefly without delaying other tables.
            defer {
                const config = self.options.ttl_cleanup orelse h.db_mod.ttl_runtime.Config{};
                const delay = if (ttl_more) config.page_interval_ms else config.interval_ms;
                schedule.next_ttl_ns = now + @as(i96, @max(1, delay)) * std.time.ns_per_ms;
            }
            if (database.ttl_runtime) |ttl| {
                const before = ttl.stats();
                defer recordTtlTurn(&schedule.ttl, before, ttl.stats());
                ttl.runOnce() catch |err| {
                    self.publication_stats.maintenance_errors +|= 1;
                    self.publication_stats.last_maintenance_error = @errorName(err);
                    if (schedule.last_ttl_error == null or schedule.last_ttl_error.? != @as(anyerror, err)) std.log.warn("native Lite TTL cleanup deferred table={s} err={s}", .{ if (selected) |table| table.name else "default", @errorName(err) });
                    schedule.last_ttl_error = err;
                    return err;
                };
                schedule.last_ttl_error = null;
                ttl_more = ttl.needsAnotherPage();
            }
        }
        self.maintenance_round_ttl_pending = self.maintenance_round_ttl_pending or ttl_more;
        const run_database = self.maintenance_pending;
        var turn_arena = std.heap.ArenaAllocator.init(alloc);
        defer turn_arena.deinit();
        const turn_alloc = turn_arena.allocator();
        var shadow_scopes: std.ArrayList(ShadowScope) = .empty;
        if (run_database and schedule.shadow_next) {
            const jobs = try database.graphReplacementStatusAlloc(turn_alloc);
            defer jobs.deinit();
            for (jobs.value) |job| if (job.phase == .catching_up) {
                // Construction remains detached. Cutover uses the live
                // coordinator under this short owner/path fence, so its tail
                // target and catalog swap exclude foreground appends together.
                _ = try database.finishBackgroundGraphReplacement(job.name, job.generation);
                schedule.shadow_next = false;
                self.notifyMutation();
                return true;
            };
            for (jobs.value) |job| if (job.phase == .building) {
                try shadow_scopes.append(turn_alloc, .{
                    .name = try turn_alloc.dupe(u8, job.name),
                    .generation = job.generation,
                    .prefix = try std.fmt.allocPrint(turn_alloc, "{s}/indexes/{s}/", .{ database.core.index_manager.base_path, job.name }),
                });
            };
        }
        schedule.shadow_next = shadow_scopes.items.len == 0;
        // Native maintenance owns independent catalog/provider state. Opening
        // only its selected namespace avoids rebuilding every foreground table
        // merely because inference or segment construction is still running.
        // Hosted/manual runtimes retain their caller-supplied provider borrow.
        const independent = self.options.profile == .native;
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.schedule_ns);
        const root = if (independent) try self.refreshMaintenanceSnapshot(foreground, run_database) else foreground;
        if (independent and run_database) {
            database = if (selected) |table| try @import("tables.zig").get(root, table.name) else &root.db;
            // Immutable inputs retain durable range leases throughout replay;
            // the separate primary snapshot owns its cross-process epoch.
            root.owned_lite_backend.?.native_docstore.?.artifact_reader_fence = false;
        }
        const publication_name = try alloc.dupe(u8, if (selected) |table| table.name else "default");
        defer alloc.free(publication_name);
        const index_prefix = try std.fmt.allocPrint(alloc, "{s}/", .{database.core.index_manager.base_path});
        defer alloc.free(index_prefix);
        // Replay may lazily open reads against this captured primary tip.
        // Retain its data frontier through the bounded replay window; artifact
        // leases and catalog fingerprints do not protect these primary roots.
        var source_read = if (run_database) try database.core.store.runtime_store.beginRead() else null;
        defer if (source_read) |*read| read.abort();
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.snapshot_ns);
        var root_catalog_read = if (run_database) try root.db.core.store.runtime_store.beginRead() else null;
        defer if (root_catalog_read) |*read| read.abort();
        var catalog = if (run_database) try PublicationCatalog.capture(turn_alloc, root, database) else null;
        defer if (catalog) |*proof| proof.deinit();
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.publication_catalog_ns);
        var publication: PreparedPublication = .{ .owner = self, .name = publication_name, .identity = database.core.identity_namespace, .incarnation = database.root_incarnation, .catalog = if (catalog) |*proof| proof else null, .index_prefix = index_prefix, .shadow_scopes = shadow_scopes.items, .snapshot_root = root, .snapshot_database = database, .source_read = if (source_read) |*read| read else null, .root_catalog_read = if (root_catalog_read) |*read| read else null, .relational_rows = if (database.enrichment_runtime) |runtime| runtime.config.relational_base_rows else false };
        root.maintenance_references += 1;
        // Only meaningful when `root == self.current` (the hosted/
        // non-independent path): marks the one reference this turn itself
        // holds, so a generated producer's publication callback reentering
        // `refresh` from inside this same turn (see that field's own doc
        // comment) does not mistake its own reference for a concurrent
        // user's and force a needless reopen.
        self.maintenance_turn_root = root;
        self.maintenance_activity.begin();
        root.maintenance_owner = self;
        root.maintenance_publication = &publication;
        const store = root.owned_lite_backend.?.native_docstore.?;
        const foreground_header = foreground.owned_lite_backend.?.native_docstore.?.file.header;
        const maintenance_entry_header = store.file.header;
        if (run_database) if (database.enrichment_runtime) |runtime| {
            if (self.embedding_memo == null) self.embedding_memo = memo_mod.QueryEmbeddingCache.init(alloc, io, .{ .max_bytes = embedding_memo_bytes, .ttl_ns = std.time.ns_per_min });
            runtime.dense_result_cache = .{ .ptr = self, .lookup = lookupEmbedding, .remember = rememberEmbedding };
            runtime.generated_publication = .{ .ptr = &publication, .commit = PreparedPublication.commit, .heartbeat = PreparedPublication.heartbeat, .acquire_store = PreparedPublication.acquireStore };
        };
        database.borrowed_group_source_state = group_state;
        if (run_database) database.async_context.replay_publication = .{ .ptr = &publication, .publish = PreparedPublication.publishReplay };
        store.writer_admission = .{ .ptr = root, .enter = enterMaintenanceWriter, .leave = leaveMaintenanceWriter };
        // The detached work runs without the writer lease; release it so
        // other processes can write meanwhile (parking would hold it until
        // this turn ends, since this loop is what releases parked leases).
        self.writer_lane.releaseBorrowed(self, &lease);
        MaintenanceHoldSites.record(&self.publication_stats.maintenance_hold_sites.turn_capture, turn_held_from);
        lease_held = false;
        self.gate.mutex.unlock(io);
        gate_locked = false;

        if (comptime h.builtin.is_test) if (self.test_after_capture) |hook| hook.notify(hook.ptr, root);
        const cancellation = h.db_mod.types.CancellationToken.fromAtomic(&self.stopping);
        if (run_database) database.graph_replacement_admission = .{ .ptr = root, .enter = enterReplacementWriter, .leave = leaveMaintenanceWriter };
        var maintenance_error: ?anyerror = null;
        if (run_database) store.prepareAllocatorVerification(&self.verification_cancel) catch |err| {
            maintenance_error = err;
        };
        // A shadow owns disjoint unpublished index inputs. Give it a bounded
        // turn before replay, whose serving-index publication invalidates the
        // broad capture. Unrelated serving writes may rebase this turn, while
        // changes to any selected shadow generation still reject publication.
        const projected = if (run_database and maintenance_error == null) @import("resolver_authority.zig").pullProjection(root, database, turn_alloc) catch |err| blk: {
            maintenance_error = err;
            break :blk false;
        } else false;
        var pending = if (maintenance_error != null) true else if (run_database and !projected) (ran: {
            const ran = database.runBackgroundTurnWithCancellation(cancellation, shadow_scopes.items.len != 0) catch |err| {
                maintenance_error = err;
                break :ran true;
            };
            // §3.6 no-consumer path: runBackgroundTurnWithCancellation's own
            // "did real work happen" signal (storage/db/db.zig) includes
            // the primary-commit sequence counter advancing past its
            // snapshot taken at this turn's entry. That counter advances on
            // every commit even with zero registered consumers (no journal
            // record is written, but replay_meta_next_sequence still
            // advances per design doc §3.6), so it is true on essentially
            // every turn while concurrent foreground writes continue --
            // regardless of whether anything is actually registered to
            // consume the backlog. Without a registered consumer, that
            // alone must not keep re-arming maintenance_pending for as long
            // as writes continue; this is what the capi "no-consumer path
            // 1037" regression (46 background turns/30 leases for 1,037
            // no-consumer writes) traced back to.
            break :ran ran and self.registeredDemand().any();
        }) else projected;
        if (run_database) database.graph_replacement_admission = null;
        if (run_database) if (database.enrichment_runtime) |runtime| {
            runtime.dense_result_cache = null;
            runtime.generated_publication = null;
        };
        if (run_database) database.async_context.replay_publication = null;
        root.maintenance_publication = null;
        if (source_read) |*read| read.abort();
        source_read = null;
        if (root_catalog_read) |*read| read.abort();
        root_catalog_read = null;
        // No detached computation/publication can use the catalog now. Drop
        // its checkpoint pin before physical service, which otherwise retains
        // an obsolete reader fence throughout its own reuse/vacuum attempt.
        if (catalog) |*proof| proof.deinit();
        catalog = null;
        if (independent and root.db_live) {
            // End the input fences before reclamation. Keeping them in an
            // idle runtime would needlessly prevent in-place page service.
            @import("tables.zig").closeAll(root);
            root.embedded_tables = .empty;
            root.db.closeImmutableSnapshot();
            root.db_live = false;
        }
        if (comptime h.builtin.is_test) if (self.test_before_storage_service) |hook| hook.notify(hook.ptr, root);
        store.maintainOnce(false) catch |err| switch (err) {
            // §3.6 no-consumer path: these three are enterMaintenanceWriter's
            // ordinary "a concurrent admission/replacement raced this
            // attempt" signals (see its other call sites throughout this
            // file), not real failures -- maintainOnce's own next scheduled
            // pass retries naturally. With continuous foreground writes,
            // forcing `pending = true` here turned a benign race into an
            // unconditional "retry at once" signal that, combined with
            // `maintenance_storage_pending`'s own re-arming below, kept
            // this turn's round self-sustaining for as long as writes
            // continued (the capi "no-consumer path 1037" regression).
            error.EnrichmentWaitCanceled, error.FileBusy, error.WouldBlock => {},
            else => {
                if (maintenance_error == null) maintenance_error = err;
                pending = true;
            },
        };

        self.gate.mutex.lockUncancelable(io);
        defer self.gate.mutex.unlock(io);
        if (maintenance_error) |err| {
            self.publication_stats.maintenance_errors +|= 1;
            self.publication_stats.last_maintenance_error = @errorName(err);
        } else self.publication_stats.last_maintenance_error = null;
        if (run_database and maintenance_error == null) {
            // Foreground table admission can grow the map while this turn is
            // detached. Reacquire its entry after taking the gate again.
            if (self.maintenance_schedules.getPtr(.{ .table_id = publication.identity.table_id, .incarnation = publication.incarnation })) |completed| completed.startup_replay_pending = false;
        }
        store.writer_admission = null;
        root.maintenance_owner = null;
        root.maintenance_references -= 1;
        if (self.maintenance_turn_root == root) self.maintenance_turn_root = null;
        self.maintenance_activity.finish(io);
        if (independent) inheritReclamationTelemetry(self.current.?.owned_lite_backend.?.native_docstore.?, store);
        if (!independent and self.current != root) {
            store.read_only = true;
            store.file.read_only = true;
        }
        const forwarded_writes = independent and !std.meta.eql(maintenance_entry_header, store.file.header);
        if (forwarded_writes) self.foreground_runtime_stale = true;
        const changed = forwarded_writes or if (independent)
            !std.meta.eql(self.current.?.owned_lite_backend.?.native_docstore.?.file.header, store.file.header)
        else
            self.current != root;
        // Retaining the serving runtime does not hide physical progress. Run
        // another bounded storage turn before declaring cleanup quiescent.
        if (independent and !run_database) try self.adoptIdleMaintenanceCheckpoint(foreground, foreground_header, maintenance_entry_header, store);
        if (independent and run_database) self.maintenance_storage_pending = true;
        // A moved physical tip requires a prompt refresh, not another DB
        // replay pass. Reader leases/index housekeeping also move that tip;
        // treating them as enrichment debt makes idle runtime opens perpetual.
        // The next capture checks the durable journal for foreground work.
        // A primary rebase may have fenced immutable index work without
        // making the replay driver itself fail. Retry on a fresh snapshot
        // even when its cached replay target looked complete this turn.
        pending = pending or projected or publication.index_inputs_changed;
        self.maintenance_round_pending = self.maintenance_round_pending or pending;
        const ttl_pending = self.maintenance_round_ttl_pending;
        if (self.maintenance_table_cursor == 0) {
            self.publication_stats.maintenance_rounds +|= 1;
            self.maintenance_round_ttl_pending = false;
            // Mutations arriving during the detached turn still wake the owner;
            // replay debt is inspected again under the next capture fence.
            self.maintenance_pending = self.maintenance_round_pending;
            self.maintenance_round_pending = false;
        }
        self.collectRetired();
        // §3.6 no-consumer path: a bare tip move (`changed`) with nothing
        // else pending exists only to prompt a fresh capture for a
        // *registered* consumer (replay/catch-up/TTL watching this owner).
        // With no durable consumer at all, continuous foreground writes
        // would otherwise move the tip on essentially every idle turn,
        // keeping this loop at maintenanceWaitMillis's 100ms "pending"
        // cadence for as long as writes continue -- the whole reason the
        // no-consumer capi test ("no-consumer path 1037 real primary
        // writes") saw dozens of background turns/leases despite
        // registeredDemand() never going non-empty. Gate `changed` on
        // demand so a no-consumer owner's idle turn still reports "nothing
        // to do" and the loop parks via maintenanceWaitMillis's
        // no_scheduled_wait, exactly as it would if the tip had not moved.
        const changed_matters = changed and self.registeredDemand().any();
        return self.maintenance_storage_pending or self.maintenance_pending or self.maintenance_table_cursor != 0 or changed_matters or ttl_pending;
    }

    /// A storage-only capture has no provider calls or long-lived replay read
    /// pins. Every write still uses the usual inode/checkpoint admission, and
    /// prepared vacuum images keep their existing publication validation.
    fn storageMaintenanceTurn(self: *Owner) !bool {
        const io = h.handleLockIo();
        if (self.stopping.load(.acquire)) return true;
        if (comptime h.builtin.is_test) if (self.test_before_maintenance_capture) |hook| hook.notify(hook.ptr);
        if (!acquireMaintenanceCapture(&self.gate.mutex, &self.maintenance_capture_misses)) return false;
        var gate_locked = true;
        defer if (gate_locked) self.gate.mutex.unlock(io);
        if (self.stopping.load(.acquire)) return true;
        // Keep the scheduled storage turn pending while foreground admission
        // proves the tip; even its runtime refresh can publish index leases.
        if (self.allocator_verification_active) return false;
        var lease = try self.acquireMaintenancePathLeaseAssumeLocked();
        const turn_held_from = nowNs();
        var lease_held = true;
        defer if (lease_held) {
            lease.close();
            MaintenanceHoldSites.record(&self.publication_stats.maintenance_hold_sites.storage_turn, turn_held_from);
        };
        const foreground = try self.refresh();
        if (needsOutcomeReconciliation(foreground)) return true;
        // A changed tip can invalidate a previously scheduled storage turn.
        // Yield it to the shared proof lane before bounded reclamation can
        // require reuse, rather than proving a second pool under its gate.
        if (!foreground.owned_lite_backend.?.native_docstore.?.file.free_pages_verified) return true;
        const entry_header = foreground.owned_lite_backend.?.native_docstore.?.file.header;
        const root = try self.refreshMaintenanceSnapshot(foreground, false);
        const store = root.owned_lite_backend.?.native_docstore.?;
        const maintenance_entry_header = store.file.header;
        root.maintenance_references += 1;
        self.maintenance_activity.begin();
        root.maintenance_owner = self;
        root.maintenance_publication = null;
        store.writer_admission = .{ .ptr = root, .enter = enterMaintenanceWriter, .leave = leaveMaintenanceWriter };
        // The detached work runs without the writer lease; release it so
        // other processes can write meanwhile (parking would hold it until
        // this turn ends, since this loop is what releases parked leases).
        self.writer_lane.releaseBorrowed(self, &lease);
        MaintenanceHoldSites.record(&self.publication_stats.maintenance_hold_sites.storage_turn, turn_held_from);
        lease_held = false;
        self.gate.mutex.unlock(io);
        gate_locked = false;
        const result = store.maintainOnce(false);
        self.gate.mutex.lockUncancelable(io);
        gate_locked = true;
        store.writer_admission = null;
        root.maintenance_owner = null;
        root.maintenance_references -= 1;
        self.maintenance_activity.finish(io);
        inheritReclamationTelemetry(self.current.?.owned_lite_backend.?.native_docstore.?, store);
        try self.adoptIdleMaintenanceCheckpoint(foreground, entry_header, maintenance_entry_header, store);
        // Same benign-race exception as the full turn's maintainOnce call
        // (see that call site's comment): a concurrent admission/
        // replacement racing this attempt is not a real failure.
        result catch |err| switch (err) {
            error.EnrichmentWaitCanceled, error.FileBusy, error.WouldBlock => {},
            else => return err,
        };
        return true;
    }

    pub fn maintenanceTurnForTest(self: *Owner) !bool {
        std.debug.assert(h.builtin.is_test);
        return self.maintenanceTurn();
    }

    fn maintenanceLoop(self: *Owner) void {
        const io = h.handleLockIo();
        // No durable consumer is known yet at spawn, so start parked; the
        // first notifyMutation()/notifyDemand() with a non-zero mask (or an
        // explicit stop) wakes this immediately.
        var next_wait_ms: i64 = if (self.registeredDemandOrUnknown()) 1000 else no_scheduled_wait;
        var earliest_turn_ns: i96 = 0;
        var next_periodic_turn_ns: i96 = 0;
        while (!self.stopping.load(.acquire)) {
            // Bound the wait while `WriterLane.checkin` still has a lease
            // parked: `maintainParkedLease` is what actually enforces
            // `foreign_fairness_quantum_ns`/`parked_idle_release_ns` for an
            // idle holder (see its own doc comment) -- but only while this
            // loop keeps calling it. Once it returns `null` (nothing
            // parked, including right after it releases one), this stops
            // influencing the wait at all, so an idle Owner goes back to
            // its ordinary demand-driven schedule instead of polling
            // forever on an empty lane.
            self.loop_may_park.store(true, .seq_cst);
            if (self.maintainParkedWriterLaneMs()) |parked_wait_ms| {
                next_wait_ms = if (next_wait_ms < 0) parked_wait_ms else @min(next_wait_ms, parked_wait_ms);
            }
            if (next_wait_ms < 0) {
                self.wake.waitUncancelable(io);
                self.loop_may_park.store(false, .seq_cst);
            } else {
                self.loop_may_park.store(false, .seq_cst);
                self.wake.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(next_wait_ms), .clock = .awake } }) catch {};
            }
            self.wake.reset();
            if (self.stopping.load(.acquire)) break;
            const now = std.Io.Clock.awake.now(io).nanoseconds;
            if (next_wait_ms >= 0 and now < earliest_turn_ns) {
                // Coalesce mutation wakes between rounds. Every selected
                // namespace still yields the gate to queued foreground calls.
                next_wait_ms = @intCast(@divCeil(earliest_turn_ns - now, std.time.ns_per_ms));
                continue;
            }
            // `maintenanceTurn()` unconditionally acquires a real
            // maintenance path lease for a `.native` profile owner
            // (`acquireMaintenancePathLeaseAssumeLocked`, gated only on
            // `independent = self.options.profile == .native`) before it
            // ever looks at demand, replay debt or retirement -- so merely
            // *calling* it costs a cross-process lease acquisition even
            // when it finds nothing. `WriterLane.maintainParkedWriterLaneMs`
            // re-arms `next_wait_ms` to a finite wait on every iteration
            // while a lease stays parked (the common case for back-to-back
            // same-process writes), which otherwise wakes this loop on that
            // cadence for as long as writes keep landing, regardless of
            // whether anything but that parked lease itself needs
            // attention -- already fully serviced, inline, by the call
            // above. Skip the turn entirely, not just the counting, when
            // nothing concrete says one is needed: no freshly scheduled
            // follow-up (`earliest_turn_ns`), no registered catalog demand
            // (or unestablished `self.current`), no owner-level pending
            // flag (replay debt, retirement, startup backfill -- all set
            // by a *previous* turn or an explicit notifyMutation/notifyDemand,
            // never only discovered by this one), no storage reclamation in
            // flight, and no mid-round table cursor. A genuinely idle,
            // no-consumer owner never has any of these set (its own
            // foreground writes only wake it when `registeredDemand().any()`
            // is true -- `notifyDemand`'s own doc comment), so this cannot
            // starve real work, only the lease-parking timer's own spurious
            // wakes.
            //
            // Clock-driven demand (TTL cleanup, which every Lite database
            // outside tests registers) does not count here: a checkin
            // wake on every write would otherwise run a full turn, which
            // takes the writer lease, between consecutive writes. It gets
            // at most one turn per `periodic_turn_interval_ns` instead.
            const periodic_due = self.registeredDemand().ttl and now >= next_periodic_turn_ns;
            if (earliest_turn_ns == 0 and !self.mutationDemandOrUnknown() and !periodic_due and !self.maintenance_pending and
                !self.maintenance_storage_pending and self.maintenance_table_cursor == 0)
            {
                next_wait_ms = self.maintenanceWaitMillis(false);
                earliest_turn_ns = 0;
                continue;
            }
            next_periodic_turn_ns = now + periodic_turn_interval_ns;
            const pending = self.maintenanceTurn() catch |err| blk: {
                // Capture/control failures otherwise disappear before the
                // detached replay diagnostics can observe them.
                self.gate.mutex.lockUncancelable(io);
                self.publication_stats.maintenance_errors +|= 1;
                self.publication_stats.last_maintenance_error = @errorName(err);
                self.gate.mutex.unlock(io);
                break :blk true;
            };
            if (comptime h.builtin.is_test) {
                if (pending) _ = self.test_background_turn_count.fetchAdd(1, .release);
            }
            // A scheduling quantum belongs to a fair round, not each idle
            // namespace. Otherwise 45 unrelated tables add 4.5 seconds to
            // every activation, delivery, claim and publication observation.
            next_wait_ms = self.maintenanceWaitMillis(pending);
            // Only a turn that left work behind schedules a follow-up. After
            // an idle turn this stays 0: a nonzero `earliest_turn_ns` (it
            // used to be set to "now" here) disables the idle skip above,
            // so every later lane-checkin wake ran a full turn.
            earliest_turn_ns = if (next_wait_ms < 0 or !pending) 0 else std.Io.Clock.awake.now(io).nanoseconds + @as(i96, next_wait_ms) * std.time.ns_per_ms;
        }
    }

    fn maintenanceWaitMillis(self: *Owner, pending: bool) i64 {
        // Pending storage and a mid-round cursor normally run immediately,
        // but neither can progress while a peer owns the background turn.
        if (self.maintenance_turn_deferred) return 100;
        // A mid-round cursor normally advances immediately. While admission
        // owns the proof lane, that same cursor cannot advance; do not spin
        // through path leases, marker scans and the foreground gate at zero delay.
        if (!self.allocator_proof_mutex.tryLock()) return 100;
        self.allocator_proof_mutex.unlock(h.handleLockIo());
        if (self.maintenance_storage_pending or self.maintenance_table_cursor != 0) return 0;
        if (pending) return 100;
        // Nothing left from this turn. With no durable consumer registered
        // at all, a fixed-interval poll could only ever rediscover the same
        // "nothing to do" answer that a future notifyDemand() wake already
        // delivers, so idle turns stop polling instead of repeating every second.
        // (`registeredDemandOrUnknown`'s own doc comment: an unestablished
        // `self.current` must recheck soon, never park indefinitely.)
        return if (self.registeredDemandOrUnknown()) 1000 else no_scheduled_wait;
    }

    pub fn maintenanceWaitMillisForTest(self: *Owner, pending: bool) i64 {
        std.debug.assert(h.builtin.is_test);
        return self.maintenanceWaitMillis(pending);
    }

    /// Drives `WriterLane.maintainParkedLease` from `maintenanceLoop`,
    /// converting its nanosecond bound to the milliseconds
    /// `maintenanceLoop`'s own wait is expressed in.
    fn maintainParkedWriterLaneMs(self: *Owner) ?i64 {
        const remaining_ns = self.writer_lane.maintainParkedLease() orelse return null;
        return @intCast(@divCeil(remaining_ns, std.time.ns_per_ms));
    }

    pub fn maintainParkedWriterLaneMsForTest(self: *Owner) ?i64 {
        std.debug.assert(h.builtin.is_test);
        return self.maintainParkedWriterLaneMs();
    }

    /// Caller holds the owner gate, path lease and proof mutex. Return with
    /// the gate held on every path; proof teardown releases the path lease.
    /// Runtime pins keep the reader's borrowed allocator/executor alive while
    /// the graph walk runs with both publication locks released.
    /// `skip_lane_wait`: forwarded to the writer-lease reacquisition below
    /// (see `Connection.refresh`'s own doc comment on its identically
    /// named parameter) -- `true` only when `connection` is the same
    /// `Connection` whose own `WriterLane.checkout` is still on the stack
    /// above this call, `false` for every other caller (background
    /// maintenance, or a direct `connection.acquire()`-based caller that
    /// never touched the lane at all).
    fn verifyAllocatorAssumeLocked(self: *Owner, lease: *?native.PathWriterLock, connection: ?*Connection, skip_lane_wait: bool) !void {
        const io = h.handleLockIo();
        if (!try self.retainMaintenanceTurnAssumeLocked()) return error.FileBusy;
        defer self.releaseMaintenanceTurnAssumeLocked();
        const foreground = try self.refresh();
        const file = &foreground.owned_lite_backend.?.native_docstore.?.file;
        if (needsOutcomeReconciliation(foreground)) return error.OutcomeReconciliationRequired;
        if (file.transaction_header != null or file.durable_header != null) return error.FileBusy;
        if (file.free_pages_verified) return;
        if (self.allocator_proof_receipt) |receipt| {
            self.allocator_proof_receipt = null;
            // The same fence as the transfer below: under the writer lease,
            // the on-disk inode must still be the live one, and the free
            // map the proof covers (not the whole header -- see
            // `NativeFile.allocatorProofIdentity`'s own doc comment: a
            // redo-only commit advances the whole header on every commit
            // without ever touching the free map) must still be the one
            // on disk.
            var probe = try native.NativeFile.openWithIo(alloc, io, self.path, .{ .read_only = true, .internal_reader = true });
            defer probe.close();
            if (std.meta.eql(native.NativeFile.allocatorProofIdentity(file.header), native.NativeFile.allocatorProofIdentity(probe.header)) and
                (try file.file.stat(file.runtime())).inode == (try probe.file.stat(probe.runtime())).inode)
            {
                try file.inheritAllocatorProofReceipt(receipt);
                if (file.free_pages_verified) {
                    self.publication_stats.allocator_proof_receipt_reuses +|= 1;
                    return;
                }
            }
        }
        // An idle maintenance snapshot usually holds a warm, verified ledger
        // for an ancestor of this tip. Copying it turns the proof walk the
        // caller would wait on into a journal rebase plus certificate check.
        if (try self.copyIdleSnapshotLedger(file) and try file.inheritAllocatorCertificate() and file.free_pages_verified)
            return;
        foreground.allocator_verification_references += 1;
        defer {
            foreground.allocator_verification_references -= 1;
            self.collectRetired();
        }
        const reader = try file.captureAllocatorVerificationSnapshot(&self.verification_cancel);
        self.allocator_verification_started = true;
        std.debug.assert(!self.allocator_verification_active);
        self.allocator_verification_active = true;
        defer self.allocator_verification_active = false;
        const proof_alloc = reader.allocator;
        defer {
            // Reader/ledger teardown can release substantial cached metadata.
            // Keep it outside both publication locks, retaining the runtime
            // pin until its borrowed allocator and executor are no longer used.
            if (lease.*) |*held| held.close();
            lease.* = null;
            self.gate.mutex.unlock(io);
            if (comptime h.builtin.is_test) if (self.test_before_allocator_reader_close) |hook| hook.notify(hook.ptr, reader);
            reader.close();
            proof_alloc.destroy(reader);
            self.gate.mutex.lockUncancelable(io);
        }
        // The walk runs without the writer lease: release it rather than
        // parking it, so other processes can write meanwhile.
        self.writer_lane.releaseBorrowed(self, &lease.*.?);
        lease.* = null;
        self.gate.mutex.unlock(io);
        if (comptime h.builtin.is_test) if (self.test_after_allocator_capture) |hook| hook.notify(hook.ptr, reader);
        const proof_started = std.Io.Clock.awake.now(io).nanoseconds;
        const verified = reader.completeAllocatorVerification();
        const proof_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - proof_started));
        self.gate.mutex.lockUncancelable(io);
        self.publication_stats.allocator_proof_walks +|= 1;
        self.publication_stats.allocator_proof_ns +|= proof_ns;
        self.publication_stats.allocator_proof_max_ns = @max(self.publication_stats.allocator_proof_max_ns, proof_ns);
        // The proof is this call's own work, not waiting: it does not use up
        // the busy budget carried to later re-entries of the same call.
        if (connection) |caller| {
            if (caller.admission_active_sink) |sink| sink.* +|= proof_ns;
            // Credit it now, before reacquiring the lease below, so the walk
            // does not shorten this attempt's own wait either.
            if (caller.admission_deadline_ns) |*deadline| deadline.* +|= proof_ns;
        }
        if (verified) |_| {} else |_| {
            self.publication_stats.allocator_proof_failures +|= 1;
        }
        try verified;
        // If reacquiring the lease below yields busy, the next attempt reuses
        // this proof instead of repeating the walk.
        self.allocator_proof_receipt = try reader.allocatorProofReceipt();
        // A foreground operation has not started its mutation yet. Preserve
        // its connection-local busy policy when another process wins the
        // writer lease during the detached proof. Background turns stay
        // nonblocking and yield to that writer instead of consuming its budget.
        lease.* = if (connection) |caller| try caller.acquirePathLeaseAssumeLocked(skip_lane_wait) else self.withReadRegistrationBatch(try self.tryWriterLeaseAssumeLocked());
        var probe = try native.NativeFile.openWithIo(alloc, io, self.path, .{ .read_only = true, .internal_reader = true });
        defer probe.close();
        if (self.current) |current| {
            // A foreground publication may have failed while the walk ran.
            // Retrying the same immutable proof cannot reconcile that outcome.
            if (needsOutcomeReconciliation(current)) return error.OutcomeReconciliationRequired;
            const live = &current.owned_lite_backend.?.native_docstore.?.file;
            if (live.transaction_header != null or live.durable_header != null) return error.FileBusy;
            // The walk ran with both publication locks released (above),
            // so an ordinary redo-only commit or two may have landed in
            // the meantime -- compare the allocator-relevant identity, not
            // the whole header, for the same reason as the stashed-receipt
            // check above.
            if (std.meta.eql(native.NativeFile.allocatorProofIdentity(live.header), native.NativeFile.allocatorProofIdentity(probe.header)) and
                (try live.file.stat(live.runtime())).inode == (try probe.file.stat(probe.runtime())).inode)
                try live.inheritAllocatorVerification(reader);
        }
        self.allocator_proof_receipt = null;
    }

    fn deinitMaintenanceSchedules(self: *Owner) void {
        var schedules = self.maintenance_schedules.valueIterator();
        while (schedules.next()) |schedule| if (schedule.group_sources) |state| {
            state.deinit();
            alloc.destroy(state);
        };
        self.maintenance_schedules.deinit(alloc);
    }

    fn certifyAllocatorOnClose(self: *Owner) !void {
        if (self.options.profile != .native or self.options.open_mode != .writer) return;
        const io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(io);
        defer self.gate.mutex.unlock(io);
        var lease: ?native.PathWriterLock = try self.tryWriterLeaseAssumeLocked();
        defer if (lease) |*held| held.close();
        for (0..2) |attempt| {
            var proof_bytes: u64 = 0;
            var can_prove_current = false;
            {
                var tip = try native.NativeFile.openWithIo(alloc, io, self.path, .{ .read_only = true, .internal_reader = true });
                defer tip.close();
                // Revision-3 files cannot carry this certificate. Their
                // conversion belongs to maintenance, never shutdown.
                if (!tip.header.indexed_reclamation) return;
                // The walk checks both recovery slots. Bound the physical
                // file, including a larger inactive slot or unpublished tail.
                proof_bytes = (try tip.file.stat(tip.runtime())).size;
                for ([_]?*h.Handle{ self.current, self.maintenance_snapshot }) |candidate| {
                    const root = candidate orelse continue;
                    const store = root.owned_lite_backend.?.native_docstore.?;
                    store.mutex.lock();
                    defer store.mutex.unlock();
                    const file = &store.file;
                    // Compare the allocator-relevant identity, not the whole
                    // header: an intervening redo-only commit must not make
                    // this candidate look foreign to `tip` when the free map
                    // it would certify is still exactly the one on disk.
                    if (file.read_only or !std.meta.eql(native.NativeFile.allocatorProofIdentity(file.header), native.NativeFile.allocatorProofIdentity(tip.header)) or
                        (try file.file.stat(file.runtime())).inode != (try tip.file.stat(tip.runtime())).inode) continue;
                    if (!file.free_pages_verified) {
                        if (self.current == root and !needsOutcomeReconciliation(root) and
                            file.transaction_header == null and file.durable_header == null) can_prove_current = true;
                        continue;
                    }
                    try file.sync();
                    // `sync()` can itself force a durability-barrier commit,
                    // which (perf/lwa-grouped-stall's `refreshIfWriterBaseStale`)
                    // may discover this instance's own writer base is behind
                    // the inode registry's latest committed sequence and
                    // adopt the newer on-disk header -- invalidating
                    // `free_pages_verified` as part of that adoption
                    // (`adoptIndexPublicationHeader`'s own doc comment).
                    // The check above is no longer valid once that happens:
                    // `certifyAllocator`'s own precondition would otherwise
                    // panic on a verification this exact call just undid.
                    // Treat it the same as never having been verified.
                    if (!file.free_pages_verified) {
                        if (self.current == root and !needsOutcomeReconciliation(root) and
                            file.transaction_header == null and file.durable_header == null) can_prove_current = true;
                        continue;
                    }
                    _ = try file.certifyAllocator();
                    return;
                }
            }
            if (attempt != 0 or !can_prove_current or !self.allocator_certificate_bootstrap_worthwhile or self.allocator_verification_started or
                proof_bytes > close_allocator_proof_max_bytes) return;
            // Registry close has drained foreground calls and joined the
            // worker. Bootstrap a small, never-scheduled pool, but never
            // repeat a failed/canceled walk or start a large one at close.
            // Never rebuild a runtime for a foreign publication just to seal it.
            // Reuse the detached proof path, releasing both publication locks
            // during the walk and reader teardown. A moving external tip may
            // still refuse certification; never retry indefinitely at close.
            self.verification_cancel.requested.store(false, .release);
            self.allocator_proof_mutex.lockUncancelable(io);
            defer self.allocator_proof_mutex.unlock(io);
            try self.verifyAllocatorAssumeLocked(&lease, null, false);
            if (lease == null) lease = try self.tryWriterLeaseAssumeLocked();
        }
    }

    fn deinit(self: *Owner) void {
        if (self.worker) |worker| worker.join();
        // `WriterLane.checkin` parks the kernel-level path lease
        // (`native.PathWriterLock`, a `flock`-style advisory lock held open
        // on its own file descriptor) between writers instead of releasing
        // it, so a same-Store writer can reclaim it for free (see
        // `WriterLane`'s doc comment). That is only ever safe while this
        // Owner -- and so this lane -- stays alive to hand it off again.
        // The last Connection closing means nobody ever will: leaving a
        // parked (or, if a lane-managed call is somehow still mid-flight
        // here, checked-out) lease open leaks the fd and keeps the kernel
        // lease held past this process's own knowledge of it, so a later
        // `open()` of the same path in this same process -- even from a
        // brand new Owner -- can never acquire a fresh lease of its own:
        // `flock` is per-descriptor, not per-process, so it just queues
        // behind its own abandoned lock forever, eventually timing out
        // with `FileBusy`/`WouldBlock` on every caller that needed it.
        self.writer_lane.mutex.lockUncancelable(h.handleLockIo());
        self.writer_lane.closing = true;
        // `closing = true` (just set, under the same mutex) stops
        // `WriterLane.pinForLocalRead` from starting any new pin from
        // here on; wait out whichever pins already exist -- same-process
        // synthetic reads relying on the lease this call is about to
        // close staying held until they finish (`local_reader_activity`'s
        // own doc comment) -- before closing it and freeing this lane
        // out from under them.
        self.writer_lane.local_reader_activity.wait(h.handleLockIo(), &self.writer_lane.mutex);
        self.writer_lane.mutex.unlock(h.handleLockIo());
        if (self.writer_lane.parked) |*lease| lease.close();
        self.writer_lane.parked = null;
        if (self.writer_lane.held_lease) |*lease| lease.close();
        self.writer_lane.held_lease = null;
        self.certifyAllocatorOnClose() catch |err| {
            // Failure only loses the optimization; the previous checkpoint
            // remains recoverable and the next owner uses ordinary proof.
            std.log.warn("native Lite allocator certificate deferred: {s}", .{@errorName(err)});
        };
        self.artifact_marker_sweep.deinit(h.handleLockIo());
        if (self.maintenance_snapshot) |snapshot| closeMaintenanceSnapshot(snapshot);
        if (self.embedding_memo) |*memo| memo.deinit(&self.embedding_memo_budget);
        for (self.retired.items) |retired| h.closeHandle(retired);
        if (self.current) |current| {
            // This is the owner's own, still-authoritative generation going
            // away for good (the last Connection closed) -- not a
            // generation retired because another owner already committed a
            // newer one (see the `previous.lease_snapshot = true` sites
            // above, where discarding pending writes is correct because a
            // newer durable generation already exists). There is no newer
            // generation here, so any work still riding along on this
            // owner's native file (a background text-segment merge, a WS5
            // retirement turn, a deferred progress-v2 record -- anything
            // committed through `setDurable(false)`/`.ride_along` that has
            // not yet piggybacked on a later durable sync) must be flushed
            // durable before teardown. Without this, `closeHandle` would
            // route through `DB.closeImmutableSnapshot()` (via
            // `handle.lease_snapshot`), which sets `discard_storage_writes`
            // and skips `deinitWrapperState`'s own close-time
            // `syncStore(true)` (gated on `open_mode.allowsIndexWorkers()`,
            // false once `closeImmutableSnapshot` switches to
            // `.query_readonly`) -- silently dropping the most recently
            // acknowledged write's ride-along effects on an entirely
            // ordinary, crash-free `close()`.
            //
            // `DB.sync`/`DB.syncIndexes` are the wrong tool here: both
            // forward through `backend_erased`'s type-erased `sync(ptr,
            // force)`, which is a silent no-op unless the concrete backend
            // Handle declares its own `sync` method -- Lite's
            // `storage/lite/backend.zig` `Backend` does not, so those calls
            // would do nothing. The actual durability barrier for Lite's
            // native file is `NativeFile.sync()` (publishes
            // `durable_header` if a deferred/ride-along commit left one
            // pending), reached here through `Store.syncIndexContents()`
            // (storage/lite/docstore.zig) -- named for its other caller
            // (the LSM virtual-filesystem sync hook in index_storage.zig)
            // but generic: "a durability barrier [that] allocates no pages
            // and requires no allocator admission, retirement service, or
            // synthetic mutation transaction."
            if (!current.lease_snapshot and current.owned_lite_backend != null and h.liteOpenModeCanWrite(current.open_mode)) {
                if (current.owned_lite_backend.?.native_docstore) |store| {
                    store.syncIndexContents() catch |err| {
                        std.log.warn("owner close-time sync deferred: {s}", .{@errorName(err)});
                    };
                }
            }
            current.lease_snapshot = true;
            h.closeHandle(current);
        }
        self.retired.deinit(alloc);
        self.connections.deinit(alloc);
        self.deinitMaintenanceSchedules();
        self.reader_coordinator.release();
        if (self.owned_ttl_owner_id) |id| alloc.free(id);
        if (self.owned_models_dir) |directory| alloc.free(directory);
        self.path_lock_io.deinit();
        @import("io_lifetime.zig").release();
        alloc.destroy(self);
    }
};

fn compatible(a: api.LiteResolvedOpenOptions, b: api.LiteResolvedOpenOptions) bool {
    var left = a;
    var right = b;
    // This controls an individual caller's admission deadline, not storage.
    left.busy_timeout_ms = 0;
    right.busy_timeout_ms = 0;
    left.reader_coordinator = null;
    right.reader_coordinator = null;
    if (left.ttl_cleanup) |*ttl| {
        const other = right.ttl_cleanup orelse return false;
        if (!std.mem.eql(u8, ttl.owner_id, other.owner_id)) return false;
        ttl.owner_id = "";
        right.ttl_cleanup.?.owner_id = "";
    }
    if (left.inference.models_dir) |directory| {
        const other = right.inference.models_dir orelse return false;
        if (!std.mem.eql(u8, directory, other)) return false;
        left.inference.models_dir = null;
        right.inference.models_dir = null;
    }
    return std.meta.eql(left, right);
}

pub const Connection = struct {
    path: []const u8,
    options: api.LiteResolvedOpenOptions,
    gate: *PathGate,
    owner: *Owner,
    mutex: QueuedMutex = .{},
    // Scoped to C API admission while mutex is held. Reacquisition after a
    // detached allocator proof consumes the same request budget.
    admission_deadline_ns: ?i96 = null,
    /// Where the admitting call accumulates its own active work (allocator
    /// proof), excluded from the waiting budget carried to re-entries. Set
    /// only while that call holds `mutex`, like `admission_deadline_ns`, so
    /// one call's work is never credited to another.
    admission_active_sink: ?*u64 = null,
    current: ?*h.Handle = null,
    attached: bool = false,
    sql_sessions: @FieldType(h.Handle, "sql_sessions") = .empty,
    next_sql_session_id: u64 = 1,
    sql_cursors: @FieldType(h.Handle, "sql_cursors") = .empty,
    next_sql_cursor_id: u64 = 1,
    table_handles: @FieldType(h.Handle, "table_handles") = .empty,
    readable_lease_hook: ?h.ReadableLeaseHook = null,
    test_admission_sleep: if (h.builtin.is_test) ?struct { ptr: *anyopaque, sleep: *const fn (*anyopaque, std.Io.Duration) std.Io.Cancelable!void } else void = if (h.builtin.is_test) null else {},
    test_path_lease_busy: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},

    /// Identifies this Connection's own Store/NativeFile, for
    /// `QueuedMutex.lockForLease`/`retainForWaiter` (WS2): two Connections
    /// to the same path may each have their own Owner and therefore their
    /// own Store, sharing only `self.gate` for the kernel-level lease, so a
    /// same-process queue position on `self.gate.mutex` alone does not mean
    /// the holder and the waiter share one native commit queue. `null` if
    /// unavailable (no live root attached yet) -- callers must treat that
    /// as "never match" rather than guess.
    pub fn storeIdentity(self: *const Connection) ?*anyopaque {
        const root = self.current orelse return null;
        const backend = root.owned_lite_backend orelse return null;
        const store = backend.native_docstore orelse return null;
        return store;
    }

    pub fn acquire(self: *Connection) !native.PathWriterLock {
        return self.acquireWithMaintenance(false, false);
    }

    /// lite-writer-authority.md W6: admit a plain read on a writer-mode
    /// connection without the writer lease. Holds the gate and the lane's
    /// reader slot on success (released by `endRead` and the gate unlock).
    /// Returns null, holding nothing, when the read needs a replacement
    /// open or capacity admission; the caller then takes the writer path.
    pub fn admitRead(self: *Connection) !?*h.Handle {
        const io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(io);
        const lane = &self.owner.writer_lane;
        const unconditional = self.admission_deadline_ns == null and self.options.busy_timeout_ms == 0;
        lane.beginRead(self.owner, &self.gate.mutex, self.admissionDeadline(), unconditional) catch |err| {
            self.gate.mutex.unlock(io);
            return err;
        };
        const served = self.owner.refreshForRead() catch |err| {
            lane.endRead();
            self.gate.mutex.unlock(io);
            return err;
        };
        const root = served orelse {
            lane.endRead();
            self.gate.mutex.unlock(io);
            return null;
        };
        // Pending SQL recovery writes on entry; capacity admission may prove.
        if (root.sql_commit_recovery_needed or root.sql_ddl_recovery_needed or
            (self.options.profile == .native and self.options.open_mode == .writer and usesCapacityAdmission(root)))
        {
            lane.endRead();
            self.gate.mutex.unlock(io);
            return null;
        }
        self.current = root;
        self.attach(root);
        return root;
    }

    pub fn acquireForMaintenance(self: *Connection) !native.PathWriterLock {
        return self.acquireWithMaintenance(true, false);
    }

    /// Like `acquire`, but for `WriterLane.checkout`'s own cold path only
    /// -- see its own call site's comment. That path has already marked
    /// `writer_lane.checked_out = true` for *this* checkout, under the
    /// lane's mutex, an instant before calling this, so `writer_lane.
    /// parked` is necessarily `null` already (the lane's own `while`
    /// loop took the `self.parked != null` branch instead whenever it
    /// was not) and nothing else can set `checked_out` in between.
    /// Skipping `WriterLane.borrowLease` here is not an optimization:
    /// borrowing would make this exact call wait on the very checkout only
    /// its own return can ever end, which is the bug
    /// `checked_out`'s own doc comment describes (observed live as
    /// `TestSQLConformance` hanging on a `DROP INDEX`, admission retrying
    /// this self-wait forever). Everything else `acquirePathLeaseAssumeLocked`
    /// does (the retained-lease reuse, and the real kernel acquisition for
    /// a write-mode connection) still applies unchanged.
    pub fn acquireForOwnCheckout(self: *Connection) !native.PathWriterLock {
        return self.acquireWithMaintenance(false, true);
    }

    fn acquireWithMaintenance(self: *Connection, manual_maintenance: bool, skip_lane_wait: bool) !native.PathWriterLock {
        const io = h.handleLockIo();
        const gate_started = std.Io.Clock.awake.now(io).nanoseconds;
        self.gate.mutex.lockForLease(io, self.storeIdentity());
        {
            const waited: u64 = @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - gate_started));
            const stats = &self.owner.publication_stats;
            stats.gate_wait_ns +|= waited;
            stats.gate_wait_max_ns = @max(stats.gate_wait_max_ns, waited);
        }
        errdefer self.gate.mutex.unlock(io);
        if (manual_maintenance) self.owner.maintenance_activity.wait(io, &self.gate.mutex);
        return self.acquirePathLeaseAssumeLocked(skip_lane_wait);
    }

    fn admissionDeadline(self: *const Connection) i96 {
        return self.admission_deadline_ns orelse (std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + @as(i96, self.options.busy_timeout_ms) * std.time.ns_per_ms);
    }

    /// Polls rather than blocking so the wait honors the deadline. Proofs
    /// run for seconds, so a millisecond poll costs nothing measurable.
    fn lockAllocatorProofBefore(self: *Connection, deadline: i96) bool {
        const io = h.handleLockIo();
        var backoff_ns: i96 = 100 * std.time.ns_per_us;
        while (!self.owner.allocator_proof_mutex.tryLock()) {
            const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
            if (remaining <= 0) return false;
            io.sleep(.fromNanoseconds(@min(backoff_ns, remaining)), .awake) catch return false;
            backoff_ns = @min(backoff_ns * 2, 5 * std.time.ns_per_ms);
        }
        return true;
    }

    fn acquirePathLeaseAssumeLocked(self: *Connection, skip_lane_wait: bool) !native.PathWriterLock {
        const io = h.handleLockIo();
        const deadline = self.admissionDeadline();
        // Writers take FIFO turns across processes. An expired budget still
        // permits an immediate acquisition when nobody is queued.
        if (h.liteOpenModeCanWrite(self.options.open_mode)) {
            const stats = &self.owner.publication_stats.foreground_lease;
            // lite-writer-authority.md W2: borrow the lane's lease. Skipped
            // only for `checkout`'s own cold path (`acquireForOwnCheckout`),
            // which already holds the lane and keeps the lease itself.
            if (!skip_lane_wait) {
                const started = std.Io.Clock.awake.now(io).nanoseconds;
                const unconditional = self.admission_deadline_ns == null and self.options.busy_timeout_ms == 0;
                var borrowed = self.owner.writer_lane.borrowLease(self.owner, &self.gate.mutex, deadline, unconditional, self) catch |err| {
                    if (err == error.FileBusy) stats.recordBusy(@intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started)));
                    return err;
                };
                stats.recordAcquired(switch (borrowed.source) {
                    .handoff => .handoff,
                    .uncontended => .uncontended,
                    .queued => .queued,
                }, if (borrowed.source == .handoff) 0 else @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started)));
                borrowed.lease.startHold(stats);
                return self.owner.withReadRegistrationBatch(borrowed.lease);
            }
            if (self.gate.mutex.takeRetained()) |held| {
                var lease = held;
                stats.recordAcquired(.handoff, 0);
                lease.startHold(stats);
                return self.owner.withReadRegistrationBatch(lease);
            }
            const started = std.Io.Clock.awake.now(io).nanoseconds;
            if (comptime h.builtin.is_test) if (self.test_path_lease_busy) |hook| {
                // Tests observe the point where admission would start waiting:
                // only after a failed attempt with budget left.
                if (native.lockWriterPathWithIo(alloc, io, self.path)) |held| return self.owner.withReadRegistrationBatch(held) else |err| {
                    if (err != error.WouldBlock and err != error.FileBusy) return err;
                    if (deadline - std.Io.Clock.awake.now(io).nanoseconds <= 0) return error.FileBusy;
                    hook.notify(hook.ptr);
                }
            };
            var lease = native.fairWriterLockWithIo(alloc, io, self.owner.path_lock_io.io(), self.path, deadline) catch |err| {
                if (err == error.FileBusy) stats.recordBusy(@intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started)));
                return err;
            };
            stats.recordAcquired(if (lease.contended) .queued else .uncontended, @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started)));
            lease.startHold(stats);
            return self.owner.withReadRegistrationBatch(lease);
        }
        // Our own retained exclusive lease would block this shared lock.
        self.gate.mutex.dropRetained();
        var backoff: i96 = std.time.ns_per_ms;
        while (true) {
            // Reader admission: `fairReaderLockWithIo` claims no exclusion
            // from a writer at all in region mode (copy-on-write; see its
            // own doc comment for why) and succeeds immediately with no
            // kernel lock -- not even the old non-blocking pre-check this
            // loop used to make before ever reaching it, which would have
            // meant taking a real kernel `.shared` lock on every admission
            // regardless. Only its own legacy fallback (region unusable)
            // can actually block here, up to `deadline`.
            if (native.fairReaderLockWithIo(alloc, self.owner.path_lock_io.io(), self.path, deadline)) |held| return self.owner.withReadRegistrationBatch(held) else |err| switch (err) {
                // Sidecar-free read-only snapshots retain their inode
                // authority. Resource-limited executors retain bounded
                // polling rather than making the deadline unbounded.
                error.FileNotFound, error.ConcurrencyUnavailable => {
                    if (comptime h.builtin.is_test) if (self.test_path_lease_busy) |hook| hook.notify(hook.ptr);
                },
                // The fast region path above claims no exclusion from a
                // writer at all, but its own legacy fallback (engaged only
                // when the region itself is unusable for this call) takes
                // a real kernel `.shared` lock, which can collide with
                // this *same* process's own writer lease: `WriterLane`
                // keeps that lease's kernel lock open while parked between
                // writes, indistinguishable at the kernel level from a
                // foreign writer's hold. A reader needs no actual
                // exclusion from a same-process writer either (copy-on-
                // write, same reasoning as the fast path's own doc
                // comment), so this is never a real conflict -- only a
                // genuinely foreign writer's lock is.
                // `pinLocalWriterLeaseForRead` tells the two apart *and*
                // pins the lease against release for as long as this read
                // holds the synthetic lease it hands back: a point-in-time
                // check alone would not stop `maintainParkedLease`'s idle/
                // foreign release, `checkinImpl`'s own force-release, or
                // `checkout`'s/`beginMaintenanceCheckout`'s own foreign-
                // overdue handling from giving this exact kernel lease to
                // a foreign writer while this read is still in flight. See
                // lite-writer-authority.md: a read-only handle in this
                // process must never be blocked by this process's own
                // parked writer lease.
                error.FileBusy, error.WouldBlock => {
                    if (self.gate.pinLocalWriterLeaseForRead()) |lane| {
                        var synthetic: native.PathWriterLock = .{ .io_impl = undefined, .borrowed_io = io, .file = null };
                        lane.lendLocalReadPin(&synthetic);
                        return self.owner.withReadRegistrationBatch(synthetic);
                    }
                    return err;
                },
                else => return err,
            }
            {
                const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
                if (remaining <= 0) return error.FileBusy;
                var entropy: [4]u8 = undefined;
                io.random(&entropy);
                const floor = @divTrunc(backoff, 2);
                const spread: u32 = @intCast(backoff - floor + 1);
                const delay = floor + @as(i96, std.mem.readInt(u32, &entropy, .little) % spread);
                try io.sleep(.fromNanoseconds(@min(delay, remaining)), .awake);
                backoff = @min(backoff * 2, 4 * std.time.ns_per_ms);
            }
        }
    }

    /// Prepare an existing free pool before a mutating request can open index
    /// leases or stage an arbitrarily large mutation. Waiting here releases publication
    /// locks so an active detached proof can finish. Empty unbounded owners
    /// keep the append-first path, reads do not demand reuse proof, and bounded
    /// owners still require capacity proof.
    /// No SQL request has started, so admission never replays partial work.
    ///
    /// `skip_lane_wait`: `true` only from `WriterLane.checkout`'s own two
    /// call sites, which have already marked `writer_lane.checked_out =
    /// true` for *this* checkout before calling this function -- exactly
    /// the condition `Connection.acquireForOwnCheckout`'s own doc comment
    /// describes for its initial lease. A capacity/reuse admission retry
    /// below reacquires the lease a *second* time (after a detached
    /// allocator proof closes and reopens it); without threading this
    /// through to that reacquisition too, it would call
    /// `WriterLane.borrowLease`, which waits for the lane's checkout --
    /// one only *this* call's own eventual return (back up through
    /// `checkout`'s own `checkin`) can ever end, so the wait can
    /// never resolve except by timing out the entire busy budget (observed
    /// live: `verifyAllocatorAssumeLocked`'s reacquire burning the whole
    /// `busy_timeout_ms` on an otherwise-uncontended lease). Every other
    /// caller (plain reads, non-lane-eligible writes, and every direct
    /// test call to this function) passes `false`, preserving the
    /// original wait behavior where it is not self-referential.
    pub fn refresh(self: *Connection, lease: *?native.PathWriterLock, intent: api.StorageIntent, skip_lane_wait: bool) !*h.Handle {
        var root = self.owner.refreshOrTurnBusy() catch |err| switch (err) {
            error.MaintenanceTurnBusy => try self.refreshAfterMaintenanceTurn(lease, skip_lane_wait),
            else => return err,
        };
        if (self.options.profile == .native and self.options.open_mode == .writer and
            (usesCapacityAdmission(root) or (intent == .mutation and try self.needsReuseAdmission(root))))
        {
            const io = h.handleLockIo();
            // Verification can be published just before its reader is closed.
            // Mutations must wait for that reader fence too, even when the
            // pool is already verified. Otherwise a whole transaction can pick
            // append-only allocation during reader teardown. Idle verified
            // unbounded owners skip this lane entirely.
            const waited = !self.owner.allocator_proof_mutex.tryLock();
            if (waited) {
                lease.*.?.close();
                lease.* = null;
                self.gate.mutex.unlock(io);
                if (comptime h.builtin.is_test) if (self.owner.test_allocator_wait) |hook| hook.notify(hook.ptr);
                // Another thread's proof can take far longer than this call's
                // busy timeout. Waiting for it counts against that budget.
                if (self.admission_deadline_ns == null and self.options.busy_timeout_ms == 0) {
                    // No busy timeout configured: keep waiting for the proof.
                    self.owner.allocator_proof_mutex.lockUncancelable(io);
                } else if (!self.lockAllocatorProofBefore(self.admissionDeadline())) {
                    self.gate.mutex.lockUncancelable(io);
                    return error.FileBusy;
                }
                self.gate.mutex.lockUncancelable(io);
            }
            defer self.owner.allocator_proof_mutex.unlock(io);
            if (waited) {
                lease.* = try self.acquirePathLeaseAssumeLocked(skip_lane_wait);
                root = try self.owner.refresh();
            }
            if (try needsCapacityProof(root) or (intent == .mutation and try self.needsReuseAdmission(root))) {
                // Keep foreign unverified opens from publishing index leases
                // between the detached proof and reacquiring the writer lease.
                if (!try self.owner.retainMaintenanceTurnAssumeLocked()) return error.FileBusy;
                defer self.owner.releaseMaintenanceTurnAssumeLocked();
                try self.owner.verifyAllocatorAssumeLocked(lease, self, skip_lane_wait);
                if (lease.* == null) lease.* = try self.acquirePathLeaseAssumeLocked(skip_lane_wait);
                root = try self.owner.refresh();
                // Another process may move the tip during the detached walk.
                // Do not repeat whole-file proofs while holding this
                // connection's admission queue. No transaction has started:
                // refuse safely and let a later request prove the new tip.
                if (try needsCapacityProof(root) or (intent == .mutation and try self.needsReuseAdmission(root))) return error.FileBusy;
            }
        }
        self.current = root;
        self.attach(root);
        return root;
    }

    /// The replacement open needs the maintenance turn, and a maintenance
    /// writer holds it. That writer needs the writer lease to finish its
    /// own commit, so waiting with the lease held only runs out the clock.
    /// Give the lease back, back off until the turn may be free, reacquire,
    /// and refresh again, all within this call's busy timeout -- the same
    /// shape as the allocator-proof wait in `refresh`.
    fn refreshAfterMaintenanceTurn(self: *Connection, lease: *?native.PathWriterLock, skip_lane_wait: bool) !*h.Handle {
        const io = h.handleLockIo();
        const deadline = self.admissionDeadline();
        var backoff_ns: i96 = 100 * std.time.ns_per_us;
        while (true) {
            if (lease.*) |*held| held.close();
            lease.* = null;
            self.gate.mutex.unlock(io);
            const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
            const slept = remaining > 0 and (if (io.sleep(.fromNanoseconds(@min(backoff_ns, remaining)), .awake)) |_| true else |_| false);
            self.gate.mutex.lockUncancelable(io);
            if (!slept) return error.FileBusy;
            backoff_ns = @min(backoff_ns * 2, 5 * std.time.ns_per_ms);
            lease.* = try self.acquirePathLeaseAssumeLocked(skip_lane_wait);
            return self.owner.refreshOrTurnBusy() catch |err| switch (err) {
                error.MaintenanceTurnBusy => continue,
                else => return err,
            };
        }
    }

    fn needsReuseAdmission(self: *Connection, root: *h.Handle) !bool {
        const store = root.owned_lite_backend.?.native_docstore.?;
        if (store.file.free_pages_verified) return self.owner.allocator_verification_active;
        // This reads allocator metadata, not the live graph. Do not trust the
        // advertised pages for allocation until the detached proof checks them.
        // Waiting only for a nonempty pool avoids a graph scan on empty imports.
        const stats = (try store.file.allocatorStats()) orelse return false;
        return stats.reusable_pages != 0;
    }

    fn usesCapacityAdmission(root: *h.Handle) bool {
        const store = root.owned_lite_backend.?.native_docstore.?;
        return store.file.max_file_bytes != null or store.maintenance_policy.options.max_storage_bytes != 0 or
            store.maintenance_policy.options.capacity_probe != null or store.disk_admission_limit != null;
    }

    fn needsCapacityProof(root: *h.Handle) !bool {
        const store = root.owned_lite_backend.?.native_docstore.?;
        if (store.file.free_pages_verified or !usesCapacityAdmission(root)) return false;
        store.mutex.lock();
        defer store.mutex.unlock();
        try store.refreshAdmissionAssumeLocked();
        return store.file.max_file_bytes != null and !store.file.free_pages_verified;
    }

    pub fn pin(self: *Connection) !*h.Handle {
        const root = self.owner.current orelse return error.InvalidArgument;
        self.current = root;
        self.attach(root);
        return root;
    }

    fn attach(self: *Connection, root: *h.Handle) void {
        std.debug.assert(!self.attached);
        if (root.owned_lite_backend) |*backend| if (backend.native_docstore) |store| {
            store.background_write_admission = .{ .ptr = &self.owner.writer_lane, .try_enter = backgroundWriteRefused, .leave = backgroundWriteLeave };
            store.pending_admission_hint = .{ .ptr = &self.owner.writer_lane, .active = pendingAdmissionActive };
        };
        std.debug.assert(root.sql_sessions.count() == 0 and root.sql_cursors.count() == 0);
        root.lite_connection_context = self;
        root.sql_sessions = self.sql_sessions;
        root.next_sql_session_id = self.next_sql_session_id;
        root.sql_cursors = self.sql_cursors;
        root.next_sql_cursor_id = self.next_sql_cursor_id;
        root.table_handles = self.table_handles;
        root.readable_lease_hook = self.readable_lease_hook;
        self.sql_sessions = .empty;
        self.sql_cursors = .empty;
        self.table_handles = .empty;
        self.attached = true;
    }

    /// A lease-managed store never writes from its own thread: its writes
    /// happen only under a lease some caller holds. The idle-turn fold
    /// and the flush loop's ride-along drain are refused; ride-along groups
    /// are driven by their own waiters, by the next writer to lead a
    /// group, or by whoever releases the lease (every release path drains
    /// first), and the log folds inline at its capacity threshold or
    /// through the maintenance snapshot.
    fn backgroundWriteRefused(_: *anyopaque) bool {
        return false;
    }

    fn backgroundWriteLeave(_: *anyopaque) void {}

    /// `Store.PendingAdmissionHint`: whether exactly one writer is
    /// currently between this lane's `checkout` and the matching
    /// `checkin` -- admitted to validate and stage its write, but not
    /// yet enqueued (see `checked_out`'s own doc comment). Reads
    /// `checked_out` under the lane's own mutex: cheap and never
    /// contended by this call, since neither `checkout` nor `checkin`
    /// ever touches a `Store`'s `commit_mutex` (the lock this hint is
    /// read under), so there is no lock-ordering cycle between them.
    fn pendingAdmissionActive(ptr: *anyopaque) bool {
        const lane: *WriterLane = @ptrCast(@alignCast(ptr));
        const io = h.handleLockIo();
        lane.mutex.lockUncancelable(io);
        defer lane.mutex.unlock(io);
        return lane.checked_out and lane.holder != std.Thread.getCurrentId();
    }

    pub fn detach(self: *Connection) void {
        if (!self.attached) return;
        const root = self.current.?;
        self.sql_sessions = root.sql_sessions;
        self.next_sql_session_id = root.next_sql_session_id;
        self.sql_cursors = root.sql_cursors;
        self.next_sql_cursor_id = root.next_sql_cursor_id;
        self.table_handles = root.table_handles;
        self.readable_lease_hook = root.readable_lease_hook;
        root.sql_sessions = .empty;
        root.sql_cursors = .empty;
        root.table_handles = .empty;
        root.readable_lease_hook = null;
        root.lite_connection_context = null;
        self.attached = false;
    }

    /// Caller holds the owner gate. Only streams borrow the live runtime
    /// between calls. Staged writes own their metadata, and child handles
    /// rebind and revalidate identity before using a cached table pointer.
    pub fn ensureOtherConnectionsCanDrop(self: *Connection) !void {
        for (self.owner.connections.items) |other| {
            if (other == self) continue;
            var cursors = other.sql_cursors.valueIterator();
            while (cursors.next()) |cursor| {
                if (cursor.*.adapter.handle == self.owner.current) return error.SqlStatementReadUnavailable;
            }
        }
    }

    pub fn notifyMutation(self: *Connection) void {
        self.owner.notifyMutation();
    }

    pub fn collectRetired(self: *Connection) void {
        self.owner.collectRetired();
    }

    pub fn close(self: *Connection) void {
        const io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(io);
        if (self.owner.current) |root| {
            self.current = root;
            self.attach(root);
            @import("sql_cursor.zig").closeAll(root);
            root.sql_cursors = .empty;
            @import("sql_session.zig").closeAll(root);
            root.sql_sessions = .empty;
            for (root.table_handles.items) |id| h.closeHandleId(id);
            root.table_handles.deinit(alloc);
            root.table_handles = .empty;
            self.detach();
        }
        const owner = self.owner;
        for (owner.connections.items, 0..) |connection, i| {
            if (connection == self) {
                _ = owner.connections.swapRemove(i);
                break;
            }
        }
        const last = owner.connections.items.len == 0;
        if (last) {
            owner.stopping.store(true, .release);
            // Retain progress from a mutating v4 owner's in-flight proof.
            // Replay/inference still observe stopping and cancel normally.
            const finish_proof = owner.options.profile == .native and owner.options.open_mode == .writer and
                owner.allocator_certificate_pending and owner.allocator_verification_active and
                if (owner.current) |root| root.owned_lite_backend.?.native_docstore.?.file.header.indexed_reclamation else false;
            if (!finish_proof) owner.verification_cancel.request();
            if (comptime h.builtin.is_test) if (owner.test_after_stop) |hook| hook.notify(hook.ptr);
            owner.wake.set(io);
            for (self.gate.owners.items, 0..) |candidate, i| {
                if (candidate == owner) {
                    _ = self.gate.owners.swapRemove(i);
                    break;
                }
            }
        } else owner.collectRetired();
        self.gate.mutex.unlock(io);
        if (last) owner.deinit();
        releaseGate(self.gate);
        alloc.destroy(self);
    }
};

pub fn open(path: []const u8, options: api.LiteResolvedOpenOptions, create: bool) !*h.Handle {
    if (!h.lite_backend.isAflitePath(path)) return error.InvalidArgument;
    if (create and !h.liteOpenModeCanWrite(options.open_mode)) return error.InvalidArgument;
    // Own string options before opening a generation: its lazy table opens
    // retain these settings after the caller's options buffer disappears.
    var resolved_options = options;
    var ttl_owner_id: ?[]u8 = null;
    if (options.ttl_cleanup) |ttl| {
        ttl_owner_id = try alloc.dupe(u8, ttl.owner_id);
        resolved_options.ttl_cleanup.?.owner_id = ttl_owner_id.?;
    }
    defer if (ttl_owner_id) |id| alloc.free(id);
    var models_dir: ?[]u8 = null;
    if (options.inference.models_dir) |directory| {
        models_dir = try alloc.dupe(u8, directory);
        resolved_options.inference.models_dir = models_dir.?;
    }
    defer if (models_dir) |directory| alloc.free(directory);
    var initial: ?*h.Handle = null;
    errdefer if (initial) |root| {
        root.lease_snapshot = true;
        h.closeHandle(root);
    };
    if (create) {
        var resolved = resolved_options;
        resolved.externally_locked = true;
        initial = try api.openLiteHandleAllocWithRuntime(alloc, path, resolved, true, h.handleLockIo(), null);
        initial.?.owned_lite_backend.?.native_docstore.?.file.releaseWriterOwnership();
        // Marks this instance subject to the `active_writer` backstop
        // (native.zig's `publishCheckpoint`/inode_lock.zig's own doc
        // comment): this becomes `owner.current` below, the Owner's own
        // live root under the writer-lease admission gate.
        initial.?.owned_lite_backend.?.native_docstore.?.file.lease_managed = true;
    } else {
        const probe = try std.Io.Dir.cwd().openFile(h.handleLockIo(), path, .{
            .mode = if (h.liteOpenModeCanWrite(options.open_mode)) .read_write else .read_only,
        });
        probe.close(h.handleLockIo());
    }
    const canonical = try native.realPathAlloc(alloc, h.handleLockIo(), path);
    defer alloc.free(canonical);
    const gate = try retainGate(canonical);
    errdefer releaseGate(gate);
    const io = h.handleLockIo();
    gate.mutex.lockUncancelable(io);
    defer gate.mutex.unlock(io);
    const connection = try alloc.create(Connection);
    errdefer alloc.destroy(connection);
    const wrapper = try alloc.create(h.Handle);
    errdefer alloc.destroy(wrapper);
    const existing = for (gate.owners.items) |owner| {
        if (compatible(owner.options, options)) break owner;
    } else null;
    const owner = existing orelse try alloc.create(Owner);
    errdefer if (existing == null) alloc.destroy(owner);
    if (existing == null) {
        // Runtime snapshots may close out of order or before this owner. Keep
        // the process signal owner alive until this executor has joined too.
        @import("io_lifetime.zig").acquire();
        owner.* = .{ .path = gate.path, .gate = gate, .options = resolved_options, .current = initial, .owned_ttl_owner_id = ttl_owner_id, .owned_models_dir = models_dir, .path_lock_io = std.Io.Threaded.init(alloc, .{ .async_limit = .nothing, .concurrent_limit = .limited(2) }) };
    }
    errdefer if (existing == null) @import("io_lifetime.zig").release();
    errdefer if (existing == null) owner.path_lock_io.deinit();
    if (existing == null) {
        owner.reader_coordinator = try native.ReaderCoordinator.create(alloc, io);
        owner.options.reader_coordinator = owner.reader_coordinator;
        if (initial) |root| root.owned_lite_backend.?.native_docstore.?.file.reader_coordinator = owner.reader_coordinator;
    }
    errdefer if (existing == null) owner.reader_coordinator.release();
    errdefer if (existing == null) owner.deinitMaintenanceSchedules();
    try owner.connections.ensureUnusedCapacity(alloc, 1);
    errdefer if (existing == null) owner.connections.deinit(alloc);
    if (existing == null) {
        try gate.owners.ensureUnusedCapacity(alloc, 1);
        if (!create) {
            if (!h.liteOpenModeCanWrite(options.open_mode)) gate.mutex.dropRetained();
            const attempt = if (h.liteOpenModeCanWrite(options.open_mode))
                (if (gate.mutex.takeRetained()) |kept| kept else native.tryFairWriterLockWithIo(alloc, io, gate.path))
            else
                native.lockReaderPathWithIo(alloc, io, gate.path);
            if (attempt) |held| {
                var lease = held;
                defer lease.close();
                if (owner.refresh()) |_| {} else |err| switch (err) {
                    // A peer proving an unverified tip owns index opening.
                    // As with a busy path lease, initialize this runtime lazily.
                    error.FileBusy, error.WouldBlock => {},
                    else => return err,
                }
            } else |err| switch (err) {
                error.WouldBlock, error.FileBusy => {},
                else => return err,
            }
        }
        errdefer if (!create) if (owner.current) |root| {
            root.lease_snapshot = true;
            h.closeHandle(root);
        };
        if (comptime h.builtin.os.tag != .freestanding and !h.builtin.single_threaded) {
            if (options.profile == .native and h.liteOpenModeCanWrite(options.open_mode))
                owner.worker = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Owner.maintenanceLoop, .{owner});
        }
        gate.owners.appendAssumeCapacity(owner);
    }
    connection.* = .{ .path = gate.path, .gate = gate, .owner = owner, .options = owner.options, .current = owner.current };
    connection.options.busy_timeout_ms = options.busy_timeout_ms;
    owner.connections.appendAssumeCapacity(connection);
    wrapper.* = .{ .alloc = alloc, .db = undefined, .db_live = false, .open_mode = options.open_mode, .lite_connection = connection };
    if (existing != null) if (initial) |root| {
        root.lease_snapshot = true;
        h.closeHandle(root);
    };
    initial = null;
    if (existing == null) {
        ttl_owner_id = null;
        models_dir = null;
    }
    return wrapper;
}

test "capi native marker sweep survives foreground replacement while proof is busy" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("foreground-marker-progress");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
    defer h.closeHandle(wrapper);
    const owner = wrapper.lite_connection.?.owner;
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    owner.allocator_proof_mutex.lockUncancelable(io);
    defer owner.allocator_proof_mutex.unlock(io);
    var markers: std.ArrayList(std.Io.File) = .empty;
    var orphan: ?usize = null;
    defer {
        for (markers.items, 0..) |marker, i| if (orphan == null or i != orphan.?) marker.close(io);
        markers.deinit(a);
    }
    for (0..130) |i| {
        const name = try std.fmt.allocPrint(a, "{s}/.aflite-lease-{x:0>32}", .{ tmp.path(), i });
        defer a.free(name);
        const marker = try std.Io.Dir.cwd().createFile(io, name, .{ .read = true, .exclusive = true, .lock = .exclusive });
        errdefer marker.close(io);
        try markers.append(a, marker);
    }
    // Select the last marker in actual directory order, so more than one
    // bounded batch of live markers precedes it on every supported filesystem.
    var dir = try std.Io.Dir.cwd().openDir(io, tmp.path(), .{ .iterate = true });
    defer dir.close(io);
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (std.mem.startsWith(u8, entry.name, ".aflite-lease-") and entry.name.len == 46)
            orphan = try std.fmt.parseInt(usize, entry.name[14..], 16);
    }
    try std.testing.expect(orphan != null);
    markers.items[orphan.?].close(io);
    for (0..6) |turn| {
        {
            owner.gate.mutex.lockUncancelable(io);
            defer owner.gate.mutex.unlock(io);
            var lease = try native.lockWriterPathWithIo(a, io, path);
            defer lease.close();
            var foreign = try native.NativeFile.openWithIo(a, io, path, .{ .externally_locked = true, .no_sync = true });
            defer foreign.close();
            foreign.defer_allocator_verification = true;
            var text: [16]u8 = undefined;
            try foreign.putCatalogRecord("test/changed-runtime", try std.fmt.bufPrint(&text, "{d}", .{turn}));
        }
        _ = try owner.maintenanceTurnForTest();
    }
    const name = try std.fmt.allocPrint(a, ".aflite-lease-{x:0>32}", .{orphan.?});
    defer a.free(name);
    try std.testing.expectError(error.FileNotFound, dir.openFile(io, name, .{}));
    // Live leases must survive every sweep.
    for (0..130) |i| {
        if (i == orphan.?) continue;
        const live_name = try std.fmt.allocPrint(a, ".aflite-lease-{x:0>32}", .{i});
        defer a.free(live_name);
        const live = try dir.openFile(io, live_name, .{});
        live.close(io);
    }
}

test "capi native unbounded writer admits reusable pool before a large request" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    const Scenario = enum { empty, cold, active, draining, foreign, uncertain, uncertain_during };
    for ([_]Scenario{ .empty, .cold, .active, .draining, .foreign, .uncertain, .uncertain_during }) |scenario| {
        const active = scenario == .active or scenario == .draining or scenario == .foreign;
        var tmp = try api.TestDirectory.init("unbounded-reuse-admission");
        defer tmp.cleanup();
        try std.Io.Dir.cwd().createDirPath(io, tmp.path());
        const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
        defer a.free(path);
        const value: [32768]u8 = @splat('v');
        {
            var seed = try native.NativeFile.createWithIo(a, io, path, .{ .indexed_reclamation = true, .no_sync = true });
            defer seed.close();
            for (0..@as(usize, if (scenario == .empty) 0 else 64)) |i| {
                var key: [16]u8 = undefined;
                try seed.putCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}), &value);
            }
            for (0..@as(usize, if (scenario == .empty) 0 else 64)) |i| {
                var key: [16]u8 = undefined;
                try seed.deleteCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}));
            }
            if (scenario != .empty) for (0..4) |_| {
                _ = try seed.reclaimPages(4096);
            };
            if (scenario != .empty) try std.testing.expect((try seed.allocatorStats()).?.reusable_pages > 256);
        }
        const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, false);
        defer h.closeHandle(wrapper);
        const connection = wrapper.lite_connection.?;
        const owner = connection.owner;
        owner.stopping.store(true, .release);
        owner.wake.set(io);
        owner.worker.?.join();
        owner.worker = null;
        owner.stopping.store(false, .release);
        owner.verification_cancel.requested.store(false, .release);
        owner.allocator_verification_started = false;
        owner.maintenance_storage_pending = false;
        {
            var lease = try connection.acquire();
            defer {
                lease.close();
                connection.gate.mutex.unlock(io);
            }
            const store = owner.current.?.owned_lite_backend.?.native_docstore.?;
            store.file.free_pages_verified = false;
            store.file.max_file_bytes = null;
            store.maintenance_policy.options.max_storage_bytes = 0;
            store.maintenance_policy.options.capacity_probe = null;
            store.disk_admission_limit = null;
        }
        {
            var lease: ?native.PathWriterLock = try connection.acquire();
            defer {
                connection.detach();
                if (lease) |*held| held.close();
                connection.gate.mutex.unlock(io);
            }
            const root = try connection.refresh(&lease, .read_only, false);
            try std.testing.expect(!root.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
            try std.testing.expect(!owner.allocator_verification_started);
        }
        const Probe = struct {
            owner: *Owner,
            foreign: bool,
            uncertain: bool,
            uncertain_during: bool,
            captures: usize = 0,
            captured: std.Io.Event = .unset,
            released: std.Io.Event = .unset,
            waited: bool = false,
            failure: ?anyerror = null,
            fn capture(ptr: *anyopaque, _: *native.NativeFile) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.captures += 1;
                if (self.uncertain or self.uncertain_during) {
                    // Bound a regression: without the publication guards,
                    // cancel the second attempt instead of hanging the suite.
                    if (self.captures > 1) {
                        self.owner.verification_cancel.request();
                    } else if (self.uncertain_during) {
                        const lock_io = h.handleLockIo();
                        self.owner.gate.mutex.lockUncancelable(lock_io);
                        defer self.owner.gate.mutex.unlock(lock_io);
                        self.owner.current.?.owned_lite_backend.?.native_docstore.?.file.checkpoint_publication_uncertain = true;
                    }
                    return;
                }
                self.captured.set(h.handleLockIo());
                self.released.waitUncancelable(h.handleLockIo());
            }
            fn waiting(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.waited = true;
                defer self.released.set(h.handleLockIo());
                if (self.foreign) self.publishForeign() catch |err| {
                    self.failure = err;
                };
            }
            fn publishForeign(self: *@This()) !void {
                const lock_io = h.handleLockIo();
                var lease = try native.lockWriterPathWithIo(alloc, lock_io, self.owner.path);
                defer lease.close();
                var file = try native.NativeFile.openWithIo(alloc, lock_io, self.owner.path, .{ .externally_locked = true, .no_sync = true });
                defer file.close();
                file.defer_allocator_verification = true;
                try file.putCatalogRecord("foreign", "publication while admission yields");
            }
            fn turn(self: *@This()) void {
                _ = self.owner.maintenanceTurnForTest() catch |err| {
                    self.failure = err;
                };
                self.captured.set(h.handleLockIo());
            }
        };
        var probe: Probe = .{ .owner = owner, .foreign = scenario == .foreign, .uncertain = scenario == .uncertain, .uncertain_during = scenario == .uncertain_during };
        defer {
            probe.released.set(io);
            if (owner.worker) |worker| {
                worker.join();
                owner.worker = null;
            }
            owner.test_after_allocator_capture = null;
            owner.test_before_allocator_reader_close = null;
            owner.test_allocator_wait = null;
        }
        if (active) {
            if (scenario == .draining) {
                owner.test_before_allocator_reader_close = .{ .ptr = &probe, .notify = Probe.capture };
            } else {
                owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
            }
            owner.test_allocator_wait = .{ .ptr = &probe, .notify = Probe.waiting };
            owner.worker = try std.Thread.spawn(.{}, Probe.turn, .{&probe});
            probe.captured.waitUncancelable(io);
            if (probe.failure) |err| return err;
            if (scenario == .draining) try std.testing.expect(owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
        }
        {
            var lease: ?native.PathWriterLock = try connection.acquire();
            defer {
                connection.detach();
                if (lease) |*held| held.close();
                connection.gate.mutex.unlock(io);
            }
            if (scenario == .empty) {
                const file = &owner.current.?.owned_lite_backend.?.native_docstore.?.file;
                try std.testing.expectEqual(@as(u64, 0), (try file.allocatorStats()).?.reusable_pages);
            }
            if (scenario == .uncertain) {
                // An uncertain runtime is never reused: refresh reopens and
                // reconciles from durable state before admitting the request.
                const poisoned = owner.current.?;
                poisoned.owned_lite_backend.?.native_docstore.?.file.checkpoint_publication_uncertain = true;
                const reconciled = try connection.refresh(&lease, .mutation, false);
                try std.testing.expect(reconciled != poisoned);
                try std.testing.expect(!reconciled.owned_lite_backend.?.native_docstore.?.file.checkpoint_publication_uncertain);
                try std.testing.expect(!owner.allocator_verification_active);
                continue;
            }
            if (scenario == .uncertain_during) {
                owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
                try std.testing.expectError(error.OutcomeReconciliationRequired, connection.refresh(&lease, .mutation, false));
                try std.testing.expectEqual(@as(usize, 1), probe.captures);
                try std.testing.expect(!owner.allocator_verification_active);
                try std.testing.expect(!owner.verification_cancel.requested.load(.acquire));
                continue;
            }
            const root = try connection.refresh(&lease, .mutation, false);
            const file = &root.owned_lite_backend.?.native_docstore.?.file;
            if (scenario == .empty) {
                try std.testing.expect(!owner.allocator_verification_started);
                try std.testing.expect(!file.free_pages_verified);
                continue;
            }
            if (!file.free_pages_verified) return error.ReusablePoolUnverifiedAtAdmission;
            try std.testing.expect(owner.allocator_verification_started);
            try std.testing.expectEqual(active, probe.waited);
            const before = (try file.file.stat(io)).size;
            const reused = (try file.allocatorStats()).?.reused_pages;
            // One admitted request can publish many transactions without ever
            // releasing the gate. Its complete volume fits in the initial pool.
            for (0..16) |i| {
                var key: [16]u8 = undefined;
                try file.putCatalogRecord(try std.fmt.bufPrint(&key, "new-{d}", .{i}), &value);
            }
            const reused_pages = (try file.allocatorStats()).?.reused_pages - reused;
            const growth = (try file.file.stat(io)).size - before;
            std.debug.print("LITE_REUSE_ADMISSION scenario={s} write_bytes={d} growth_bytes={d} reused_pages={d}\n", .{ @tagName(scenario), 16 * value.len, growth, reused_pages });
            try std.testing.expect(reused_pages > 0);
            try std.testing.expect(growth < 32 * file.header.page_size);
            try std.testing.expect((try file.check()).valid);
            connection.notifyMutation();
        }
        if (owner.worker) |worker| {
            worker.join();
            owner.worker = null;
        }
        if (probe.failure) |err| return err;
        if (active) try std.testing.expectEqual(@as(usize, if (scenario == .foreign) 2 else 1), probe.captures);
    }
}

test "capi SQL native read intent avoids allocator proofs with exclusive locking" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("sql-read-storage-intent");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    {
        const value: [32768]u8 = @splat('v');
        var seed = try native.NativeFile.createWithIo(a, io, path, .{ .indexed_reclamation = true, .no_sync = true });
        defer seed.close();
        for (0..64) |i| {
            var key: [16]u8 = undefined;
            try seed.putCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}), &value);
        }
        for (0..64) |i| {
            var key: [16]u8 = undefined;
            try seed.deleteCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}));
        }
        for (0..4) |_| _ = try seed.reclaimPages(4096);
    }
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, false);
    const id = api.publishHandle(wrapper) catch |err| {
        h.closeHandle(wrapper);
        return err;
    };
    defer api.antfly_db_close(id);
    const owner = wrapper.lite_connection.?.owner;
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    owner.verification_cancel.requested.store(false, .release);
    owner.allocator_verification_started = false;
    owner.maintenance_storage_pending = false;
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    const Check = struct {
        fn ok(code: h.capi.ErrorCode, buffer: h.capi.Buffer, line: u32) !void {
            if (code != .ok) std.debug.print("SQL_INTENT failure_line={d} response={s}\n", .{ line, if (buffer.ptr) |ptr| ptr[0..buffer.len] else "<empty>" });
            try std.testing.expectEqual(h.capi.ErrorCode.ok, code);
        }
    };
    // Streaming cursors require a table-backed SELECT. Provision its source
    // before modeling the cold unverified owner used by the read assertions.
    try Check.ok(api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"CREATE TABLE seed_numbers (n BIGINT)\"}"), &output), output, @src().line);
    api.antfly_buffer_free(&output);
    try Check.ok(api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"INSERT INTO seed_numbers (_id,n) VALUES ('seed',7)\"}"), &output), output, @src().line);
    api.antfly_buffer_free(&output);
    owner.allocator_verification_started = false;
    const store = owner.current.?.owned_lite_backend.?.native_docstore.?;
    store.file.free_pages_verified = false;
    store.file.max_file_bytes = null;
    store.maintenance_policy.options.max_storage_bytes = 0;
    store.maintenance_policy.options.capacity_probe = null;
    store.disk_admission_limit = null;
    try std.testing.expect((try store.file.allocatorStats()).?.reusable_pages > 256);
    // Catalog cache locking is exclusive, but listing/opening existing tables
    // is a read operation. SDK opens must not demand whole-file reuse proofs.
    const tables = @import("tables.zig");
    try Check.ok(tables.antfly_db_list_tables_json(id, &output), output, @src().line);
    api.antfly_buffer_free(&output);
    try std.testing.expect(!owner.allocator_verification_started);
    var table: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(id, .fromSlice("seed_numbers"), &table));
    defer api.antfly_db_close(table);
    try std.testing.expect(!owner.allocator_verification_started);
    var timestamp: u64 = 0;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_timestamp(table, .fromSlice("seed"), &timestamp));
    try Check.ok(api.antfly_db_lookup_json(table, .fromSlice("seed"), &output), output, @src().line);
    api.antfly_buffer_free(&output);
    try std.testing.expect(!owner.allocator_verification_started);

    var session: u64 = 0;
    try Check.ok(@import("sql_session.zig").antfly_db_sql_session_open(id, &session), output, @src().line);
    for ([_][]const u8{
        "{\"statement\":\"SELECT 1\"}",
        "{\"statement\":\"EXPLAIN SELECT 1\"}",
        "{\"statement\":\"EXPLAIN ANALYZE SELECT 1\"}",
        "{\"statement\":\"BEGIN\",\"session_id\":1}",
        "{\"statement\":\"SELECT 1\",\"session_id\":1}",
    }) |request| {
        try Check.ok(api.antfly_db_sql_json(id, .fromSlice(request), &output), output, @src().line);
        api.antfly_buffer_free(&output);
    }
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"SELECT (\",\"session_id\":1}"), &output));
    api.antfly_buffer_free(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"SELECT 1\",\"session_id\":1}"), &output));
    try std.testing.expect(std.mem.indexOf(u8, output.ptr.?[0..output.len], "25P02") != null);
    api.antfly_buffer_free(&output);
    try Check.ok(api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"ROLLBACK\",\"session_id\":1}"), &output), output, @src().line);
    api.antfly_buffer_free(&output);
    const select = h.capi.Slice.fromSlice("{\"statement\":\"SELECT 1\"}");
    try Check.ok(@import("sql_describe.zig").antfly_db_sql_describe_json(id, select, &output), output, @src().line);
    api.antfly_buffer_free(&output);
    var cursor: u64 = 0;
    try Check.ok(@import("sql_cursor.zig").antfly_db_sql_open_cursor_json(id, .fromSlice("{\"statement\":\"SELECT n FROM seed_numbers\"}"), &cursor, &output), output, @src().line);
    api.antfly_buffer_free(&output);
    try Check.ok(@import("sql_cursor.zig").antfly_db_sql_fetch_cursor_json(id, cursor, 1, &output), output, @src().line);
    api.antfly_buffer_free(&output);
    try Check.ok(@import("sql_cursor.zig").antfly_db_sql_close_cursor(id, cursor), output, @src().line);
    // Mutating ANALYZE is rejected by the runtime before execution or binding.
    try std.testing.expectEqual(h.capi.ErrorCode.unsupported, api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"EXPLAIN ANALYZE INSERT INTO missing (_id) VALUES ('x')\"}"), &output));
    api.antfly_buffer_free(&output);
    try std.testing.expect(!owner.allocator_verification_started);
    try std.testing.expect(!owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
    try Check.ok(api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"CREATE TABLE numbers (n BIGINT)\"}"), &output), output, @src().line);
    api.antfly_buffer_free(&output);
    try std.testing.expect(owner.allocator_verification_started);
    try std.testing.expect(owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
    try Check.ok(api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"INSERT INTO numbers (_id,n) VALUES ('row',7)\"}"), &output), output, @src().line);
    api.antfly_buffer_free(&output);
    const file = &owner.current.?.owned_lite_backend.?.native_docstore.?.file;
    try std.testing.expect((try file.check()).valid);
}

test "capi native allocator proof reacquisition honors per-connection busy admission" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    const Scenario = enum { foreground, immediate, background };
    for ([_]Scenario{ .foreground, .immediate, .background }) |scenario| {
        var tmp = try api.TestDirectory.init("proof-writer-admission");
        defer tmp.cleanup();
        try std.Io.Dir.cwd().createDirPath(io, tmp.path());
        const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
        defer a.free(path);
        const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
        defer h.closeHandle(wrapper);
        const connection = wrapper.lite_connection.?;
        const owner = connection.owner;
        owner.stopping.store(true, .release);
        owner.wake.set(io);
        owner.worker.?.join();
        owner.worker = null;
        owner.stopping.store(false, .release);
        owner.verification_cancel.requested.store(false, .release);
        // Options belong to a connection, not to the shared storage owner.
        // The background case must remain nonblocking even with this budget.
        connection.options.busy_timeout_ms = if (scenario == .immediate) 0 else 5000;
        owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified = false;
        const Probe = struct {
            path: []const u8,
            held: ?native.PathWriterLock = null,
            failure: ?anyerror = null,
            retries: usize = 0,
            fn capture(ptr: *anyopaque, _: *native.NativeFile) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                // Win the actual writer lease while proof runs outside it.
                self.held = native.lockWriterPathWithIo(alloc, h.handleLockIo(), self.path) catch |err| {
                    self.failure = err;
                    return;
                };
            }
            fn release(self: *@This()) void {
                if (self.held) |*held| held.close();
                self.held = null;
            }
            fn busy(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.retries += 1;
                // No sleeps or scheduler races: release only after a real
                // failed reacquisition reaches the caller's admission policy.
                self.release();
            }
        };
        var probe: Probe = .{ .path = path };
        defer probe.release();
        owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
        connection.test_path_lease_busy = .{ .ptr = &probe, .notify = Probe.busy };
        defer {
            owner.test_after_allocator_capture = null;
            connection.test_path_lease_busy = null;
        }
        var lease: ?native.PathWriterLock = try connection.acquire();
        defer {
            if (lease) |*held| held.close();
            connection.gate.mutex.unlock(io);
        }
        const result = owner.verifyAllocatorAssumeLocked(&lease, if (scenario == .background) null else connection, false);
        if (probe.failure) |err| return err;
        switch (scenario) {
            .foreground => {
                try result;
                try std.testing.expectEqual(@as(usize, 1), probe.retries);
                try std.testing.expect(owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
            },
            .immediate => {
                try std.testing.expectError(error.FileBusy, result);
                try std.testing.expectEqual(@as(usize, 0), probe.retries);
            },
            .background => {
                try std.testing.expectError(error.WouldBlock, result);
                try std.testing.expectEqual(@as(usize, 0), probe.retries);
            },
        }
        if (scenario != .foreground) {
            // The proof completed before the busy yield. The next attempt
            // transfers it under the lease instead of walking again.
            probe.release();
            owner.test_after_allocator_capture = null;
            if (lease == null) lease = try owner.tryWriterLeaseAssumeLocked();
            const walks = owner.publication_stats.allocator_proof_walks;
            try owner.verifyAllocatorAssumeLocked(&lease, if (scenario == .background) null else connection, false);
            try std.testing.expectEqual(walks, owner.publication_stats.allocator_proof_walks);
            try std.testing.expectEqual(@as(u64, 1), owner.publication_stats.allocator_proof_receipt_reuses);
            try std.testing.expect(owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
        }
    }
}

test "capi native allocator proof survives close and foreground runtime replacement" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    const Scenario = enum { foreground, closing, canceled, queued, queued_storage };
    for ([_]Scenario{ .foreground, .closing, .canceled, .queued, .queued_storage }) |scenario| {
        const queued = scenario == .queued or scenario == .queued_storage;
        var tmp = try api.TestDirectory.init("inflight-proof-lifetime");
        defer tmp.cleanup();
        try std.Io.Dir.cwd().createDirPath(io, tmp.path());
        const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
        defer a.free(path);
        const value: [32768]u8 = @splat('x');
        {
            // Seed the pool before a DB runtime can pin any input generations.
            var seed = try native.NativeFile.createWithIo(a, io, path, .{ .indexed_reclamation = true, .no_sync = true });
            defer seed.close();
            for (0..16) |i| {
                var key: [16]u8 = undefined;
                try seed.putCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}), &value);
            }
            for (0..16) |i| {
                var key: [16]u8 = undefined;
                try seed.deleteCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}));
            }
            _ = try seed.reclaimPages(4096);
            if ((try seed.allocatorStats()).?.reusable_pages <= 64) return error.ProofFixtureHasNoFreePool;
        }
        const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, false);
        var closed = false;
        defer if (!closed) h.closeHandle(wrapper);
        const connection = wrapper.lite_connection.?;
        const owner = connection.owner;
        owner.stopping.store(true, .release);
        owner.wake.set(io);
        owner.worker.?.join();
        owner.worker = null;
        owner.stopping.store(false, .release);
        owner.verification_cancel.requested.store(false, .release);
        owner.allocator_verification_started = false;
        owner.maintenance_storage_pending = false;
        {
            var lease = try connection.acquire();
            defer {
                lease.close();
                connection.gate.mutex.unlock(io);
            }
            const store = owner.current.?.owned_lite_backend.?.native_docstore.?;
            // Bootstrap may now publish a live certificate. Keep this fixture
            // explicitly unsealed so replacement/cancellation still exercises
            // the original pending-proof lineage, including legacy writers.
            try store.file.putCatalogRecord("uncertified-proof-fixture", "primitive publication without a seal");
            store.file.free_pages_verified = false;
            store.file.defer_allocator_verification = true;
            store.file.max_file_bytes = null;
            store.maintenance_policy.options.max_storage_bytes = 0;
            store.maintenance_policy.options.capacity_probe = null;
            store.disk_admission_limit = null;
            owner.notifyMutation();
        }
        if (queued) {
            // Finish a real earlier proof, then move the tip. A historical
            // attempt must not be mistaken for a currently running walk.
            _ = try owner.maintenanceTurnForTest();
            try std.testing.expect(owner.allocator_verification_started);
            try std.testing.expect(!owner.allocator_verification_active);
            var lease = try connection.acquire();
            defer {
                lease.close();
                connection.gate.mutex.unlock(io);
            }
            var foreign = try native.NativeFile.openWithIo(a, io, path, .{ .externally_locked = true, .no_sync = true });
            defer foreign.close();
            foreign.defer_allocator_verification = true;
            const large = try a.alloc(u8, Owner.close_allocator_proof_max_bytes);
            defer a.free(large);
            @memset(large, 'q');
            try foreign.putCatalogRecord("foreign-large", large);
            try std.testing.expect((try foreign.file.stat(io)).size > Owner.close_allocator_proof_max_bytes);
            // Whether the worker runs before or after close drops the gate,
            // force admission through the fair queue rather than yielding.
            owner.maintenance_capture_misses = 2;
            owner.maintenance_storage_pending = scenario == .queued_storage;
        }
        const Probe = struct {
            owner: *Owner,
            entry_header: native.Header,
            exit_header: ?native.Header = null,
            captured: std.Io.Event = .unset,
            released: std.Io.Event = .unset,
            calls: usize = 0,
            failure: ?anyerror = null,
            canceled_on_stop: bool = false,
            fn beforeGate(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.captured.set(h.handleLockIo());
                self.released.waitUncancelable(h.handleLockIo());
            }
            fn capture(ptr: *anyopaque, _: *native.NativeFile) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.calls += 1;
                self.captured.set(h.handleLockIo());
                self.released.waitUncancelable(h.handleLockIo());
            }
            fn stopped(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.canceled_on_stop = self.owner.verification_cancel.requested.load(.acquire);
                self.released.set(h.handleLockIo());
            }
            fn turn(self: *@This()) void {
                _ = self.owner.maintenanceTurnForTest() catch |err| {
                    self.failure = err;
                    self.captured.set(h.handleLockIo());
                    return;
                };
                self.exit_header = self.owner.current.?.owned_lite_backend.?.native_docstore.?.file.header;
                self.captured.set(h.handleLockIo());
            }
        };
        var probe: Probe = .{ .owner = owner, .entry_header = owner.current.?.owned_lite_backend.?.native_docstore.?.file.header };
        defer probe.released.set(io);
        owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
        if (queued) owner.test_before_maintenance_capture = .{ .ptr = &probe, .notify = Probe.beforeGate };
        owner.worker = try std.Thread.spawn(.{}, Probe.turn, .{&probe});
        probe.captured.waitUncancelable(io);
        if (probe.failure) |err| return err;
        if (scenario == .closing or queued) {
            owner.test_after_stop = .{ .ptr = &probe, .notify = Probe.stopped };
        } else if (scenario == .canceled) {
            owner.verification_cancel.request();
            probe.released.set(io);
            owner.worker.?.join();
            owner.worker = null;
            if (probe.failure) |err| {
                if (err != error.MaintenanceCanceled) return error.CanceledProofUnexpectedOutcome;
            } else return error.CanceledProofUnexpectedOutcome;
            probe.failure = null;
        } else {
            {
                var lease: ?native.PathWriterLock = try connection.acquire();
                defer {
                    connection.detach();
                    if (lease) |*held| held.close();
                    connection.gate.mutex.unlock(io);
                }
                const previous = owner.current.?;
                const replacement = blk: {
                    // Retire the runtime captured by the proof while retaining
                    // its allocator/executor until the original walk finishes.
                    previous.maintenance_references += 1;
                    defer previous.maintenance_references -= 1;
                    break :blk try owner.refresh();
                };
                try std.testing.expect(replacement != previous);
                const store = replacement.owned_lite_backend.?.native_docstore.?;
                store.file.max_file_bytes = null;
                store.maintenance_policy.options.max_storage_bytes = 0;
                store.maintenance_policy.options.capacity_probe = null;
                store.disk_admission_limit = null;
                // Exercise append-only lineage below request admission, which
                // now waits for a reusable pool instead of permitting this race.
                try store.file.putCatalogRecord("during-proof", "foreground progress");
                if (store.file.free_pages_verified) return error.ReplacementVerifiedBeforeOriginalProof;
                connection.notifyMutation();
            }
            probe.released.set(io);
            owner.worker.?.join();
            owner.worker = null;
            if (probe.failure) |err| return err;
            {
                var lease: ?native.PathWriterLock = try connection.acquire();
                defer {
                    connection.detach();
                    if (lease) |*held| held.close();
                    connection.gate.mutex.unlock(io);
                }
                const root = try connection.refresh(&lease, .mutation, false);
                const file = &root.owned_lite_backend.?.native_docstore.?.file;
                if (!file.free_pages_verified) return error.CompletedProofLostAcrossReplacement;
                const before = (try file.allocatorStats()).?.reused_pages;
                const before_pages = file.activeCheckpoint().page_count;
                for (0..8) |_| try file.putCatalogRecord("during-proof", &value);
                if ((try file.allocatorStats()).?.reused_pages <= before) return error.CompletedProofDidNotEnableReuse;
                if (file.activeCheckpoint().page_count >= before_pages + 32) return error.CompletedProofKeptAppending;
                connection.notifyMutation();
            }
        }
        h.closeHandle(wrapper);
        closed = true;
        if (probe.failure) |err| return err;
        if (queued) {
            if (!probe.canceled_on_stop) return error.CloseDidNotCancelQueuedWork;
            if (!std.meta.eql(probe.entry_header, probe.exit_header.?)) return error.StoppedMaintenanceRefreshedRuntime;
        } else if (probe.canceled_on_stop) return error.CloseCanceledExistingProof;
        try std.testing.expectEqual(@as(usize, if (queued) 0 else 1), probe.calls);
        var certificate = try native.NativeFile.openWithIo(a, io, path, .{ .read_only = true, .internal_reader = true });
        defer certificate.close();
        if (try certificate.inheritAllocatorCertificate() != (scenario != .canceled and !queued)) return error.CloseCertificateUnexpectedOutcome;
        try std.testing.expect((try certificate.check()).valid);
    }
}

test "capi native close bounds proof work and skips legacy files" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    const Reason = enum { legacy, large, foreign };
    for ([_]Reason{ .legacy, .large, .foreign }) |reason| {
        const indexed = reason != .legacy;
        var tmp = try api.TestDirectory.init("close-proof-budget");
        defer tmp.cleanup();
        try std.Io.Dir.cwd().createDirPath(io, tmp.path());
        const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
        defer a.free(path);
        {
            var seed = try native.NativeFile.createWithIo(a, io, path, .{ .indexed_reclamation = indexed, .no_sync = true });
            defer seed.close();
            if (reason == .large) {
                const large = try a.alloc(u8, Owner.close_allocator_proof_max_bytes);
                defer a.free(large);
                @memset(large, 'x');
                try seed.putDocument("large", large);
            } else try seed.putDocument("legacy", "revision 3");
        }
        const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, false);
        var closed = false;
        defer if (!closed) h.closeHandle(wrapper);
        const connection = wrapper.lite_connection.?;
        const owner = connection.owner;
        owner.stopping.store(true, .release);
        owner.wake.set(io);
        owner.worker.?.join();
        owner.worker = null;
        owner.allocator_verification_started = false;
        {
            var lease = try connection.acquire();
            defer {
                lease.close();
                connection.gate.mutex.unlock(io);
            }
            const file = &owner.current.?.owned_lite_backend.?.native_docstore.?.file;
            try std.testing.expectEqual(indexed, file.header.indexed_reclamation);
            if (reason == .large) try std.testing.expect(file.activeCheckpoint().page_count *| file.header.page_size > Owner.close_allocator_proof_max_bytes);
            file.free_pages_verified = false;
            file.defer_allocator_verification = true;
            file.max_file_bytes = null;
            try file.putCatalogRecord("changed", "one small publication");
            owner.notifyMutation();
            if (reason == .foreign) {
                var foreign = try native.NativeFile.openWithIo(a, io, path, .{ .externally_locked = true, .no_sync = true });
                defer foreign.close();
                foreign.defer_allocator_verification = true;
                try foreign.putCatalogRecord("foreign", "a newer publication");
            }
        }
        const Probe = struct {
            calls: usize = 0,
            fn capture(ptr: *anyopaque, _: *native.NativeFile) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.calls += 1;
            }
        };
        var probe: Probe = .{};
        owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
        h.closeHandle(wrapper);
        closed = true;
        try std.testing.expectEqual(@as(usize, 0), probe.calls);
        var file = try native.NativeFile.openWithIo(a, io, path, .{ .read_only = true, .internal_reader = true });
        defer file.close();
        try std.testing.expect(!try file.inheritAllocatorCertificate());
        const value = (try file.getCatalogRecordAlloc(a, "changed")).?;
        defer a.free(value);
        try std.testing.expectEqualStrings("one small publication", value);
    }
}

test "capi native short lived writers carry reuse across clean closes without a worker turn" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("short-lived-reuse");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const value: [32768]u8 = @splat('x');
    {
        // A crash/older writer left a valid free pool without a certificate.
        var seed = try native.NativeFile.createWithIo(a, io, path, .{ .indexed_reclamation = true, .no_sync = true });
        defer seed.close();
        // Keep enough distinct payloads to cover the entire request. Repeatedly
        // overwriting one key can reuse the same few pages during seeding and
        // leave a pool too small for a request that pins its entry generation.
        for (0..64) |i| {
            var key: [16]u8 = undefined;
            try seed.putCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}), &value);
        }
        for (0..64) |i| {
            var key: [16]u8 = undefined;
            try seed.deleteCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}));
        }
        for (0..4) |_| _ = try seed.reclaimPages(4096);
        try std.testing.expect((try seed.allocatorStats()).?.reusable_pages > 256);
    }
    for (0..3) |process| {
        const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, false);
        var closed = false;
        defer if (!closed) h.closeHandle(wrapper);
        const connection = wrapper.lite_connection.?;
        const owner = connection.owner;
        // No worker turn is available. The first owner must prove its pool
        // at admission; subsequent owners inherit its clean-close certificate.
        owner.stopping.store(true, .release);
        owner.verification_cancel.request();
        owner.wake.set(io);
        owner.worker.?.join();
        owner.worker = null;
        owner.stopping.store(false, .release);
        owner.verification_cancel.requested.store(false, .release);
        owner.allocator_verification_started = false;
        var lease: ?native.PathWriterLock = try connection.acquire();
        {
            defer {
                connection.detach();
                if (lease) |*held| held.close();
                connection.gate.mutex.unlock(io);
            }
            const root = try connection.refresh(&lease, .mutation, false);
            const file = &root.owned_lite_backend.?.native_docstore.?.file;
            if (!file.free_pages_verified) return error.ShortLivedWriterPoolUnverified;
            const before = (try file.allocatorStats()).?.reused_pages;
            const before_pages = file.activeCheckpoint().page_count;
            for (0..8) |_| try file.putCatalogRecord("next", &value);
            connection.notifyMutation();
            const reused_pages = (try file.allocatorStats()).?.reused_pages - before;
            const growth_pages = file.activeCheckpoint().page_count - before_pages;
            std.debug.print("LITE_OWNER_REUSE owner={d} growth_pages={d} reused_pages={d}\n", .{ process, growth_pages, reused_pages });
            if (reused_pages == 0) return error.ShortLivedWriterDidNotReuse;
            if (growth_pages >= 32) return error.ShortLivedWriterKeptAppending;
        }
        h.closeHandle(wrapper);
        closed = true;
        var probe = try native.NativeFile.openWithIo(a, io, path, .{ .read_only = true, .internal_reader = true });
        defer probe.close();
        if (!try probe.inheritAllocatorCertificate()) return error.ShortLivedWriterLostCertificate;
        if (!(try probe.check()).valid) return error.ShortLivedWriterInvalidFile;
    }
}

test "capi resolver curation publication proofs skip large catalogs and fence changed records" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = try api.TestDirectory.init("publication-catalog-proof");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    const root = try api.openLiteHandleAllocWithRuntime(alloc, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    defer h.closeHandle(root);
    const store = root.owned_lite_backend.?.native_docstore.?;
    const large = try a.alloc(u8, 4 * 1024 * 1024);
    @memset(large, 'x');
    try @import("tables.zig").create(root, "named", "{}", false);
    const named = try @import("tables.zig").get(root, "named");
    var named_proof = try PublicationCatalog.capture(a, root, named);
    defer named_proof.deinit();
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try txn.put(keys.public_schema_json_key, large);
        try txn.commit();
    }
    var proof = try PublicationCatalog.capture(a, root, &root.db);
    defer proof.deinit();
    const hydrated_before = store.file.test_value_read_bytes.load(.monotonic);
    try std.testing.expectEqual(std.math.maxInt(u64), store.reader_frontier.load(.acquire));
    for (0..20) |i| {
        {
            var txn = try store.beginWrite();
            errdefer txn.abort();
            try txn.put(try std.fmt.allocPrint(a, "unrelated/{d}", .{i}), "primary");
            try txn.commit();
        }
        try proof.verify(root);
    }
    // Neither capture nor twenty verifications hydrated a catalog value.
    try std.testing.expect(proof.fingerprints[0] != null);
    try std.testing.expectEqual(hydrated_before, store.file.test_value_read_bytes.load(.monotonic));
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try txn.put(keys.public_schema_json_key, large);
        try txn.commit();
    }
    try proof.verify(root); // same bytes, new immutable record remains valid.
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try txn.put(keys.public_schema_json_key, "changed");
        try txn.commit();
    }
    try std.testing.expectError(error.EnrichmentSourceChanged, proof.verify(root));
    // Root changes must not invalidate an unrelated named namespace.
    try named_proof.verify(root);
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try txn.put(named_proof.paths[0], "{\"named\":true}");
        try txn.commit();
    }
    try std.testing.expectError(error.EnrichmentSourceChanged, named_proof.verify(root));
    // Metadata absent at capture must not be admitted if later introduced.
    var absent = try PublicationCatalog.capture(a, root, &root.db);
    defer absent.deinit();
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try txn.put(@import("../storage/db/resolver_projection.zig").epoch_key, "1");
        try txn.commit();
    }
    try std.testing.expectError(error.EnrichmentSourceChanged, absent.verify(root));
    std.debug.print("PUBLICATION PROOF catalog_bytes=4194304 primary_commits=20 hydrated_catalogs=0 same_content=accepted changed_content=fenced\n", .{});
}

test "capi native allocator proof refuses admission on a moving tip" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("moving-allocator-admission");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    {
        var seed = try native.NativeFile.createWithIo(a, io, path, .{ .indexed_reclamation = true, .no_sync = true });
        defer seed.close();
        const value: [32768]u8 = @splat('v');
        for (0..16) |i| {
            var key: [16]u8 = undefined;
            try seed.putCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}), &value);
        }
        for (0..16) |i| {
            var key: [16]u8 = undefined;
            try seed.deleteCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}));
        }
        _ = try seed.reclaimPages(4096);
        try std.testing.expect((try seed.allocatorStats()).?.reusable_pages > 64);
    }
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, false);
    defer h.closeHandle(wrapper);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    owner.verification_cancel.requested.store(false, .release);
    const Probe = struct {
        owner: *Owner,
        captures: usize = 0,
        failure: ?anyerror = null,
        fn capture(ptr: *anyopaque, _: *native.NativeFile) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.captures += 1;
            // Bound the unfixed regression instead of hanging the test suite.
            if (self.captures > 1) {
                self.owner.verification_cancel.request();
                return;
            }
            self.publish() catch |err| {
                self.failure = err;
            };
        }
        fn publish(self: *@This()) !void {
            var lease = try native.lockWriterPathWithIo(alloc, h.handleLockIo(), self.owner.path);
            defer lease.close();
            var file = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.owner.path, .{ .externally_locked = true, .no_sync = true });
            defer file.close();
            file.defer_allocator_verification = true;
            try file.putCatalogRecord("foreign", "move the tip while the proof is detached");
        }
    };
    var probe: Probe = .{ .owner = owner };
    owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
    defer owner.test_after_allocator_capture = null;
    var lease: ?native.PathWriterLock = try connection.acquire();
    defer {
        connection.detach();
        if (lease) |*held| held.close();
        connection.gate.mutex.unlock(io);
    }
    owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified = false;
    const proof_walks_before = owner.publication_stats.allocator_proof_walks;
    const proof_failures_before = owner.publication_stats.allocator_proof_failures;
    const result = connection.refresh(&lease, .mutation, false);
    if (probe.failure) |err| return err;
    try std.testing.expectError(error.FileBusy, result);
    try std.testing.expectEqual(@as(usize, 1), probe.captures);
    try std.testing.expectEqual(@as(u64, 1), owner.publication_stats.allocator_proof_walks - proof_walks_before);
    try std.testing.expectEqual(@as(u64, 0), owner.publication_stats.allocator_proof_failures - proof_failures_before);
    try std.testing.expect(owner.publication_stats.allocator_proof_ns >= owner.publication_stats.allocator_proof_max_ns);
    try std.testing.expect(!owner.verification_cancel.requested.load(.acquire));
    // The refusal occurs before a transaction starts. A later, stable request
    // can obtain a new proof and proceed using the same connection.
    owner.test_after_allocator_capture = null;
    const root = try connection.refresh(&lease, .mutation, false);
    try std.testing.expect(root.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
}

test "capi native allocator proof queue retains eligible wake after spurious competitor" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const io = h.handleLockIo();
    var gate: QueuedMutex = .{};
    const Schedule = struct {
        const Outcome = enum(u8) { none, entered, reblocked };
        first_parked: std.Io.Event = .unset,
        second_parked: std.Io.Event = .unset,
        second_held: std.Io.Event = .unset,
        release_first: std.Io.Event = .unset,
        release_second: std.Io.Event = .unset,
        outcome_ready: std.Io.Event = .unset,
        first_entered: std.Io.Event = .unset,
        outcome: std.atomic.Value(Outcome) = .init(.none),
    };
    var schedule: Schedule = .{};
    const Waiter = struct {
        base: std.Io,
        gate: *QueuedMutex,
        schedule: *Schedule,
        first: bool,
        calls: usize = 0,
        fn wait(ptr: ?*anyopaque, word: *const u32, expected: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            if (word != &self.gate.ready.epoch.raw) return self.base.futexWaitUncancelable(u32, word, expected);
            self.calls += 1;
            if (self.calls == 1) {
                const parked = if (self.first) &self.schedule.first_parked else &self.schedule.second_parked;
                parked.set(self.base);
            }
            if (self.first and self.calls == 2) {
                self.schedule.outcome.store(.reblocked, .release);
                self.schedule.outcome_ready.set(self.base);
            }
            if (!self.first and self.calls == 2) {
                // A legal spurious wake lets a competitor recheck before the
                // eligible waiter resumes. It must not consume that waiter's
                // remaining broadcast signal and leave its ticket asleep.
                return;
            }
            if (!self.first and self.calls == 3) {
                self.schedule.second_held.set(self.base);
                self.schedule.release_second.waitUncancelable(self.base);
            }
            self.base.futexWaitUncancelable(u32, word, expected);
            if (self.first and self.calls == 1) self.schedule.release_first.waitUncancelable(self.base);
        }
        fn wake(ptr: ?*anyopaque, word: *const u32, count: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.base.futexWake(u32, word, count);
        }
        fn run(self: *@This()) void {
            // QueuedMutex only uses futex operations on its supplied Io.
            var vtable = self.base.vtable.*;
            vtable.futexWaitUncancelable = wait;
            vtable.futexWake = wake;
            const controlled: std.Io = .{ .userdata = self, .vtable = &vtable };
            self.gate.lockUncancelable(controlled);
            if (self.first) {
                self.schedule.outcome.store(.entered, .release);
                self.schedule.outcome_ready.set(self.base);
                self.schedule.first_entered.set(self.base);
            }
            self.gate.unlock(controlled);
        }
    };
    var first: Waiter = .{ .base = io, .gate = &gate, .schedule = &schedule, .first = true };
    var second: Waiter = .{ .base = io, .gate = &gate, .schedule = &schedule, .first = false };
    gate.lockUncancelable(io);
    const first_thread = try std.Thread.spawn(.{}, Waiter.run, .{&first});
    schedule.first_parked.waitUncancelable(io);
    const second_thread = try std.Thread.spawn(.{}, Waiter.run, .{&second});
    schedule.second_parked.waitUncancelable(io);
    gate.unlock(io);
    schedule.second_held.waitUncancelable(io);
    schedule.release_first.set(io);
    schedule.outcome_ready.waitUncancelable(io);
    const initial = schedule.outcome.load(.acquire);
    // Repair the old implementation's lost wake only to join every thread
    // before reporting the regression. The fixed implementation needs none.
    if (initial == .reblocked) {
        gate.state.lockUncancelable(io);
        gate.ready.broadcast(io);
        gate.state.unlock(io);
    }
    schedule.first_entered.waitUncancelable(io);
    schedule.release_second.set(io);
    first_thread.join();
    second_thread.join();
    try std.testing.expectEqual(Schedule.Outcome.entered, initial);
    try std.testing.expectEqual(gate.next, gate.serving);
}

test "capi native allocator proof mutation preflight honors busy timeout before batch and SQL execution" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    for ([_]enum { batch, sql }{ .batch, .sql }) |operation| {
        for ([_]u64{ 0, 1000 }) |timeout_ms| {
            var tmp = try api.TestDirectory.init("batch-proof-admission");
            defer tmp.cleanup();
            try std.Io.Dir.cwd().createDirPath(io, tmp.path());
            const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
            defer a.free(path);
            {
                var seed = try native.NativeFile.createWithIo(a, io, path, .{ .indexed_reclamation = true, .no_sync = true });
                defer seed.close();
                const value: [32768]u8 = @splat('v');
                for (0..16) |i| {
                    var key: [16]u8 = undefined;
                    try seed.putCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}), &value);
                }
                for (0..16) |i| {
                    var key: [16]u8 = undefined;
                    try seed.deleteCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}));
                }
                _ = try seed.reclaimPages(4096);
                try std.testing.expect((try seed.allocatorStats()).?.reusable_pages > 64);
            }
            const wrapper = try open(path, .{ .profile = .native, .no_sync = true, .busy_timeout_ms = timeout_ms }, false);
            const id = api.publishHandle(wrapper) catch |err| {
                h.closeHandle(wrapper);
                return err;
            };
            defer api.antfly_db_close(id);
            const owner = wrapper.lite_connection.?.owner;
            owner.stopping.store(true, .release);
            owner.wake.set(io);
            owner.worker.?.join();
            owner.worker = null;
            owner.stopping.store(false, .release);
            owner.verification_cancel.requested.store(false, .release);
            if (operation == .sql) {
                var output: h.capi.Buffer = .{};
                defer api.antfly_buffer_free_zero(&output);
                try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"CREATE TABLE numbers (n BIGINT)\"}"), &output));
            }
            const Probe = struct {
                owner: *Owner,
                captures: usize = 0,
                failure: ?anyerror = null,
                fn capture(ptr: *anyopaque, _: *native.NativeFile) void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.captures += 1;
                    if (self.captures != 1) return;
                    self.publish() catch |err| {
                        self.failure = err;
                    };
                }
                fn publish(self: *@This()) !void {
                    var lease = try native.lockWriterPathWithIo(alloc, h.handleLockIo(), self.owner.path);
                    defer lease.close();
                    var file = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.owner.path, .{ .externally_locked = true, .no_sync = true });
                    defer file.close();
                    file.defer_allocator_verification = true;
                    try file.putCatalogRecord("foreign", "invalidate the first proof before the requested batch starts");
                }
            };
            var probe: Probe = .{ .owner = owner };
            owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
            defer owner.test_after_allocator_capture = null;
            owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified = false;
            const key = "accepted";
            const value = "{\"text\":\"one accepted batch\"}";
            const writes = [_]h.capi.WriteIntent{.{ .key = .{ .ptr = key.ptr, .len = key.len }, .value = .{ .ptr = value.ptr, .len = value.len }, .is_delete = false }};
            var output: h.capi.Buffer = .{};
            defer api.antfly_buffer_free_zero(&output);
            const result = if (operation == .batch)
                api.antfly_db_batch(id, &writes, writes.len, null, 0, 0, 0)
            else
                api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"INSERT INTO numbers (_id,n) VALUES ('accepted',7)\"}"), &output);
            if (probe.failure) |err| return err;
            try std.testing.expectEqual(if (timeout_ms == 0) h.capi.ErrorCode.busy else h.capi.ErrorCode.ok, result);
            try std.testing.expectEqual(if (timeout_ms == 0) h.capi.ErrorCode.busy else h.capi.ErrorCode.ok, h.capi.lastErrorCode());
            try std.testing.expectEqual(@as(usize, if (timeout_ms == 0) 1 else 2), probe.captures);
            var out: h.capi.Buffer = .{};
            defer api.antfly_buffer_free_zero(&out);
            if (operation == .batch) {
                const found = api.antfly_db_lookup_json(id, .{ .ptr = key.ptr, .len = key.len }, &out);
                try std.testing.expectEqual(if (timeout_ms == 0) h.capi.ErrorCode.not_found else h.capi.ErrorCode.ok, found);
            } else {
                try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"SELECT n FROM numbers WHERE _id = 'accepted'\"}"), &out));
                const parsed = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
                defer parsed.deinit();
                const rows = parsed.value.object.get("rows").?.array.items;
                try std.testing.expectEqual(@as(usize, if (timeout_ms == 0) 0 else 1), rows.len);
            }
        }
    }
}

test "capi native allocator proof peer close certificate survives runtime refresh" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("request-allocator-certificate");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .busy_timeout_ms = 1000 }, true);
    const id = api.publishHandle(wrapper) catch |err| {
        h.closeHandle(wrapper);
        return err;
    };
    defer api.antfly_db_close(id);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    owner.verification_cancel.requested.store(false, .release);
    {
        var lease: ?native.PathWriterLock = try connection.acquire();
        defer {
            if (lease) |*held| held.close();
            connection.gate.mutex.unlock(io);
        }
        owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified = false;
        try owner.verifyAllocatorAssumeLocked(&lease, connection, false);
        try std.testing.expect(owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
    }
    const key = "certificate-document";
    const value = "{\"text\":\"durable request publication\"}";
    const writes = [_]h.capi.WriteIntent{.{ .key = .{ .ptr = key.ptr, .len = key.len }, .value = .{ .ptr = value.ptr, .len = value.len }, .is_delete = false }};
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch(id, &writes, writes.len, null, 0, 0, 0));
    {
        var lease = try connection.acquire();
        defer {
            lease.close();
            connection.gate.mutex.unlock(io);
        }
        var foreign = try native.NativeFile.openWithIo(a, io, path, .{ .externally_locked = true });
        defer foreign.close();
        try foreign.completeAllocatorVerification();
        try foreign.putCatalogRecord("foreign-certificate", "new primary tip with unchanged index inputs");
        try foreign.sync();
        try std.testing.expect(try foreign.certifyAllocator());
        try std.testing.expect(owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
        owner.allocator_verification_started = false;
    }
    var timestamp: u64 = 0;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_timestamp(id, .{ .ptr = key.ptr, .len = key.len }, &timestamp));
    try std.testing.expect(timestamp != 0);
    try std.testing.expect(owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
    try std.testing.expect(!owner.allocator_verification_started);
}

test "capi native allocator proof admission shares deadline with lease reacquisition" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("proof-admission-deadline");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    {
        var seed = try native.NativeFile.createWithIo(a, io, path, .{ .indexed_reclamation = true, .no_sync = true });
        defer seed.close();
        const value: [32768]u8 = @splat('v');
        for (0..16) |i| {
            var key: [16]u8 = undefined;
            try seed.putCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}), &value);
        }
        for (0..16) |i| {
            var key: [16]u8 = undefined;
            try seed.deleteCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}));
        }
        _ = try seed.reclaimPages(4096);
        try std.testing.expect((try seed.allocatorStats()).?.reusable_pages > 64);
    }
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true, .busy_timeout_ms = 1000 }, false);
    const id = try api.publishHandle(wrapper);
    defer api.antfly_db_close(id);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    owner.verification_cancel.requested.store(false, .release);
    const Probe = struct {
        connection: *Connection,
        held: ?native.PathWriterLock = null,
        failure: ?anyerror = null,
        captures: usize = 0,
        retries: usize = 0,
        fn capture(ptr: *anyopaque, _: *native.NativeFile) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.captures += 1;
            self.expireBudget() catch |err| {
                self.failure = err;
            };
        }
        fn expireBudget(self: *@This()) !void {
            self.held = try native.lockWriterPathWithIo(alloc, h.handleLockIo(), self.connection.path);
            // Expire the actual request deadline while the proof is detached.
            // Reacquisition must fail without entering a fresh lease retry loop.
            const deadline = self.connection.admission_deadline_ns orelse return error.MissingAdmissionDeadline;
            const remaining = deadline - std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds;
            if (remaining > 0) try h.handleLockIo().sleep(.fromNanoseconds(remaining + std.time.ns_per_ms), .awake);
        }
        fn busy(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.retries += 1;
        }
    };
    var probe: Probe = .{ .connection = connection };
    defer if (probe.held) |*held| held.close();
    owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
    connection.test_path_lease_busy = .{ .ptr = &probe, .notify = Probe.busy };
    defer {
        owner.test_after_allocator_capture = null;
        connection.test_path_lease_busy = null;
    }
    owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified = false;
    const writes = [_]h.capi.WriteIntent{.{ .key = .fromSlice("rejected"), .value = .fromSlice("{}"), .is_delete = false }};
    const result = api.antfly_db_batch(id, &writes, writes.len, null, 0, 0, 0);
    if (probe.failure) |err| return err;
    try std.testing.expectEqual(h.capi.ErrorCode.busy, result);
    try std.testing.expectEqual(@as(usize, 1), probe.captures);
    try std.testing.expectEqual(@as(usize, 0), probe.retries);
    try std.testing.expectEqual(@as(?i96, null), connection.admission_deadline_ns);
    probe.held.?.close();
    probe.held = null;
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, api.antfly_db_lookup_json(id, .fromSlice("rejected"), &out));
}

const AdmissionFollowupOperation = enum { batch, sql, read };

const AdmissionFollowupFixture = struct {
    tmp: api.TestDirectory,
    path: []u8,
    id: *anyopaque,
    connection: *Connection,

    fn init(comptime name: []const u8, timeout_ms: u64, operation: AdmissionFollowupOperation) !@This() {
        const a = std.testing.allocator;
        const io = h.handleLockIo();
        var tmp = try api.TestDirectory.init(name);
        errdefer tmp.cleanup();
        try std.Io.Dir.cwd().createDirPath(io, tmp.path());
        const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
        errdefer a.free(path);
        {
            var seed = try native.NativeFile.createWithIo(a, io, path, .{ .indexed_reclamation = true, .no_sync = true });
            defer seed.close();
            const value: [32768]u8 = @splat('v');
            for (0..16) |i| {
                var key: [16]u8 = undefined;
                try seed.putCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}), &value);
            }
            for (0..16) |i| {
                var key: [16]u8 = undefined;
                try seed.deleteCatalogRecord(try std.fmt.bufPrint(&key, "pool-{d}", .{i}));
            }
            _ = try seed.reclaimPages(4096);
            try std.testing.expect((try seed.allocatorStats()).?.reusable_pages > 64);
        }
        const wrapper = try open(path, .{ .profile = .native, .no_sync = true, .busy_timeout_ms = timeout_ms }, false);
        const id = try api.publishHandle(wrapper);
        errdefer api.antfly_db_close(id);
        const connection = wrapper.lite_connection.?;
        const owner = connection.owner;
        owner.stopping.store(true, .release);
        owner.wake.set(io);
        owner.worker.?.join();
        owner.worker = null;
        owner.stopping.store(false, .release);
        owner.verification_cancel.requested.store(false, .release);
        if (operation == .sql) {
            var out: h.capi.Buffer = .{};
            defer api.antfly_buffer_free_zero(&out);
            try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(id, .fromSlice("{\"statement\":\"CREATE TABLE numbers (n BIGINT)\"}"), &out));
        }
        if (operation == .read) try boundCapacity(owner.current.?);
        owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified = false;
        return .{ .tmp = tmp, .path = path, .id = id, .connection = connection };
    }

    fn boundCapacity(root: *h.Handle) !void {
        const store = root.owned_lite_backend.?.native_docstore.?;
        store.mutex.lock();
        defer store.mutex.unlock();
        store.maintenance_policy.options.max_storage_bytes = 1024 * 1024 * 1024;
        try store.refreshAdmissionAssumeLocked();
    }

    fn deinit(self: *@This()) void {
        self.connection.owner.test_after_allocator_capture = null;
        self.connection.test_admission_sleep = null;
        api.antfly_db_close(self.id);
        std.testing.allocator.free(self.path);
        self.tmp.cleanup();
    }

    fn call(self: *@This(), operation: AdmissionFollowupOperation) h.capi.ErrorCode {
        var out: h.capi.Buffer = .{};
        defer api.antfly_buffer_free_zero(&out);
        const writes = [_]h.capi.WriteIntent{.{ .key = .fromSlice("accepted"), .value = .fromSlice("{\"text\":\"one admitted write\"}"), .is_delete = false }};
        return switch (operation) {
            .batch => api.antfly_db_batch(self.id, &writes, writes.len, null, 0, 0, 0),
            .sql => api.antfly_db_sql_json(self.id, .fromSlice("{\"statement\":\"INSERT INTO numbers (_id,n) VALUES ('accepted',7)\"}"), &out),
            .read => @import("tables.zig").antfly_db_list_tables_json(self.id, &out),
        };
    }

    fn expectWritten(self: *@This(), operation: AdmissionFollowupOperation, written: bool) !void {
        var out: h.capi.Buffer = .{};
        defer api.antfly_buffer_free_zero(&out);
        switch (operation) {
            .batch => try std.testing.expectEqual(if (written) h.capi.ErrorCode.ok else h.capi.ErrorCode.not_found, api.antfly_db_lookup_json(self.id, .fromSlice("accepted"), &out)),
            .sql => {
                try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(self.id, .fromSlice("{\"statement\":\"SELECT n FROM numbers WHERE _id = 'accepted'\"}"), &out));
                const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, out.ptr.?[0..out.len], .{});
                defer parsed.deinit();
                try std.testing.expectEqual(@as(usize, if (written) 1 else 0), parsed.value.object.get("rows").?.array.items.len);
            },
            .read => {},
        }
    }
};

const AdmissionFollowupProbe = struct {
    connection: *Connection,
    slow_stable: bool = false,
    capacity_bounded: bool = false,
    captures: usize = 0,
    sleep_calls: usize = 0,
    failure: ?anyerror = null,

    fn capture(ptr: *anyopaque, _: *native.NativeFile) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.captures += 1;
        self.duringProof() catch |err| {
            self.failure = err;
        };
    }

    fn duringProof(self: *@This()) !void {
        const io = h.handleLockIo();
        if (self.slow_stable) {
            const deadline = self.connection.admission_deadline_ns orelse return error.MissingAdmissionDeadline;
            const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
            // Model a multi-minute graph walk with a short, real timeout:
            // the tip stays stable while proof work outlasts the budget.
            if (remaining > 0) try io.sleep(.fromNanoseconds(remaining + std.time.ns_per_ms), .awake);
            return;
        }
        if (self.captures != 1) return;
        const owner = self.connection.owner;
        owner.gate.mutex.lockUncancelable(io);
        defer owner.gate.mutex.unlock(io);
        var lease = try native.lockWriterPathWithIo(alloc, io, owner.path);
        defer lease.close();
        var foreign = try native.NativeFile.openWithIo(alloc, io, owner.path, .{ .externally_locked = true, .no_sync = true });
        defer foreign.close();
        foreign.defer_allocator_verification = true;
        try foreign.putCatalogRecord("foreign", "invalidate the detached proof once");
        // Keep the read's explicit capacity policy across a foreground refresh.
        if (self.capacity_bounded) try AdmissionFollowupFixture.boundCapacity(try owner.refresh());
    }

    fn cancelSleep(ptr: *anyopaque, _: std.Io.Duration) std.Io.Cancelable!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.sleep_calls += 1;
        return error.Canceled;
    }
};

test "capi native allocator proof path admission uses its bounded concurrent executor" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("queued-path-admission", 30, .batch);
    defer fixture.deinit();
    const connection = fixture.connection;
    const io = h.handleLockIo();
    var held = try native.lockWriterPathWithIo(alloc, io, fixture.path);
    const Probe = struct {
        retries: usize = 0,
        fn busy(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.retries += 1;
        }
    };
    var probe: Probe = .{};
    connection.test_path_lease_busy = .{ .ptr = &probe, .notify = Probe.busy };
    defer connection.test_path_lease_busy = null;
    const result = connection.acquire();
    held.close();
    if (result) |value| {
        var unexpected = value;
        unexpected.close();
        connection.gate.mutex.unlock(io);
        return error.ExpectedBusy;
    } else |err| try std.testing.expectEqual(error.FileBusy, err);
    try std.testing.expectEqual(@as(usize, 1), probe.retries);
    // The contract this owns is bounded concurrency for whatever this `Io`
    // *does* run concurrently, not that every bounded wait spawns a
    // worker: §8's shared-mmap-region migration replaced the old file-
    // based CLH ticket queue (writer_queue.zig, deleted) with
    // `fairWriterLockWithIo`'s direct futex waits on the region itself
    // (shared_lock_region.zig's `acquireWriterFairBounded`/
    // `waitWriterFreeBounded`), which block the calling thread in place
    // and never submit a `concurrent()`/`async()` task to `path_lock_io`
    // -- so `worker_threads` legitimately stays null for this admission
    // path now, contended or not. `concurrent_limit` is still this
    // `Io`'s own configured bound for whatever *is* submitted to it
    // concurrently elsewhere; verify the configuration survived, not a
    // worker thread this path never needed.
    try std.testing.expectEqual(std.Io.Limit.limited(2), connection.owner.path_lock_io.concurrent_limit);
    var acquired = try connection.acquire();
    acquired.close();
    connection.gate.mutex.unlock(io);
}

test "capi native allocator proof maintenance capture joins the bounded lease queue" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("queued-maintenance-admission", 1000, .batch);
    defer fixture.deinit();
    const owner = fixture.connection.owner;
    owner.test_maintenance_wait_ns = 30 * std.time.ns_per_ms;
    defer owner.test_maintenance_wait_ns = null;
    var held = try native.lockWriterPathWithIo(alloc, h.handleLockIo(), fixture.path);
    const turn = owner.maintenanceTurn();
    held.close();
    try std.testing.expectError(error.FileBusy, turn);
    // See the sibling "path admission uses its bounded concurrent
    // executor" test's own comment: `fairWriterLockWithIo`'s region-
    // backed wait blocks this call's own thread directly and never
    // submits a `concurrent()` task to `path_lock_io`, so its
    // `worker_threads` never populates for this bounded-queue admission
    // either, contended or not.
    try std.testing.expectEqual(std.Io.Limit.limited(2), owner.path_lock_io.concurrent_limit);
}

test "capi native refresh waiting for the maintenance turn hands the lease back and reacquires it" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const io = h.handleLockIo();
    {
        var fixture = try AdmissionFollowupFixture.init("refresh-turn-wait", 1000, .batch);
        defer fixture.deinit();
        const connection = fixture.connection;
        var lease: ?native.PathWriterLock = try connection.acquire();
        defer {
            if (lease) |*held| held.close();
            connection.gate.mutex.unlock(io);
        }
        const root = try connection.refreshAfterMaintenanceTurn(&lease, false);
        try std.testing.expect(lease != null);
        try std.testing.expectEqual(connection.owner.current.?, root);
    }
    {
        // No busy timeout: fail at once, with the lease already given back
        // and the gate held again, exactly as the caller's own error path
        // expects.
        var fixture = try AdmissionFollowupFixture.init("refresh-turn-wait-no-timeout", 0, .batch);
        defer fixture.deinit();
        const connection = fixture.connection;
        var lease: ?native.PathWriterLock = try connection.acquire();
        defer {
            if (lease) |*held| held.close();
            connection.gate.mutex.unlock(io);
        }
        try std.testing.expectError(error.FileBusy, connection.refreshAfterMaintenanceTurn(&lease, false));
        try std.testing.expect(lease == null);
    }
}

test "issue1015 issue1431 native background turns coordinate across independent owners" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("background-turn-owner", 1000, .batch);
    defer fixture.deinit();
    const owner = fixture.connection.owner;
    const Probe = struct {
        captures: usize = 0,
        fn capture(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.captures += 1;
        }
    };
    var probe: Probe = .{};
    owner.test_before_maintenance_capture = .{ .ptr = &probe, .notify = Probe.capture };
    defer owner.test_before_maintenance_capture = null;
    const peer = (try owner.acquireMaintenanceTurnGuard()).?;
    owner.maintenance_storage_pending = true;
    owner.maintenance_table_cursor = 1;
    const deferred = owner.maintenanceTurn();
    peer.close(h.handleLockIo());
    try std.testing.expect(try deferred);
    try std.testing.expectEqual(@as(usize, 0), probe.captures);
    try std.testing.expectEqual(@as(i64, 100), owner.maintenanceWaitMillisForTest(true));
    owner.maintenance_storage_pending = false;
    owner.maintenance_table_cursor = 0;
    _ = try owner.maintenanceTurn();
    try std.testing.expectEqual(@as(usize, 1), probe.captures);
}

test "no-consumer path registeredDemand is empty for a plain Lite DB" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("no-consumer-demand-registry");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
    defer h.closeHandle(wrapper);
    const owner = wrapper.lite_connection.?.owner;
    // A freshly created Lite DB starts with a default full-text index
    // (full_text_index_v0, see write_amplification_bench_test.zig's
    // Fixture.open, which deletes it the same way for its "primary-only"
    // mode); remove it so this really is a no-consumer DB.
    _ = try owner.current.?.db.deleteIndex("full_text_index_v0");

    // No index, enrichment, group/replication source, TTL or resolver is
    // registered, so there is nothing a background turn could help with.
    try std.testing.expect(!owner.registeredDemandForTest().any());
    // maintenanceWaitMillis must therefore report "park without a timer"
    // rather than the periodic 1000ms poll, once nothing is pending.
    try std.testing.expectEqual(Owner.no_scheduled_wait, owner.maintenanceWaitMillisForTest(false));
    // Pending-from-this-turn still takes priority over demand, unaffected.
    try std.testing.expectEqual(@as(i64, 100), owner.maintenanceWaitMillisForTest(true));
}

test "no-consumer path clock-driven TTL demand does not run a maintenance turn per write" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("no-consumer-ttl-turns");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    // Outside tests every Lite database runs a TTL runtime
    // (`ttl_runtime.Config.enabled` defaults to `!builtin.is_test`); enable
    // it here so this exercises the same demand the product sees.
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true, .ttl_cleanup = .{ .enabled = true } }, true);
    _ = try wrapper.lite_connection.?.owner.current.?.db.deleteIndex("full_text_index_v0");
    const id = try api.publishHandle(wrapper);
    defer api.antfly_db_close(id);
    const owner = wrapper.lite_connection.?.owner;
    try std.testing.expect(owner.worker != null);
    const demand = owner.registeredDemandForTest();
    try std.testing.expect(demand.ttl);
    try std.testing.expect(!Owner.mutationDemand(demand).any());

    // Let the index deletion's own drain finish before measuring.
    {
        var quiet_ms: u64 = 0;
        var last = owner.publication_stats.maintenance_lease.acquisitions;
        var waited_ms: u64 = 0;
        while ((quiet_ms < 1500 or owner.maintenance_pending) and waited_ms < 20000) : (waited_ms += 20) {
            io.sleep(.fromMilliseconds(20), .awake) catch {};
            const now_count = owner.publication_stats.maintenance_lease.acquisitions;
            if (now_count == last) quiet_ms += 20 else {
                quiet_ms = 0;
                last = now_count;
            }
        }
    }

    const leases_before = owner.publication_stats.maintenance_lease.acquisitions;
    const started = std.Io.Clock.awake.now(io).nanoseconds;
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    var body_buf: [160]u8 = undefined;
    for (0..300) |i| {
        const body = try std.fmt.bufPrint(&body_buf, "{{\"inserts\":{{\"doc:{d}\":{{\"search_text\":\"ttl demand write {d}\"}}}},\"sync_level\":\"write\"}}", .{ i, i });
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice(body), &output));
        api.antfly_buffer_free_zero(&output);
    }
    const elapsed_s: u64 = @intCast(@divTrunc(std.Io.Clock.awake.now(io).nanoseconds - started, std.time.ns_per_s));
    const maintenance_leases = owner.publication_stats.maintenance_lease.acquisitions - leases_before;
    // Clock-driven demand gets at most one turn per second. Every write's
    // lane checkin wakes the worker, and before this was gated each wake
    // ran a full turn that took the writer lease: hundreds here.
    try std.testing.expect(maintenance_leases <= elapsed_s + 3);
}

test "no-consumer path 1037 real primary writes cause no consumer-driven background work" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("no-consumer-zero-turns");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
    // A freshly created Lite DB starts with a default full-text index
    // (full_text_index_v0); remove it so this really is a no-consumer DB,
    // matching write_amplification_bench_test.zig's Fixture.open.
    _ = try wrapper.lite_connection.?.owner.current.?.db.deleteIndex("full_text_index_v0");
    const id = try api.publishHandle(wrapper);
    defer api.antfly_db_close(id);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    // The real maintenance worker is left running (not stopped/joined, as
    // other fixtures in this file do) so this exercises the live
    // wake/park path, not just the synchronous demand computation above.
    try std.testing.expect(owner.worker != null);
    try std.testing.expect(!owner.registeredDemandForTest().any());

    // The worker thread snapshot its own poll schedule at spawn time, while
    // full_text_index_v0 (deleted above) still existed, and deleteIndex's
    // removal of that index's stored postings drains over several of the
    // worker's own turns rather than completing inline. Wait for the worker
    // to go quiet before establishing the baseline below, so that
    // startup/deletion drain isn't counted as this test's own no-consumer
    // write amplification. The drain's own turns are spaced about 100ms
    // apart (maintenanceWaitMillis's "pending" retry interval), so require
    // a full second of no new turn, not just long enough to span one
    // retry: a shared, loaded build machine can delay the worker thread's
    // *next* already-scheduled 100ms-timer turn past a shorter quiet
    // window, which would otherwise land inside the measured window below
    // and look like consumer-driven background work. Also require
    // registeredDemand to be empty and no turn-local work left pending,
    // both cheap, so a stalled-but-about-to-fire turn can't slip through
    // either.
    {
        var quiet_ms: u64 = 0;
        var last = owner.test_background_turn_count.load(.acquire);
        var waited_ms: u64 = 0;
        while ((quiet_ms < 1000 or owner.registeredDemandForTest().any() or owner.maintenance_pending) and waited_ms < 20000) : (waited_ms += 20) {
            io.sleep(.fromMilliseconds(20), .awake) catch {};
            const now_count = owner.test_background_turn_count.load(.acquire);
            if (now_count == last) {
                quiet_ms += 20;
            } else {
                quiet_ms = 0;
                last = now_count;
            }
        }
    }

    const write_count: usize = 1037;
    const turns_before = owner.test_background_turn_count.load(.acquire);
    const refreshes_before = owner.test_refresh_count.load(.acquire);
    // Lifetime counter (never reset): the quiescence wait above legitimately
    // acquires leases draining full_text_index_v0's deletion before this
    // window starts, so the assertion below must compare a delta like every
    // other counter here, not the raw cumulative value.
    const lease_acquisitions_before = owner.publication_stats.maintenance_lease.acquisitions;
    const store_before = owner.current.?.owned_lite_backend.?.native_docstore.?;
    const retirement_checks_before = store_before.test_retirement_check_count.load(.acquire);
    const retirement_check_ns_before = store_before.test_retirement_check_ns.load(.acquire);
    // write_amplification_trace is a diagnostic-only global (shared by
    // every test in this binary); reset it here, right before the
    // measured window, exactly as the lite-write-amplification harness
    // itself does (storage/lite/write_amplification_bench_test.zig),
    // so source_turn/derived_worker_turn/maintenance_lease_acquired below
    // reflect only this test's 1,037 writes.
    wal_trace.reset();
    wal_trace.registerCallingThread();

    // 1,037 single-document primary-only writes through the real public C
    // API batch path (antfly_db_batch_json), the same entry point
    // capi/db.zig's antfly_db_batch/antfly_db_batch_json use, so every
    // post-commit notify site (capi/db.zig:816,879 -> connection.notifyMutation)
    // fires exactly as it would for a real caller.
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    var body_buf: [160]u8 = undefined;
    for (0..write_count) |i| {
        const body = try std.fmt.bufPrint(&body_buf, "{{\"inserts\":{{\"doc:{d}\":{{\"search_text\":\"no consumer write {d}\"}}}},\"sync_level\":\"write\"}}", .{ i, i });
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice(body), &output));
        api.antfly_buffer_free_zero(&output);
    }

    const trace_report = wal_trace.snapshot();
    const turns_after = owner.test_background_turn_count.load(.acquire);
    const refreshes_after = owner.test_refresh_count.load(.acquire);
    const background_turns = turns_after - turns_before;
    // refresh() has exactly two call sites: the foreground capi entry path
    // and maintenanceTurn/storageMaintenanceTurn (both counted the same
    // way), so most of this window's refreshes are attributable to the
    // calling writes themselves, with a small remainder from the background
    // turns bounded below.
    const calling_thread_refreshes = refreshes_after - refreshes_before;
    const store_after = owner.current.?.owned_lite_backend.?.native_docstore.?;
    const retirement_checks = store_after.test_retirement_check_count.load(.acquire) - retirement_checks_before;
    const retirement_check_ns = store_after.test_retirement_check_ns.load(.acquire) - retirement_check_ns_before;
    const retirement_check_ns_per_write: f64 = if (retirement_checks == 0) 0 else @as(f64, @floatFromInt(retirement_check_ns)) / @as(f64, @floatFromInt(retirement_checks));
    std.debug.print(
        "no-consumer-path real-writes: writes={d} background_turns={d} maintenance_leases={d} source_turns={d} derived_worker_turns={d} calling_thread_refreshes={d} retirement_checks={d} retirement_check_ns_total={d} retirement_check_ns_per_write={d:.1}\n",
        .{ write_count, background_turns, trace_report.op(.maintenance_lease_acquired), trace_report.op(.source_turn), trace_report.op(.derived_worker_turn), calling_thread_refreshes, retirement_checks, retirement_check_ns, retirement_check_ns_per_write },
    );

    // Neither derived-index nor source-catalog background work ran (there
    // are zero indexes/enrichments/sources to serve): these are fully
    // demand-gated today, so they stay exactly zero regardless of write
    // volume.
    try std.testing.expectEqual(@as(u64, 0), trace_report.op(.source_turn));
    try std.testing.expectEqual(@as(u64, 0), trace_report.op(.derived_worker_turn));
    // The design doc's full no-consumer target is 0 background turns / 0
    // leases / 0 refreshes (lite-single-commit-write-path.md §5), now
    // reached for this harness: once the post-quiescence worker is truly
    // parked (maintenanceWaitMillis's no_scheduled_wait), nothing in the
    // primary-only write path raises a non-empty Demand mask, so
    // notifyDemand never wakes it again. The previous 46-turn/30-lease
    // regression was `maintenanceTurn`'s own idle-turn "the physical tip
    // moved" signal (`changed`) feeding back into its return value
    // unconditionally: with continuous foreground writes, an idle turn's
    // tip had almost always moved again by the time it finished, so every
    // triggered turn rescheduled another one 100ms later for as long as
    // writes kept landing. `changed` now only counts toward "more to do"
    // when a consumer is actually registered (registeredDemand().any());
    // see that call site's comment.
    try std.testing.expectEqual(@as(u64, 0), background_turns);
    try std.testing.expectEqual(@as(u64, 0), owner.publication_stats.maintenance_lease.acquisitions - lease_acquisitions_before);
    try std.testing.expectEqual(@as(u64, 0), trace_report.op(.maintenance_lease_acquired));
    // The calling thread's own refreshes (one validation per capi entry) are
    // real required work, not background amplification; report the count
    // rather than pin an exact per-call multiplier, and only bound it
    // loosely against a gross regression.
    try std.testing.expect(calling_thread_refreshes > 0);
    try std.testing.expect(calling_thread_refreshes <= write_count * 4);
}

test "no-consumer path registering an index produces demand and wakes a background turn" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("consumer-demand-wakes-turn");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
    defer h.closeHandle(wrapper);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    const root = owner.current.?;
    // A freshly created Lite DB starts with a default full-text index
    // (full_text_index_v0); remove it so this really is a no-consumer DB.
    _ = try root.db.deleteIndex("full_text_index_v0");

    try std.testing.expect(!owner.registeredDemandForTest().any());

    {
        var lease = try connection.acquire();
        defer {
            lease.close();
            connection.gate.mutex.unlock(io);
        }
        try root.db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    }
    try std.testing.expect(owner.registeredDemandForTest().index);

    // addIndex's own catalog write already notifies; call again directly to
    // mirror a subsequent primary write's post-commit notification.
    connection.notifyMutation();

    var waited_ms: u64 = 0;
    while (owner.test_background_turn_count.load(.acquire) == 0 and waited_ms < 5000) : (waited_ms += 5) {
        io.sleep(.fromMilliseconds(5), .awake) catch {};
    }
    try std.testing.expect(owner.test_background_turn_count.load(.acquire) != 0);
    try std.testing.expect(owner.publication_stats.maintenance_lease.acquisitions != 0);
}

test "no-consumer path adding an index after writes backfills every prior document" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("no-consumer-then-backfill");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
    defer h.closeHandle(wrapper);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    const root = owner.current.?;
    // A freshly created Lite DB starts with a default full-text index
    // (full_text_index_v0); remove it so this really is a no-consumer DB.
    _ = try root.db.deleteIndex("full_text_index_v0");

    try std.testing.expect(!owner.registeredDemandForTest().any());

    // Ten primary-only writes with zero registered consumers. This change
    // (WS7, scheduling only) never touches whether a replay/journal record
    // is written for them (that gate belongs to the commit-path rewrite,
    // WS5) — only whether a background turn wakes for them. addIndex below
    // must still find and index every one of them from primary, exactly as
    // it does today (it backfills by scanning primary directly, not by
    // replaying the change journal; see storage/db/catalog/index_manager.zig
    // backfillTextIndex).
    try root.db.batch(.{ .writes = &.{
        .{ .key = "doc:0", .value = "{\"search_text\":\"raft snapshot number 0\"}" },
        .{ .key = "doc:1", .value = "{\"search_text\":\"raft snapshot number 1\"}" },
        .{ .key = "doc:2", .value = "{\"search_text\":\"raft snapshot number 2\"}" },
        .{ .key = "doc:3", .value = "{\"search_text\":\"raft snapshot number 3\"}" },
        .{ .key = "doc:4", .value = "{\"search_text\":\"raft snapshot number 4\"}" },
        .{ .key = "doc:5", .value = "{\"search_text\":\"raft snapshot number 5\"}" },
        .{ .key = "doc:6", .value = "{\"search_text\":\"raft snapshot number 6\"}" },
        .{ .key = "doc:7", .value = "{\"search_text\":\"raft snapshot number 7\"}" },
        .{ .key = "doc:8", .value = "{\"search_text\":\"raft snapshot number 8\"}" },
        .{ .key = "doc:9", .value = "{\"search_text\":\"raft snapshot number 9\"}" },
    } });

    {
        var lease = try connection.acquire();
        defer {
            lease.close();
            connection.gate.mutex.unlock(io);
        }
        try root.db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    }

    var result = try root.db.search(a, .{ .query = .{ .match_all = {} }, .limit = 100 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 10), result.total_hits);
}

test "issue1015 issue1431 unverified peer opens yield to a detached proof turn" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("proof-turn-peer-open", 1000, .batch);
    defer fixture.deinit();
    const owner = fixture.connection.owner;
    const io = h.handleLockIo();
    {
        // Startup maintenance may already have sealed the fixture. Publish an
        // uncertified raw tip so this exercises proof admission, rather than
        // the deliberately permitted certified-peer fast path.
        var lease = try native.lockWriterPathWithIo(alloc, io, fixture.path);
        defer lease.close();
        var foreign = try native.NativeFile.openWithIo(alloc, io, fixture.path, .{ .externally_locked = true, .no_sync = true });
        defer foreign.close();
        try foreign.putCatalogRecord("uncertified-peer-tip", "changed");
        try std.testing.expect(!try foreign.inheritAllocatorCertificate());
    }
    owner.gate.mutex.lockUncancelable(io);
    const retained = owner.retainMaintenanceTurnAssumeLocked();
    owner.gate.mutex.unlock(io);
    try std.testing.expect(try retained);
    var turn_held = true;
    defer if (turn_held) {
        owner.gate.mutex.lockUncancelable(io);
        owner.releaseMaintenanceTurnAssumeLocked();
        owner.gate.mutex.unlock(io);
    };
    // A different durability policy creates an independent owner on the same
    // path. Opening succeeds lazily without publishing its index leases.
    const peer = try open(fixture.path, .{ .profile = .native, .no_sync = false, .busy_timeout_ms = 1000 }, false);
    const peer_id = api.publishHandle(peer) catch |err| {
        h.closeHandle(peer);
        return err;
    };
    defer api.antfly_db_close(peer_id);
    const peer_owner = peer.lite_connection.?.owner;
    try std.testing.expect(peer_owner != owner);
    peer_owner.stopping.store(true, .release);
    peer_owner.wake.set(io);
    peer_owner.worker.?.join();
    peer_owner.worker = null;
    peer_owner.stopping.store(false, .release);
    peer_owner.verification_cancel.requested.store(false, .release);
    try std.testing.expect(peer_owner.current == null);
    owner.gate.mutex.lockUncancelable(io);
    owner.releaseMaintenanceTurnAssumeLocked();
    owner.gate.mutex.unlock(io);
    turn_held = false;
    const writes = [_]h.capi.WriteIntent{.{ .key = .fromSlice("after-proof-turn"), .value = .fromSlice("{}"), .is_delete = false }};
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch(peer_id, &writes, writes.len, null, 0, 0, 0));
    try std.testing.expectEqual(@as(u64, 1), peer_owner.publication_stats.allocator_proof_walks);
    try std.testing.expectEqual(@as(usize, 0), peer_owner.maintenance_turn_references);
}

test "issue1431 unindexed diagnostics do not wait for a peer allocator proof" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const Scenario = enum { indexed, unindexed, named_indexed, pending_commit, pending_ddl, pending_fk };
    for (std.meta.tags(Scenario)) |scenario| {
        const recovery_key: ?[]const u8 = switch (scenario) {
            .pending_commit => "\x00antfly/embedded/sql-commit/v1",
            .pending_ddl => "\x00antfly/embedded/sql-ddl",
            .pending_fk => "\x00antfly/embedded/sql-fk-publication",
            else => null,
        };
        var fixture = try AdmissionFollowupFixture.init("unindexed-proof-diagnostic", 1000, .batch);
        defer fixture.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
        const owner = fixture.connection.owner;
        const io = h.handleLockIo();
        {
            var lease = try fixture.connection.acquire();
            defer {
                lease.close();
                owner.gate.mutex.unlock(io);
            }
            try owner.current.?.db.addIndex(.{ .name = "proof-index", .kind = .full_text, .config_json = "{}" });
            if (scenario != .indexed) try std.testing.expect(try owner.current.?.db.deleteIndex("proof-index"));
            if (scenario == .named_indexed) {
                try @import("tables.zig").create(owner.current.?, "named", "{}", false);
                const named = try @import("tables.zig").get(owner.current.?, "named");
                try named.addIndex(.{ .name = "named-index", .kind = .full_text, .config_json = "{}" });
                _ = try named.indexTextKernelDocuments("named-index", &.{.{
                    .id = "named-doc",
                    .stored_data = "{}",
                    .text_fields = &.{.{ .field_name = "body", .text = "named diagnostic reader" }},
                    .doc_ordinal = 1,
                }});
            }
            if (recovery_key) |key| {
                var write = try owner.current.?.db.core.store.beginWriteTxn();
                errdefer write.abort();
                // The guard must refuse before recovery parses this marker.
                try write.put(key, "{}");
                try write.commit();
            }
        }
        var expected: native.Header = undefined;
        {
            var lease = try native.lockWriterPathWithIo(alloc, io, fixture.path);
            defer lease.close();
            var foreign = try native.NativeFile.openWithIo(alloc, io, fixture.path, .{ .externally_locked = true, .no_sync = true });
            defer foreign.close();
            try foreign.putCatalogRecord("unsealed-status-tip", "peer proof in progress");
            expected = foreign.header;
            try std.testing.expect(!try foreign.inheritAllocatorCertificate());
        }
        owner.gate.mutex.lockUncancelable(io);
        const retained = owner.retainMaintenanceTurnAssumeLocked();
        owner.gate.mutex.unlock(io);
        try std.testing.expect(try retained);
        var held = true;
        defer if (held) {
            owner.gate.mutex.lockUncancelable(io);
            owner.releaseMaintenanceTurnAssumeLocked();
            owner.gate.mutex.unlock(io);
        };
        const peer = try open(fixture.path, .{ .profile = .native, .no_sync = false, .busy_timeout_ms = 0 }, false);
        const peer_id = api.publishHandle(peer) catch |err| {
            h.closeHandle(peer);
            return err;
        };
        defer api.antfly_db_close(peer_id);
        const peer_owner = peer.lite_connection.?.owner;
        peer_owner.stopping.store(true, .release);
        peer_owner.wake.set(io);
        peer_owner.worker.?.join();
        peer_owner.worker = null;
        peer_owner.stopping.store(false, .release);
        peer_owner.verification_cancel.requested.store(false, .release);
        var out: h.capi.Buffer = .{};
        defer api.antfly_buffer_free_zero(&out);
        try std.testing.expectEqual(if (scenario == .unindexed) h.capi.ErrorCode.ok else h.capi.ErrorCode.busy, api.antfly_db_pending_work_stats_json(peer_id, &out));
        try std.testing.expectEqual(@as(u64, 0), peer_owner.publication_stats.allocator_proof_walks);
        {
            var probe = try native.NativeFile.openWithIo(alloc, io, fixture.path, .{ .read_only = true, .internal_reader = true });
            defer probe.close();
            try std.testing.expect(std.meta.eql(expected, probe.header));
        }
        // The diagnostic exception grants no allocation authority.
        const writes = [_]h.capi.WriteIntent{.{ .key = .fromSlice("after-read"), .value = .fromSlice("{}"), .is_delete = false }};
        try std.testing.expectEqual(h.capi.ErrorCode.busy, api.antfly_db_batch(peer_id, &writes, writes.len, null, 0, 0, 0));
        owner.gate.mutex.lockUncancelable(io);
        owner.releaseMaintenanceTurnAssumeLocked();
        owner.gate.mutex.unlock(io);
        held = false;
        if (recovery_key) |key| {
            var lease = try native.lockWriterPathWithIo(alloc, io, fixture.path);
            defer lease.close();
            var foreign = try native.NativeFile.openWithIo(alloc, io, fixture.path, .{ .externally_locked = true, .no_sync = true });
            defer foreign.close();
            try foreign.deleteDocument(key);
        }
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch(peer_id, &writes, writes.len, null, 0, 0, 0));
    }
}

test "issue1015 issue1431 warm SQL retains the incremental allocator digest" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("warm-sql-digest", 1000, .sql);
    defer fixture.deinit();
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(fixture.id, .fromSlice("{\"statement\":\"INSERT INTO numbers (n, _id) VALUES (1, 'warm')\"}"), &output));
    api.antfly_buffer_free_zero(&output);
    const owner = fixture.connection.owner;
    const root = owner.current.?;
    const ledger = root.owned_lite_backend.?.native_docstore.?.file.ledger.?;
    for (0..8) |i| {
        const before = ledger.digestWork();
        var request: [160]u8 = undefined;
        const bytes = try std.fmt.bufPrint(&request, "{{\"statement\":\"INSERT INTO numbers (n, _id) VALUES (1, 'warm-{d}')\"}}", .{i});
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(fixture.id, .fromSlice(bytes), &output));
        api.antfly_buffer_free_zero(&output);
        try std.testing.expect(owner.current == root);
        try std.testing.expect(root.owned_lite_backend.?.native_docstore.?.file.ledger == ledger);
        try std.testing.expect(ledger.digestWork() - before < 1000);
    }
}

test "capi native allocator proof issue1362 storage publication fences a named table without opening its indexes" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("publication-metadata-only", 1000, .sql);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.sql));
    const owner = fixture.connection.owner;
    const root = owner.current.?;
    const database = try @import("tables.zig").get(root, "numbers");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var catalog = try PublicationCatalog.capture(arena.allocator(), root, database);
    defer catalog.deinit();
    var publication: PreparedPublication = .{
        .owner = owner,
        .name = "numbers",
        .identity = database.core.identity_namespace,
        .incarnation = database.root_incarnation,
        .catalog = &catalog,
        .index_prefix = "unused/",
        .snapshot_root = root,
        .snapshot_database = database,
        .source_read = null,
        .relational_rows = false,
    };
    // Keep the detached database alive, but remove it from the serving cache.
    // The old validation path would reopen it and publish index-reader leases.
    const removed = root.embedded_tables.fetchRemove("numbers").?;
    defer root.embedded_tables.put(root.alloc, removed.key, removed.value) catch unreachable;
    const header = root.owned_lite_backend.?.native_docstore.?.file.header;
    try publication.validateStorageCurrent();
    try std.testing.expect(!root.embedded_tables.contains("numbers"));
    try std.testing.expectEqualDeep(header, root.owned_lite_backend.?.native_docstore.?.file.header);
    const id = root.embedded_table_records.getPtr("numbers").?;
    const original_id = id.*;
    id.* += 1;
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateStorageCurrent());
    id.* = original_id;
    publication.incarnation +%= 1;
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateStorageCurrent());
    publication.incarnation -%= 1;
    var txn = try database.core.store.beginWriteTxn();
    errdefer txn.abort();
    try txn.put(keys.public_schema_json_key, "{\"changed\":true}");
    try txn.commit();
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateStorageCurrent());
    try std.testing.expect(!root.embedded_tables.contains("numbers"));
}

test "capi native allocator proof live commits carry a checked peer certificate" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("live-proof-certificate", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    // Keep the writer open: a competing process must not need a quiet whole-
    // graph walk merely because this checked writer is still serving traffic.
    var peer = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), fixture.connection.owner.path, .{ .read_only = true, .internal_reader = true });
    defer peer.close();
    try std.testing.expect(try peer.inheritAllocatorCertificate());
    try std.testing.expect(peer.free_pages_verified);
    try std.testing.expect((try peer.check()).valid);
}

test "capi native allocator proof maintenance yields before refreshing an active proof" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("proof-maintenance-yield", 1000, .batch);
    defer fixture.deinit();
    const Probe = struct {
        connection: *Connection,
        captures: usize = 0,
        failure: ?anyerror = null,
        fn capture(ptr: *anyopaque, _: *native.NativeFile) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.captures += 1;
            self.check() catch |err| {
                self.failure = err;
            };
        }
        fn check(self: *@This()) !void {
            const owner = self.connection.owner;
            try std.testing.expect(owner.allocator_verification_active);
            const captured = owner.current.?;
            if (self.captures == 1) {
                // A peer moves the tip while the foreground owns a detached
                // proof. Background refresh must not reopen index sources and
                // publish more leases before checking that proof's admission.
                var change = AdmissionFollowupProbe{ .connection = self.connection, .captures = 1 };
                try change.duringProof();
            }
            defer owner.maintenance_storage_pending = false;
            for ([_]bool{ false, true }) |storage_pending| {
                owner.maintenance_storage_pending = storage_pending;
                try std.testing.expect(try owner.maintenanceTurn());
                try std.testing.expect(owner.current.? == captured);
                if (storage_pending) try std.testing.expect(owner.maintenance_storage_pending);
            }
        }
    };
    var probe = Probe{ .connection = fixture.connection };
    fixture.connection.owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
    const result = fixture.call(.batch);
    if (probe.failure) |err| return err;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, result);
    try std.testing.expectEqual(@as(usize, 2), probe.captures);
    try fixture.expectWritten(.batch, true);
}

test "capi native allocator proof admits a slow stable proof after the retry deadline" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    for ([_]AdmissionFollowupOperation{ .batch, .sql, .read }) |operation| {
        var fixture = try AdmissionFollowupFixture.init("slow-stable-proof", 1000, operation);
        defer fixture.deinit();
        var probe: AdmissionFollowupProbe = .{ .connection = fixture.connection, .slow_stable = true };
        fixture.connection.owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = AdmissionFollowupProbe.capture };
        const stats = &fixture.connection.owner.publication_stats;
        const admissions_before = stats.mutation_admission_count;
        const admission_ns_before = stats.mutation_admission_ns;
        const result = fixture.call(operation);
        if (probe.failure) |err| return err;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, result);
        try std.testing.expectEqual(@as(u64, if (operation == .read) 0 else 1), stats.mutation_admission_count - admissions_before);
        if (operation != .read) try std.testing.expect(stats.mutation_admission_ns - admission_ns_before >= std.time.ns_per_s);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, h.capi.lastErrorCode());
        try std.testing.expectEqual(@as(usize, 1), probe.captures);
        try std.testing.expect(fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
        try fixture.expectWritten(operation, true);
        // The finished proof remains usable; an immediate next call does not
        // repeat the expensive scan even though the first call ran past budget.
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.read));
        try std.testing.expectEqual(@as(usize, 1), probe.captures);
    }
}

test "capi native allocator proof capacity reads retry a moving tip before execution" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    for ([_]u64{ 0, 1000 }) |timeout_ms| {
        var fixture = try AdmissionFollowupFixture.init("moving-read-proof", timeout_ms, .read);
        defer fixture.deinit();
        var probe: AdmissionFollowupProbe = .{ .connection = fixture.connection, .capacity_bounded = true };
        fixture.connection.owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = AdmissionFollowupProbe.capture };
        const result = fixture.call(.read);
        if (probe.failure) |err| return err;
        try std.testing.expectEqual(if (timeout_ms == 0) h.capi.ErrorCode.busy else h.capi.ErrorCode.ok, result);
        try std.testing.expectEqual(@as(usize, if (timeout_ms == 0) 1 else 2), probe.captures);
    }
}

test "capi native allocator proof cancelled backoff does not reenter admission" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    for ([_]AdmissionFollowupOperation{ .batch, .sql }) |operation| {
        var fixture = try AdmissionFollowupFixture.init("cancelled-proof-backoff", 1000, operation);
        defer fixture.deinit();
        var probe: AdmissionFollowupProbe = .{ .connection = fixture.connection };
        fixture.connection.owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = AdmissionFollowupProbe.capture };
        fixture.connection.test_admission_sleep = .{ .ptr = &probe, .sleep = AdmissionFollowupProbe.cancelSleep };
        const result = fixture.call(operation);
        if (probe.failure) |err| return err;
        try std.testing.expectEqual(h.capi.ErrorCode.cancelled, result);
        try std.testing.expectEqual(h.capi.ErrorCode.cancelled, h.capi.lastErrorCode());
        try std.testing.expectEqual(@as(usize, 1), probe.captures);
        try std.testing.expectEqual(@as(usize, 1), probe.sleep_calls);
        try std.testing.expect(fixture.connection.mutex.tryLock());
        fixture.connection.mutex.unlock(h.handleLockIo());
        try fixture.expectWritten(operation, false);
    }
}

test "capi native allocator proof initial admission survives a long local queue wait" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const io = h.handleLockIo();
    var fixture = try AdmissionFollowupFixture.init("queued-stable-proof", 1000, .batch);
    defer fixture.deinit();
    const Writer = struct {
        fixture: *AdmissionFollowupFixture,
        result: h.capi.ErrorCode = .busy,
        fn run(self: *@This()) void {
            self.result = self.fixture.call(.batch);
        }
    };
    var writer: Writer = .{ .fixture = &fixture };
    const mutex = &fixture.connection.mutex;
    mutex.lockUncancelable(io);
    const thread = std.Thread.spawn(.{}, Writer.run, .{&writer}) catch |err| {
        mutex.unlock(io);
        return err;
    };
    // Observe an actual queued ticket: the writer has already calculated its
    // admission deadline before it parks behind this connection's API call.
    while (true) {
        mutex.state.lockUncancelable(io);
        const queued = mutex.next == mutex.serving +% 2;
        mutex.state.unlock(io);
        if (queued) break;
        io.sleep(.fromMilliseconds(1), .awake) catch {};
    }
    io.sleep(.fromMilliseconds(1001), .awake) catch {};
    mutex.unlock(io);
    thread.join();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, writer.result);
    try fixture.expectWritten(.batch, true);
}

test "capi native allocator proof issue1431 committed transform visibility timeout is not Busy" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("committed-transform-visibility", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const root = fixture.connection.owner.current.?;
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        try root.db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    }
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    const seed = "{\"inserts\":{\"counter\":{\"body\":\"counter\",\"count\":0}},\"sync_level\":\"write\"}";
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice(seed), &output));
    api.antfly_buffer_free_zero(&output);
    // Native serving handles omit optional runtimes. Install a manual one to
    // reproduce the public batch path's post-commit visibility refusal.
    const workers = root.db.start_index_workers;
    root.db.start_index_workers = true;
    root.db.initOptionalTextMergeRuntime(.{ .enabled = true, .manual_producer_assistance = true }) catch |err| {
        root.db.start_index_workers = workers;
        return err;
    };
    root.db.start_index_workers = workers;
    const runtime = root.db.text_merge_runtime.?;
    runtime.config.max_pending_segments = 12;
    runtime.config.resume_pending_segments = 6;
    runtime.config.max_pending_bytes = 0;
    runtime.config.backpressure_max_wait_ms = 100;
    const active = root.db.core.index_manager.textActiveSegmentCountSnapshot("ft_v1");
    try std.testing.expect(active < runtime.config.max_pending_segments);
    var held = try runtime.acquireProducerPermit("ft_v1", runtime.config.max_pending_segments - active, 0);
    defer held.release();
    const increment = "{\"transforms\":[{\"key\":\"counter\",\"operations\":[{\"op\":\"$inc\",\"path\":\"count\",\"value\":1}]}],\"sync_level\":\"full_text\"}";
    try std.testing.expectEqual(h.capi.ErrorCode.stalled, api.antfly_db_batch_json(fixture.id, .fromSlice(increment), &output));
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(h.capi.lastErrorDetail()), "primary write committed") != null);
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(h.capi.lastErrorDetail()), "do not retry") != null);
    const raw = (try root.db.get(std.testing.allocator, "counter")).?;
    defer std.testing.allocator.free(raw);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, raw, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("count").?.integer);
    held.release();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(fixture.id));
    // A separate refusal before primary execution remains Busy and does not
    // apply the transform. Only derived completion was retried above.
    fixture.connection.options.busy_timeout_ms = 0;
    {
        var lease = try native.lockWriterPathWithIo(alloc, h.handleLockIo(), fixture.path);
        defer lease.close();
        try std.testing.expectEqual(h.capi.ErrorCode.busy, api.antfly_db_batch_json(fixture.id, .fromSlice(increment), &output));
    }
    const completed = (try root.db.get(std.testing.allocator, "counter")).?;
    defer std.testing.allocator.free(completed);
    var visible = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, completed, .{});
    defer visible.deinit();
    try std.testing.expectEqual(@as(i64, 1), visible.value.object.get("count").?.integer);
}

test "capi native allocator proof issue1431 bounded allocator service rebases a moved checkpoint" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("allocator-service-rebase", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    var detached: *h.Handle = undefined;
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        detached = try owner.refreshMaintenanceSnapshot(owner.current.?, false);
    }
    const file = &detached.owned_lite_backend.?.native_docstore.?.file;
    const captured = file.header;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    try std.testing.expect(!std.meta.eql(captured, owner.current.?.owned_lite_backend.?.native_docstore.?.file.header));
    detached.maintenance_owner = owner;
    defer detached.maintenance_owner = null;
    // A prepared vacuum must still refuse this stale capture.
    try std.testing.expectError(error.EnrichmentWaitCanceled, Owner.enterMaintenanceWriter(detached, .reclamation));
    try std.testing.expectEqual(captured, file.header);
    // The exemption is metadata-only: detached document/index writes still
    // require their publication and catalog proof before they can rebase.
    for ([_]PublicationScope{ .primary, .index, .artifact_lease }) |scope| {
        try std.testing.expectError(error.Canceled, Owner.enterMaintenanceWriter(detached, scope));
        try std.testing.expectEqual(captured, file.header);
    }
    const admitted = owner.publication_stats.allocator_service_admissions;
    try Owner.enterMaintenanceWriter(detached, .allocator_service);
    var held = true;
    defer if (held) Owner.leaveMaintenanceWriter(detached);
    try std.testing.expect(std.meta.eql(file.header, owner.current.?.owned_lite_backend.?.native_docstore.?.file.header));
    try std.testing.expectEqual(admitted + 1, owner.publication_stats.allocator_service_admissions);
    _ = try file.reclaimPagesForMaintenanceWithCancel(8, null, true);
    try std.testing.expect((try file.check()).valid);
    Owner.leaveMaintenanceWriter(detached);
    held = false;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.read));
    try fixture.expectWritten(.batch, true);
}

test "capi native logical generations maintenance snapshot replacement keeps the writer's warm allocator ledger" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("snapshot-ledger-handoff", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    const refreshSnapshot = struct {
        fn run(f: *AdmissionFollowupFixture) !*h.Handle {
            var lease = try f.connection.acquire();
            defer {
                lease.close();
                f.connection.gate.mutex.unlock(h.handleLockIo());
            }
            return f.connection.owner.refreshMaintenanceSnapshot(f.connection.owner.current.?, false);
        }
    }.run;
    const writerFile = struct {
        fn get(o: *Owner) *native.NativeFile {
            return &o.current.?.owned_lite_backend.?.native_docstore.?.file;
        }
    }.get;
    // lite-writer-authority.md W1: allocator service runs on the one
    // writable file, so warm that file's ledger under a snapshot's turn.
    const first = try refreshSnapshot(&fixture);
    first.maintenance_owner = owner;
    try Owner.enterMaintenanceWriter(first, .allocator_service);
    _ = try writerFile(owner).reclaimPagesForMaintenanceWithCancel(8, null, true);
    Owner.leaveMaintenanceWriter(first);
    first.maintenance_owner = null;
    try std.testing.expect(writerFile(owner).free_pages_verified and writerFile(owner).ledger != null);
    const full_loads = writerFile(owner).allocator_work.full_loads;

    // A foreground write moves the tip, so the next capture replaces the
    // snapshot. The writer's ledger stays warm: no full reload.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const second = try refreshSnapshot(&fixture);
    try std.testing.expect(second != first);
    try std.testing.expect(writerFile(owner).free_pages_verified);
    try std.testing.expectEqual(full_loads, writerFile(owner).allocator_work.full_loads);
    second.maintenance_owner = owner;
    defer second.maintenance_owner = null;
    try Owner.enterMaintenanceWriter(second, .allocator_service);
    _ = try writerFile(owner).reclaimPagesForMaintenanceWithCancel(8, null, true);
    try std.testing.expect((try writerFile(owner).check()).valid);
    Owner.leaveMaintenanceWriter(second);
    try std.testing.expectEqual(full_loads, writerFile(owner).allocator_work.full_loads);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.read));
    try fixture.expectWritten(.batch, true);
}

test "capi native allocator proof issue1431 physical maintenance releases detached catalog pins" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("maintenance-releases-catalog", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    const Probe = struct {
        calls: usize = 0,
        captured: bool = false,
        failure: ?anyerror = null,
        fn notify(ptr: *anyopaque, root: *h.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (root.maintenance_publication) |publication| {
                self.captured = true;
                if (publication.source_read == null) {
                    self.failure = error.IndexReplayLostDocumentSnapshot;
                    return;
                }
            } else self.calls += 1;
            const during_replay = root.maintenance_publication != null;
            const store = root.owned_lite_backend.?.native_docstore.?;
            const region = (store.file.regionForInode() catch |err| {
                self.failure = err;
                return;
            }) orelse {
                self.failure = error.IndexReplaySnapshotNotFenced;
                return;
            };
            if (during_replay != (region.minimumPinnedSequence() != null)) self.failure = error.IndexReplaySnapshotNotFenced;
        }
    };
    var probe: Probe = .{};
    owner.test_after_capture = .{ .ptr = &probe, .notify = Probe.notify };
    defer owner.test_after_capture = null;
    owner.test_before_storage_service = .{ .ptr = &probe, .notify = Probe.notify };
    defer owner.test_before_storage_service = null;
    for (0..8) |_| {
        owner.maintenance_pending = true;
        _ = try owner.maintenanceTurn();
        if (probe.calls != 0) break;
    }
    try std.testing.expect(probe.calls != 0);
    try std.testing.expect(probe.captured);
    if (probe.failure) |err| return err;
}

test "capi native allocator proof issue1431 refreshed readers distinguish current and old tips" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("fresh-reader-tip", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const owner = fixture.connection.owner;
    const root = owner.current.?;
    const file = &root.owned_lite_backend.?.native_docstore.?.file;
    var value: [8192]u8 = @splat('v');
    for (0..80) |_| try file.putDocument("padding", &value);
    try std.testing.expect((try file.allocatorStats()).?.reusable_pages > 0);
    const coordinator = file.reader_coordinator.?;
    // Emulate publication by a peer: it cannot update this owner's local
    // reader coordinator. The path lease fences admission of the new tip.
    {
        file.reader_coordinator = null;
        defer file.reader_coordinator = coordinator;
        try file.putDocument("peer", "published");
    }
    try std.testing.expect(try owner.refresh() == root);
    var reader = try native.NativeFile.openWithIo(std.testing.allocator, h.handleLockIo(), fixture.path, .{ .read_only = true, .internal_reader = true, .reader_coordinator = coordinator });
    var reader_open = true;
    defer if (reader_open) reader.close();
    try reader.acquireReaderFence(file.activeCheckpoint().commit_sequence, file);
    try std.testing.expect(!reader.reader_fence.?.poisoned);
    const before = (try file.allocatorStats()).?.reused_pages;
    try file.putDocument("resumed", &value);
    try std.testing.expect((try file.allocatorStats()).?.reused_pages > before);
    // The peer scans the captured reader's epoch even without access to its
    // local coordinator. It may reuse unrelated pages while preserving data.
    {
        file.reader_coordinator = null;
        defer file.reader_coordinator = coordinator;
        value[0] = 'w';
        try file.putDocument("padding", &value);
    }
    try std.testing.expect(try owner.refresh() == root);
    try std.testing.expect(!reader.reader_fence.?.poisoned);
    const blocked = (try file.allocatorStats()).?.reused_pages;
    try file.putDocument("still-fenced", "safe");
    try std.testing.expect((try file.allocatorStats()).?.reused_pages > blocked);
    const captured = (try reader.getDocumentAlloc(std.testing.allocator, "padding")).?;
    defer std.testing.allocator.free(captured);
    try std.testing.expectEqual(@as(u8, 'v'), captured[0]);
    reader.close();
    reader_open = false;
    try file.putDocument("after-reader-close", "safe");
    try std.testing.expect((try file.allocatorStats()).?.reused_pages > blocked);
    try std.testing.expect((try file.check()).valid);
}

// perf/lwa-change-detect: `Owner.refresh()`'s fast paths used to compare
// only physical state (`std.meta.eql(probe.header, file.header)`,
// `runtimeMetadataUnchanged`/`loadedIndexInputsUnchanged`'s own root-page
// comparisons) between a fresh probe and this Owner's own, possibly
// long-idle `file`. Under the redo log, an ordinary commit on a *different*
// `NativeFile` instance on the same inode -- another connection entirely,
// modeled here the same way `AdmissionFollowupProbe.duringProof` models "a
// peer committed" elsewhere in this file -- advances the logical commit
// sequence without ever touching the physical checkpoint slot until the
// next `checkpointRedoLog` fold, so both fast paths saw no change and kept
// serving `before` forever. This test commits on a second, independent
// `NativeFile` with no fold at all and checks `refresh()` still sees it.
test "capi native sibling connection's refresh sees a redo-only commit without a fold" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("redo-sibling-refresh");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
    defer h.closeHandle(wrapper);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    const before = owner.current.?;
    const before_file = &before.owned_lite_backend.?.native_docstore.?.file;
    try std.testing.expect(before_file.header.redo_log_capacity != 0);
    try std.testing.expect(try before_file.getDocumentAlloc(a, "sibling-redo") == null);

    // A second, independent connection on the same inode commits directly,
    // never through `owner` at all -- durable, but with no fold.
    {
        var lease = try native.lockWriterPathWithIo(a, io, path);
        defer lease.close();
        var sibling = try native.NativeFile.openWithIo(a, io, path, .{ .externally_locked = true, .no_sync = true });
        defer sibling.close();
        try sibling.putDocument("sibling-redo", "{\"text\":\"redo-only\"}");
        // Confirmed redo-only: the logical sequence moved past the real,
        // on-disk fold boundary this checkpoint slot still reflects.
        try std.testing.expect(sibling.redo != null);
        try std.testing.expect(sibling.activeCheckpoint().redo_folded_through < sibling.committedSequence());
    }

    var lease = try connection.acquire();
    defer {
        lease.close();
        connection.gate.mutex.unlock(io);
    }
    const after = try owner.refresh();
    try std.testing.expect(after != before);
    const value = try after.owned_lite_backend.?.native_docstore.?.file.getDocumentAlloc(a, "sibling-redo");
    defer if (value) |bytes| a.free(bytes);
    try std.testing.expect(value != null);
}

// perf/lwa-enrich-visibility: `Owner.refresh()`'s retirement block used to
// mark a superseded generation read-only (`previous.owned_lite_backend.?.
// native_docstore.?.read_only = true`) whenever `maintenance_references ==
// 0 and immutable_query_borrows == 0` -- never checking `lane_pins` at all.
// `WriterLane.checkout`'s own doc comment says a pinned root's `lane_pins`
// is kept nonzero "for as long as this call might still touch it, including
// while its own ticket is still outstanding" -- exactly a `sync_level:
// .enrichments`/`.full_index` write's own derived-visibility wait (WS3's
// `releaseWriterLaneBeforeWait`), which keeps driving further writes
// against this same generation with no connection-level lock held across
// it. A concurrent `refresh` (a sibling write, or the maintenance owner's
// own republish) could retire and mark read-only the exact generation that
// pinned write was still using, surfacing live as
// `test_grouped_sources_drive_downstream_graph`'s table-scoped variants
// intermittently failing `ANTFLY_STALLED` with the underlying cause
// `error.ReadOnly`, or escalating into native.zig's "stale writer base
// reached publishCheckpoint" panic once a write actually landed in the
// gap. Fixed by `markRetiredReadOnlyIfIdle` deferring the marking (and
// `collectRetired` rechecking it once the pin clears).
test "capi native refresh during a lane-pinned write does not mark that generation read-only" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("lane-pin-refresh-readonly");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
    defer h.closeHandle(wrapper);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);

    // Simulate a lane-eligible mutation's own admission
    // (`enterCapturedHandleWithDeadline`'s `WriterLane.checkout` call):
    // `pinned_root.lane_pins` is now nonzero, exactly as it is for the
    // whole span of a real write's own derived-visibility wait. This also
    // holds the lane's own kernel-level lease, so forcing staleness via an
    // *external* sibling writer (the shape the test above uses) would
    // contend with that same lease (`lockWriterPathWithIo` would return
    // `error.WouldBlock`) rather than reproducing the actual bug, which
    // needs no second writer at all -- only a second *caller* of
    // `Owner.refresh()` while this pin is outstanding (exactly
    // `PreparedPublication.acquireStore`/`.current()`'s own call from the
    // maintenance owner's republish pass). Force the identical slow-path
    // decision `refresh()`'s own fast-path check makes
    // (`!needsOutcomeReconciliation(current)`) the cheap way: flip one of
    // the three flags that function reads directly, no lock or second
    // writer required.
    const pinned_root = try owner.writer_lane.checkout(connection, .mutation);
    try std.testing.expect(pinned_root.lane_pins.load(.acquire) > 0);
    const store = pinned_root.owned_lite_backend.?.native_docstore.?;
    pinned_root.sql_decision_uncertain = true;
    // A second, independent reason this generation cannot be fully reaped
    // yet (`collectRetired`'s own retention check, not
    // `markRetiredReadOnlyIfIdle`'s -- `query_borrows`, unlike
    // `immutable_query_borrows`, only gates the former): without it,
    // checking the lane pin in below would make `collectRetired` destroy
    // `pinned_root` outright (nothing else references it in this test),
    // leaving no handle left to assert the read-only marking landed on.
    pinned_root.query_borrows += 1;

    // `refresh()` must retire `pinned_root` (a fresh generation reopens
    // from durable state, the ordinary reconciliation path for an
    // uncertain decision) but must not mark it read-only while this
    // call's own lane pin is still outstanding.
    const after = try owner.refresh();
    try std.testing.expect(after != pinned_root);
    try std.testing.expect(!store.read_only);
    try std.testing.expect(!store.file.read_only);

    // Not just an unchecked flag: the write path itself still succeeds
    // through the pinned generation while the pin holds.
    try store.file.putDocument("still-writable-while-pinned", "{\"text\":\"ok\"}");

    // Checking the lane in (`leaveWithRegistry`'s own `WriterLane.checkin`
    // call, which hands the kernel lease onward and detaches the
    // connection) and unpinning (that same function's *separate*
    // `lane_pinned_root.lane_pins.fetchSub(1, .release)`, run after
    // `checkin` -- `WriterLane.checkin` itself never touches `lane_pins`
    // at all, by design: see `WriterLane.checkout`'s own doc comment,
    // "the caller... must decrement it exactly once, in
    // `leaveWithRegistry`") together simulate a real write's own call
    // finishing after its derived-visibility wait resolves. Only once
    // both have happened should the deferred marking land, via
    // `collectRetired`'s own recheck -- the retired generation stays
    // writable only for as long as the write it was protecting is still
    // outstanding, not forever.
    owner.writer_lane.checkin(connection, store);
    _ = pinned_root.lane_pins.fetchSub(1, .release);
    owner.collectRetired();
    try std.testing.expect(store.read_only);
    try std.testing.expect(store.file.read_only);
    pinned_root.query_borrows -= 1;
    owner.collectRetired();
}

/// Every fork-based test below waits on a real second OS process across
/// these bounds, same generous reasoning as `native.zig`'s own
/// `cross_process_fork_read_timeout_ms`/`cross_process_fork_reap_timeout_ms`
/// (duplicated here, private to this file, rather than exported: the first
/// fork-based test in this file to need it).
const xproc_fork_read_timeout_ms: i32 = 30_000;
const xproc_fork_reap_timeout_ms: i64 = 10_000;

fn xprocReadBounded(fd: std.c.fd_t, buf: []u8, timeout_ms: i32) !void {
    var filled: usize = 0;
    while (filled < buf.len) {
        var fds = [_]std.c.pollfd{.{ .fd = fd, .events = std.c.POLL.IN, .revents = 0 }};
        const ready = std.c.poll(&fds, 1, timeout_ms);
        if (ready <= 0) return error.Timeout;
        const n = std.c.read(fd, buf[filled..].ptr, buf.len - filled);
        if (n <= 0) return error.PipeClosed;
        filled += @intCast(n);
    }
}

fn xprocReapBounded(pid: std.c.pid_t, timeout_ms: i64) !void {
    var status: c_int = 0;
    var waited_ms: i64 = 0;
    const poll_ms = 5;
    while (true) {
        const got = std.c.waitpid(pid, &status, std.c.W.NOHANG);
        if (got == pid) return;
        if (got < 0) return error.WaitFailed;
        if (waited_ms >= timeout_ms) {
            _ = std.c.kill(pid, std.c.SIG.KILL);
            while (std.c.waitpid(pid, &status, 0) < 0) {}
            return error.ChildTimedOut;
        }
        var no_fds: [0]std.c.pollfd = .{};
        _ = std.c.poll(&no_fds, 0, poll_ms);
        waited_ms += poll_ms;
    }
}

fn xprocParkForever() noreturn {
    while (true) {
        var no_fds: [0]std.c.pollfd = .{};
        _ = std.c.poll(&no_fds, 0, 20);
    }
}

// perf/lwa-xproc-refresh: the sibling test right above
// (`perf/lwa-change-detect`) proves `Owner.refresh()` sees a redo-only
// commit from a second, *same-process* `NativeFile` on this inode. That is
// not the same claim as "sees a commit from a different process": a
// same-process sibling commits straight into the one in-memory
// `SharedRedoOverlay` this inode's `file` and every sibling already share
// (`inode_lock.ensureRedoOverlay`'s own doc comment -- one overlay per
// inode, per process), so a stale `file` only ever needs its *bound*
// (`committedSequence()`) bumped to see it, which a reopen's own
// independent physical log scan already did correctly even before this
// fix. A genuinely different process never reaches that shared instance at
// all: `ensureRedoOverlay` trusts an already-populated, same-pid overlay
// verbatim and never re-populates it on a reopen, so -- before this fix --
// `Owner.refresh()`'s slow reopen path advanced the replacement's bound
// (its own fresh physical log scan found the truth) while silently
// inheriting this process's existing, still-incomplete overlay instance:
// a lookup for a key that exists only in the forked child's logical
// mutations kept resolving not-found no matter how many times `refresh()`
// ran. Forked here, not simulated in-process, specifically to defeat that
// shortcut the same way a real second process would -- matching the
// reported bug (a long-lived connection in one process never sees a
// commit a different process made under the logical redo log).
test "capi native owner refresh observes a redo-diverted commit from a genuinely separate process" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .windows) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("xproc-owner-refresh");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
    defer h.closeHandle(wrapper);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    const before = owner.current.?;
    const before_file = &before.owned_lite_backend.?.native_docstore.?.file;
    try std.testing.expect(before_file.header.redo_log_capacity != 0);
    try std.testing.expect(try before_file.getDocumentAlloc(a, "xproc-redo") == null);

    var result_fds: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(0, std.c.pipe(&result_fds));

    const child_pid = std.c.fork();
    if (child_pid == 0) {
        _ = std.posix.system.close(result_fds[0]);
        var lease = native.lockWriterPathWithIo(a, io, path) catch xprocParkForever();
        var writer = native.NativeFile.openWithIo(a, io, path, .{ .externally_locked = true, .no_sync = true }) catch xprocParkForever();
        writer.putDocument("xproc-redo", "{\"text\":\"written-by-child-process\"}") catch xprocParkForever();
        // Confirmed redo-only, exactly like the same-process sibling test
        // above: the logical sequence moved past the real, on-disk fold
        // boundary this checkpoint slot still reflects.
        if (writer.redo == null) xprocParkForever();
        if (!(writer.activeCheckpoint().redo_folded_through < writer.committedSequence())) xprocParkForever();
        writer.close();
        lease.close();
        const report: [1]u8 = .{1};
        _ = std.c.write(result_fds[1], &report, report.len);
        xprocParkForever();
    }
    _ = std.posix.system.close(result_fds[1]);
    var child_reaped = false;
    defer if (!child_reaped) {
        _ = std.c.kill(child_pid, std.c.SIG.KILL);
        _ = std.c.waitpid(child_pid, null, 0);
    };
    defer _ = std.posix.system.close(result_fds[0]);

    var report: [1]u8 = undefined;
    try xprocReadBounded(result_fds[0], &report, xproc_fork_read_timeout_ms);
    try std.testing.expectEqual(@as(u8, 1), report[0]);

    var lease = try connection.acquire();
    defer {
        lease.close();
        connection.gate.mutex.unlock(io);
    }
    const after = try owner.refresh();
    const value = try after.owned_lite_backend.?.native_docstore.?.file.getDocumentAlloc(a, "xproc-redo");
    defer if (value) |bytes| a.free(bytes);
    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("{\"text\":\"written-by-child-process\"}", value.?);

    _ = std.c.kill(child_pid, std.c.SIG.KILL);
    try xprocReapBounded(child_pid, xproc_fork_reap_timeout_ms);
    child_reaped = true;
}

// Regression for py/packages/embedded's
// test_cross_process_writers_do_not_lose_each_others_rows: two genuinely
// separate OS processes, each holding the cross-process writer lease for a
// handful of commits in turn, must never lose a sibling's committed rows --
// no SEGFAULT, no InvalidBatchRequest, no FileBusy once each writer's own
// lease wait succeeds. Forked (not simulated in-process) for the same
// reason as the redo-observation test above: a same-process sibling shares
// this process's one `SharedRedoOverlay`/allocator state per inode, which
// would hide a bug that only a genuinely different process's independent
// open can expose.
test "capi native two genuinely separate writer processes do not lose each other's rows" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .windows) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("xproc-two-writers");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
    defer h.closeHandle(wrapper);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);

    const writers_per_child = 8;
    const Child = struct {
        // `lockWriterPathWithIo` is a single non-blocking try (WouldBlock on
        // contention, never a wait): poll it the way every real caller must
        // under genuine cross-process contention from the sibling writer
        // below, bounded the same as the Python repro's busy_timeout=10.
        fn acquireWithRetry(child_io: std.Io, child_path: []const u8) native.PathWriterLock {
            const deadline_ns = std.Io.Clock.awake.now(child_io).nanoseconds + 10 * std.time.ns_per_s;
            while (true) {
                if (native.lockWriterPathWithIo(alloc, child_io, child_path)) |lease| return lease else |err| switch (err) {
                    error.WouldBlock, error.FileBusy => {
                        if (std.Io.Clock.awake.now(child_io).nanoseconds >= deadline_ns) xprocParkForever();
                        var no_fds: [0]std.c.pollfd = .{};
                        _ = std.c.poll(&no_fds, 0, 2);
                    },
                    else => xprocParkForever(),
                }
            }
        }
        fn run(child_path: []const u8, writer_id: u8, result_fd: std.c.fd_t) noreturn {
            const child_io = h.handleLockIo();
            var key_buffer: [8]u8 = undefined;
            var i: u8 = 0;
            while (i < writers_per_child) : (i += 1) {
                var lease = acquireWithRetry(child_io, child_path);
                var writer = native.NativeFile.openWithIo(alloc, child_io, child_path, .{ .externally_locked = true, .no_sync = true }) catch xprocParkForever();
                const key = std.fmt.bufPrint(&key_buffer, "{d}:{d}", .{ writer_id, i }) catch xprocParkForever();
                var value_buffer: [64]u8 = undefined;
                const value = std.fmt.bufPrint(&value_buffer, "{{\"body\":\"writer {d} item {d}\"}}", .{ writer_id, i }) catch xprocParkForever();
                writer.putDocument(key, value) catch xprocParkForever();
                writer.close();
                lease.close();
            }
            const report: [1]u8 = .{1};
            _ = std.c.write(result_fd, &report, report.len);
            xprocParkForever();
        }
    };

    var result_fds: [2][2]std.c.fd_t = undefined;
    for (&result_fds) |*fds| try std.testing.expectEqual(0, std.c.pipe(fds));

    var child_pids: [2]std.c.pid_t = undefined;
    for (0..2) |writer_id| {
        const pid = std.c.fork();
        if (pid == 0) {
            for (0..2) |other| {
                if (other != writer_id) _ = std.posix.system.close(result_fds[other][1]);
            }
            _ = std.posix.system.close(result_fds[writer_id][0]);
            Child.run(path, @intCast(writer_id), result_fds[writer_id][1]);
        }
        try std.testing.expect(pid > 0);
        child_pids[writer_id] = pid;
    }
    for (&result_fds) |*fds| _ = std.posix.system.close(fds[1]);
    var children_reaped = false;
    defer if (!children_reaped) for (child_pids) |pid| {
        _ = std.c.kill(pid, std.c.SIG.KILL);
        _ = std.c.waitpid(pid, null, 0);
    };
    defer for (&result_fds) |*fds| {
        _ = std.posix.system.close(fds[0]);
    };

    for (&result_fds) |*fds| {
        var report: [1]u8 = undefined;
        try xprocReadBounded(fds[0], &report, xproc_fork_read_timeout_ms);
        try std.testing.expectEqual(@as(u8, 1), report[0]);
    }

    var lease = try connection.acquire();
    defer {
        lease.close();
        connection.gate.mutex.unlock(io);
    }
    const after = try owner.refresh();
    const file = &after.owned_lite_backend.?.native_docstore.?.file;
    for (0..2) |writer_id| {
        var i: u8 = 0;
        while (i < writers_per_child) : (i += 1) {
            var key_buffer: [8]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "{d}:{d}", .{ writer_id, i });
            const value = try file.getDocumentAlloc(a, key);
            defer if (value) |bytes| a.free(bytes);
            try std.testing.expect(value != null);
            var expected_buffer: [64]u8 = undefined;
            const expected = try std.fmt.bufPrint(&expected_buffer, "{{\"body\":\"writer {d} item {d}\"}}", .{ writer_id, i });
            try std.testing.expectEqualStrings(expected, value.?);
        }
    }

    for (child_pids) |pid| _ = std.c.kill(pid, std.c.SIG.KILL);
    for (child_pids) |pid| try xprocReapBounded(pid, xproc_fork_reap_timeout_ms);
    children_reaped = true;
}

// perf/lwa-xproc-writers item 4: a cold-opened writer whose opportunistic
// lock attempt in `open()`'s `!create` path loses its race leaves
// `self.current` null when `Owner.maintenanceLoop` spawns. Before this fix,
// `registeredDemand()`'s `self.current orelse return .none` made that
// unknown state look like "nothing registered", parking the loop's own
// wake `Io.Event` forever (`no_scheduled_wait`) with nothing left to ever
// wake it -- not even the durable dense-index generator backlog seeded
// below. No foreground call on `wrapper`/`connection` ever runs here: the
// background loop alone must discover and drain it.
test "capi native cold-opened owner drains durable enrichment backlog with no foreground call" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("cold-owner-enrichment-drain");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);

    // Seed durable state and close cleanly: an index with an embedded
    // generator and one document needing it, committed, with no owner
    // left registered on this path afterward -- the same durable shape a
    // genuinely separate process (perf/lwa-xproc-writers's own motivating
    // scenario) leaves behind once it exits.
    {
        const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
        const id = try api.publishHandle(wrapper);
        defer api.antfly_db_close(id);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(
            "{\"name\":\"semantic\",\"kind\":\"dense_vector\",\"config_json\":\"{\\\"field\\\":\\\"embedding\\\",\\\"dims\\\":3,\\\"metric\\\":\\\"l2_squared\\\",\\\"generator\\\":{\\\"kind\\\":\\\"dense_embedding\\\",\\\"source_field\\\":\\\"body\\\",\\\"embedding_name\\\":\\\"dense_v1\\\"}}\"}",
        )));
        var out: h.capi.Buffer = .{};
        defer api.antfly_buffer_free_zero(&out);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice(
            "{\"inserts\":{\"doc\":{\"body\":\"durable generated work\"}},\"sync_level\":\"write\"}",
        ), &out));
    }

    // Cold reopen: a brand new Owner/maintenanceLoop. No foreground call
    // on this connection follows -- only the background thread can make
    // the enrichment runtime's `applied_sequence` reach `target_sequence`.
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, false);
    defer h.closeHandle(wrapper);
    const owner = wrapper.lite_connection.?.owner;

    const deadline_ns = std.Io.Clock.awake.now(io).nanoseconds + 10 * std.time.ns_per_s;
    while (true) {
        const applied = blk: {
            const root = owner.current orelse break :blk 0;
            const runtime = root.db.enrichment_runtime orelse break :blk 0;
            break :blk runtime.stats().applied_sequence;
        };
        if (applied >= 1) break;
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline_ns) return error.EnrichmentNeverDrained;
        var no_fds: [0]std.c.pollfd = .{};
        _ = std.c.poll(&no_fds, 0, 20);
    }
}

test "capi native recovery agenda clean opens avoid unrelated table runtimes" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("recovery-agenda-discovery", 5000, .sql);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const root = fixture.connection.owner.current.?;
    for (0..8) |_| try @import("sql_ddl.zig").recover(root);
    try std.testing.expect(!root.sql_ddl_recovery_needed);
    @import("tables.zig").closeAll(root);
    root.sql_ddl_recovery_needed = true;
    root.sql_ddl_agenda_after_len = 0;
    try @import("sql_ddl.zig").recover(root);
    try std.testing.expectEqual(@as(u32, 0), root.embedded_tables.count());
    try std.testing.expect(!root.sql_ddl_recovery_needed);
    // A native producer bypassing SQL's invalidation still changes the durable
    // agenda and metadata generation. The cached clean flag cannot hide it.
    const database = try @import("tables.zig").get(root, "numbers");
    const schema = (try database.getSchemaJson(root.alloc)).?;
    defer root.alloc.free(schema);
    var txn = try database.core.store.beginWriteTxn();
    errdefer txn.abort();
    try txn.put(keys.public_schema_json_key, schema);
    try txn.commit();
    try std.testing.expect(!root.sql_ddl_recovery_needed);
    const before = root.sql_ddl_recovery_scans;
    try @import("sql_ddl.zig").recover(root);
    try std.testing.expectEqual(before + 1, root.sql_ddl_recovery_scans);
    try std.testing.expect(!root.sql_ddl_recovery_needed);
}

test "capi native logical generations adopt foreign lease checkpoints without reopening runtime" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("logical-generation-adoption", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const owner = fixture.connection.owner;
    const root = owner.current.?;
    const file = &root.owned_lite_backend.?.native_docstore.?.file;
    var peer = try native.NativeFile.openWithIo(std.testing.allocator, h.handleLockIo(), fixture.path, .{ .externally_locked = true, .no_sync = true });
    defer peer.close();
    try peer.inheritAllocatorVerification(file);
    peer.allocator_certificate_on_commit = true;
    for (0..4) |i| {
        try peer.beginArtifactLeaseTransaction();
        errdefer peer.abortTransaction();
        var key_buffer: [64]u8 = undefined;
        const marker = try std.fmt.bufPrint(&key_buffer, "{s}generation-{d}", .{ native.NativeFile.artifact_lease_key_prefix, i });
        try peer.putCatalogRecord(marker, "lease");
        try peer.commitTransaction();
    }
    try std.testing.expect(!std.meta.eql(file.header, peer.header));
    try std.testing.expectEqual(root, try owner.refresh());
    try std.testing.expectEqualDeep(peer.header, file.header);
}

test "capi native recovery agenda bounds a turn and acknowledges dropped namespaces" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("recovery-agenda-page", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const root = fixture.connection.owner.current.?;
    for (0..8) |_| try @import("sql_ddl.zig").recover(root);
    try std.testing.expect(!root.sql_ddl_recovery_needed);
    const file = &root.owned_lite_backend.?.native_docstore.?.file;
    try file.beginTransaction();
    errdefer file.abortTransaction();
    for (100..109) |id| {
        var buffer: [128]u8 = undefined;
        const marker = try std.fmt.bufPrint(&buffer, "{s}{s}{d}\x00", .{
            @import("../storage/lite/recovery_agenda.zig").prefix,
            @import("../storage/lite/recovery_agenda.zig").table_prefix,
            id,
        });
        try file.putCatalogRecord(marker, "");
    }
    try file.commitTransaction();
    try @import("sql_ddl.zig").recover(root);
    try std.testing.expect(root.sql_ddl_recovery_needed);
    try std.testing.expect(root.sql_ddl_agenda_after_len != 0);
    try @import("sql_ddl.zig").recover(root);
    try std.testing.expect(!root.sql_ddl_recovery_needed);
    try std.testing.expectEqual(@as(u8, 0), root.sql_ddl_agenda_after_len);
}

test "capi native logical generations preserve adaptive retirement budgets across snapshot replacement" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("retirement-budget-refresh", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const owner = fixture.connection.owner;
    const root = owner.current.?;
    owner.publication_stats.retirement_work_pages = 256;
    const first = try owner.refreshMaintenanceSnapshot(root, false);
    try std.testing.expectEqual(@as(usize, 256), first.owned_lite_backend.?.native_docstore.?.maintenance_policy.options.retirement_work_pages);
    owner.publication_stats.retirement_work_pages = 512;
    try root.owned_lite_backend.?.native_docstore.?.file.putCatalogRecord("budget-test", "next snapshot");
    const replacement = try owner.refreshMaintenanceSnapshot(root, false);
    const store = replacement.owned_lite_backend.?.native_docstore.?;
    try std.testing.expectEqual(@as(usize, 512), store.file.retirement_work_pages);
    try std.testing.expectEqual(@as(usize, 512), store.maintenance_policy.options.retirement_work_pages);
}

test "capi native recovery agenda readonly sweeps become clean across eight and nine namespaces" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const ddl = @import("sql_ddl.zig");
    for ([_]usize{ 8, 9 }) |count| {
        var fixture = try AdmissionFollowupFixture.init("readonly-agenda-pages", 5000, .batch);
        defer fixture.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
        {
            var lease = try fixture.connection.acquire();
            defer {
                lease.close();
                fixture.connection.gate.mutex.unlock(h.handleLockIo());
            }
            const root = fixture.connection.owner.current.?;
            for (0..count) |i| {
                var name: [32]u8 = undefined;
                try @import("tables.zig").create(root, try std.fmt.bufPrint(&name, "namespace-{d}", .{i}), "{}", false);
            }
        }
        const reader = try open(fixture.path, .{ .profile = .native, .open_mode = .query_readonly, .no_sync = true }, false);
        defer h.closeHandle(reader);
        const connection = reader.lite_connection.?;
        var lease: ?native.PathWriterLock = try connection.acquire();
        var gate_held = true;
        defer if (gate_held) {
            connection.detach();
            if (lease) |*held| held.close();
            connection.gate.mutex.unlock(h.handleLockIo());
        };
        const root = try connection.refresh(&lease, .read_only, false);
        for (0..4) |_| try ddl.recover(root);
        try std.testing.expect(!root.sql_ddl_recovery_needed);
        const scans = root.sql_ddl_recovery_scans;
        try std.testing.expect(scans <= 3);
        for (0..20) |_| try ddl.recover(root);
        try std.testing.expectEqual(scans, root.sql_ddl_recovery_scans);
        const metadata = root.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().logical.metadata;
        // A peer's data-only commit replaces the serving wrapper but cannot
        // erase this clean witness for the same logical metadata generation.
        connection.detach();
        lease.?.close();
        lease = null;
        connection.gate.mutex.unlock(h.handleLockIo());
        gate_held = false;
        {
            var writer_lease = try fixture.connection.acquire();
            defer {
                writer_lease.close();
                fixture.connection.gate.mutex.unlock(h.handleLockIo());
            }
            try fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file.putDocument("foreign-data-only", "{}");
        }
        lease = try connection.acquire();
        gate_held = true;
        const refreshed = try connection.refresh(&lease, .read_only, false);
        try std.testing.expectEqualSlices(u8, &metadata, &refreshed.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().logical.metadata);
        try std.testing.expect(!refreshed.sql_ddl_recovery_needed);
        try @import("sql_ddl.zig").recover(refreshed);
        try std.testing.expectEqual(@as(u64, 0), refreshed.sql_ddl_recovery_scans);
        // Reprojecting an existing marker for new DDL must invalidate the memo.
        connection.detach();
        lease.?.close();
        lease = null;
        connection.gate.mutex.unlock(h.handleLockIo());
        gate_held = false;
        {
            var writer_lease = try fixture.connection.acquire();
            defer {
                writer_lease.close();
                fixture.connection.gate.mutex.unlock(h.handleLockIo());
            }
            try fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file.putDocument(
                "\x02db/embedded/tables/2\x00\x00antfly/embedded/sql-ddl",
                "unfinished",
            );
        }
        lease = try connection.acquire();
        gate_held = true;
        const changed = try connection.refresh(&lease, .read_only, false);
        try @import("sql_ddl.zig").recover(changed);
        try std.testing.expect(changed.sql_ddl_recovery_needed);
        try std.testing.expectError(error.SqlStatementReadUnavailable, @import("tables.zig").get(changed, "namespace-0"));
    }
}

test "capi native recovery agenda fences an affected namespace beyond the current page" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("affected-agenda-page", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const root = fixture.connection.owner.current.?;
        for (0..10) |i| {
            var name: [32]u8 = undefined;
            try @import("tables.zig").create(root, try std.fmt.bufPrint(&name, "namespace-{d}", .{i}), "{}", false);
        }
        // ID 9 sorts after the eight-entry page (10,11,2,3,4,5,6,7).
        const affected = try @import("tables.zig").get(root, "namespace-7");
        var txn = try affected.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put("\x00antfly/embedded/sql-ddl", "unfinished");
        try txn.commit();
    }
    const reader = try open(fixture.path, .{ .profile = .native, .open_mode = .query_readonly, .no_sync = true }, false);
    defer h.closeHandle(reader);
    const connection = reader.lite_connection.?;
    var lease: ?native.PathWriterLock = try connection.acquire();
    defer {
        connection.detach();
        if (lease) |*held| held.close();
        connection.gate.mutex.unlock(h.handleLockIo());
    }
    const root = try connection.refresh(&lease, .read_only, false);
    for (0..20) |_| {
        try @import("sql_ddl.zig").recover(root);
        try std.testing.expectError(error.SqlStatementReadUnavailable, @import("tables.zig").get(root, "namespace-7"));
    }
    try std.testing.expect(root.sql_ddl_recovery_needed);
}

test "capi native recovery agenda root admission cannot bypass an unresolved job" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("root-agenda-admission", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const root = fixture.connection.owner.current.?;
        for (0..9) |i| {
            var name: [32]u8 = undefined;
            try @import("tables.zig").create(root, try std.fmt.bufPrint(&name, "namespace-{d}", .{i}), "{}", false);
        }
        try root.owned_lite_backend.?.native_docstore.?.file.putDocument("\x00antfly/embedded/sql-ddl", "unfinished");
    }
    const reader = try open(fixture.path, .{ .profile = .native, .open_mode = .query_readonly, .no_sync = true }, false);
    const id = try api.publishHandle(reader);
    defer api.antfly_db_close(id);
    const expected = h.capi.mapError(error.SqlStatementReadUnavailable);
    for (0..20) |_| {
        var result: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&result);
        try std.testing.expectEqual(expected, api.antfly_db_stats_json(id, &result));
        try std.testing.expectEqual(@as(usize, 0), result.len);
    }
}

test "capi native recovery agenda interrupted readonly namespace cannot certify a sweep" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("readonly-agenda-interrupted", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    {
        var writer_lease = try fixture.connection.acquire();
        defer {
            writer_lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const root = fixture.connection.owner.current.?;
        try @import("tables.zig").create(root, "pending", "{}", false);
        const table = try @import("tables.zig").get(root, "pending");
        var txn = try table.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put("\x00antfly/embedded/sql-ddl", "unfinished");
        try txn.commit();
    }
    const reader = try open(fixture.path, .{ .profile = .native, .open_mode = .query_readonly, .no_sync = true }, false);
    defer h.closeHandle(reader);
    const connection = reader.lite_connection.?;
    var lease: ?native.PathWriterLock = try connection.acquire();
    defer {
        connection.detach();
        if (lease) |*held| held.close();
        connection.gate.mutex.unlock(h.handleLockIo());
    }
    const root = try connection.refresh(&lease, .read_only, false);
    try @import("tables.zig").loadCatalog(root);
    const original = root.alloc;
    var failing = std.testing.FailingAllocator.init(original, .{ .fail_index = 2 });
    // The page's key and key-array are captured first, then opening the named
    // namespace fails. All successful allocations delegate to the same backing
    // allocator; restore it before any later recovery or handle destruction.
    root.alloc = failing.allocator();
    const result = @import("sql_ddl.zig").recover(root);
    root.alloc = original;
    try std.testing.expectError(error.OutOfMemory, result);
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expect(root.sql_ddl_agenda_after_len != 0);
    for (0..20) |_| {
        try @import("sql_ddl.zig").recover(root);
        try std.testing.expect(root.sql_ddl_recovery_needed);
        try std.testing.expectError(error.SqlStatementReadUnavailable, @import("tables.zig").get(root, "pending"));
    }
}

test "capi native recovery agenda repeated compaction invalidates a clean metadata witness" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("agenda-compaction-lineage", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    {
        var writer_lease = try fixture.connection.acquire();
        defer {
            writer_lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const file = &fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
        var image = try file.prepareVacuum(null);
        defer image.deinit();
        try file.publishVacuum(&image);
    }
    const reader = try open(fixture.path, .{ .profile = .native, .open_mode = .query_readonly, .no_sync = true }, false);
    defer h.closeHandle(reader);
    const connection = reader.lite_connection.?;
    var lease: ?native.PathWriterLock = try connection.acquire();
    var gate_held = true;
    defer if (gate_held) {
        connection.detach();
        if (lease) |*held| held.close();
        connection.gate.mutex.unlock(h.handleLockIo());
    };
    const root = try connection.refresh(&lease, .read_only, false);
    try @import("sql_ddl.zig").recover(root);
    try std.testing.expect(!root.sql_ddl_recovery_needed);
    const clean = root.sql_ddl_recovery_generation;
    connection.detach();
    lease.?.close();
    lease = null;
    connection.gate.mutex.unlock(h.handleLockIo());
    gate_held = false;
    {
        var writer_lease = try fixture.connection.acquire();
        defer {
            writer_lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const file = &fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
        try file.putDocument("\x00antfly/embedded/sql-ddl", "unfinished");
        var image = try file.prepareVacuum(null);
        defer image.deinit();
        try file.publishVacuum(&image);
    }
    lease = try connection.acquire();
    gate_held = true;
    const refreshed = try connection.refresh(&lease, .read_only, false);
    try std.testing.expect(!std.mem.eql(u8, &clean, &refreshed.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().logical.metadata));
    for (0..20) |_| {
        try @import("sql_ddl.zig").recover(refreshed);
        try std.testing.expect(refreshed.sql_ddl_recovery_needed);
        try std.testing.expectError(error.SqlStatementReadUnavailable, @import("sql_ddl.zig").requireRecovered(refreshed, &refreshed.db, "default"));
    }
}

// Regression: a brand-new, independently-opened connection's own stable
// snapshot used to fail `FileBusy` right after a vacuum or compact on a
// *different* (writer) connection to the same path -- nothing to do with
// an open transaction, despite how the bug first surfaced (lite_cgo_test.go
// "snapshot with open transaction"; reproduced with just a write, a vacuum,
// and a fresh reader, no transaction at all). Root cause:
// `NativeFile.replaceWithPreparedGeneration`'s `inode_lock.rebind` only
// lazily invalidates this process's own cached `shared_lock_region` for
// the new inode; the sidecar itself keeps the *previous* generation's
// stamped `db_inode` until something reopens it. A fresh reader's own
// `Region.open` then finds that stale stamp and, by design, refuses to
// rebuild it ("shared lock region refuses a db_inode mismatch instead of
// rebuilding under a live peer") -- forcing the legacy flock fallback,
// which self-conflicts forever with the writer's own parked `WriterLane`
// lease (pid-keyed foreign-waiter fairness never treats a same-process
// reader as foreign). Fixed at the source by `Store.
// reopenRegionAfterGenerationSwap` (eagerly reopens the region the moment
// nothing in this process still holds the retired generation -- right
// after `vacuumWithCancel`'s own `performVacuum` call returns, by which
// point its `image.deinit()` has already run).
test "capi native fresh readonly connection takes a stable snapshot after vacuum" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("vacuum-snapshot-fresh-reader", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));

    var vacuum_out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&vacuum_out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_vacuum_json(fixture.id, &vacuum_out));

    // A brand-new connection, opened fresh *after* the vacuum's generation
    // swap -- never the writer's own handle, which would just reuse its
    // already-current region.
    const reader = try open(fixture.path, .{ .profile = .native, .open_mode = .query_readonly, .no_sync = true }, false);
    const reader_id = try api.publishHandle(reader);
    defer api.antfly_db_close(reader_id);

    const snapshot_path = try std.fmt.allocPrintSentinel(std.testing.allocator, "{s}-snapshot.aflite", .{fixture.path}, 0);
    defer std.testing.allocator.free(snapshot_path);
    var snapshot_out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&snapshot_out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_copy_stable_snapshot_json(reader_id, snapshot_path, false, &snapshot_out));
}

// Regression (lite_cgo_test.go "snapshot with open transaction", flaky
// `ANTFLY_BUSY: ... FileBusy`): a zero-`busy_timeout_ms` snapshot copy
// (`antfly_lite_copy_stable_snapshot_file_json` -> `antfly_lite_open_readonly`,
// exactly what the Go SDK's `CopyStableSnapshotFile` uses, with no way to
// configure a nonzero timeout) used to fail outright while a *different*,
// writer-mode connection to the same path had an open (begun, written,
// never committed or rolled back) transaction. The preceding test's own
// doc comment traces the same underlying chain (a stale `db_inode` stamp
// on the region sidecar after a generation swap forces the legacy flock
// fallback, which self-conflicts with this process's own parked
// `WriterLane` lease) and records that `Store.reopenRegionAfterGenerationSwap`
// closes it "the moment nothing in this process still holds the retired
// generation" -- but an open transaction's own root reference is exactly
// such a holder, so that eager reopen never fires here, and the stale
// stamp (now compounded by a *second* generation swap: this test runs
// both `antfly_lite_compact_json` and `antfly_lite_vacuum_json`, as the Go
// test does) survives long enough for a zero-timeout reader to collide
// with it. Fixed by `PathGate.pinLocalWriterLeaseForRead`: a reader's
// legacy fallback that collides with a kernel lock this process's own
// writer lane holds (checked out or merely parked between writes)
// resolves at once instead of ever treating that self-collision as
// busy -- and pins the lease so it cannot move while the read is still
// in flight (`WriterLane.local_reader_activity`'s own doc comment) --
// see `acquirePathLeaseAssumeLocked`'s reader branch where it is
// consulted.
test "capi native readonly snapshot copy with zero timeout ignores a same-process open transaction" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("open-txn-snapshot-fresh-reader", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));

    var compact_out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&compact_out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_compact_json(fixture.id, &compact_out));

    var vacuum_out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&vacuum_out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_vacuum_json(fixture.id, &vacuum_out));

    // Begin and write a transaction on the writer connection, and leave it
    // open (never committed, never rolled back) -- exactly the Go test's
    // own setup, which is what pins the retired generation and keeps
    // `reopenRegionAfterGenerationSwap`'s eager reopen from ever running.
    const open_txn_id: [16]u8 = .{ 0x6f, 0x70, 0x65, 0x6e, 0x2d, 0x74, 0x78, 0x6e, 0, 0, 0, 0, 0, 0, 0, 1 };
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_begin_transaction_with_id(fixture.id, &open_txn_id, 0, null, 0));
    const pending_write = [_]h.capi.WriteIntent{.{ .key = .fromSlice("doc:pending"), .value = .fromSlice("{\"text\":\"pending open-transaction write\"}"), .is_delete = false }};
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_write_transaction(fixture.id, &open_txn_id, &pending_write, pending_write.len, null, 0));

    // Mirrors the Go SDK's `CopyStableSnapshotFile`: opens its own
    // zero-`busy_timeout_ms` readonly handle on `fixture.path` internally,
    // then takes the snapshot through it -- never the writer's own handle.
    const snapshot_path = try std.fmt.allocPrintSentinel(std.testing.allocator, "{s}-open-txn-snapshot.aflite", .{fixture.path}, 0);
    defer std.testing.allocator.free(snapshot_path);
    const path_c = try std.fmt.allocPrintSentinel(std.testing.allocator, "{s}", .{fixture.path}, 0);
    defer std.testing.allocator.free(path_c);
    var snapshot_out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&snapshot_out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_copy_stable_snapshot_file_json(path_c, snapshot_path, false, &snapshot_out));
}

// Safety follow-up to the preceding test: the synthetic, kernel-lock-free
// lease `Connection.acquirePathLeaseAssumeLocked`'s reader branch hands
// out on a same-process self-collision carries no kernel lock of its
// own -- it is safe only because `WriterLane.pinForLocalRead` keeps the
// lane's *real* kernel lease pinned for as long as that synthetic lease
// stays open. A point-in-time check alone would not be enough:
// `maintainParkedLease`'s own idle release, or `beginMaintenanceCheckout`'s
// foreign-overdue handling, could give the kernel lease to a foreign
// writer while the synthetic read is still in flight, letting that
// writer reuse pages the read is still looking at. This holds a pin
// exactly as that reader branch does, then asks the lane to release for
// both reasons `local_reader_activity`'s own doc comment names (idle,
// and a foreign waiter overdue -- simulated here via `foreign_release_due`
// directly, since provoking a genuinely foreign process's own overdue
// wait is not practical from a single-process unit test) and asserts
// the parked lease stays open through both. Releasing the pin afterward
// confirms the deferred release is not simply swallowed -- it goes
// through on the very next call once nothing still needs the lease held.
test "capi native local reader pin keeps the writer lane's parked lease held through idle and foreign release" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var tmp = try api.TestDirectory.init("local-reader-pin-holds-lease");
    defer tmp.cleanup();
    const io = h.handleLockIo();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/database.aflite", .{tmp.path()});
    defer std.testing.allocator.free(path);

    const wrapper = try open(path, .{ .profile = .native, .no_sync = true, .busy_timeout_ms = 1000 }, true);
    const id = try api.publishHandle(wrapper);
    defer api.antfly_db_close(id);
    const connection = wrapper.lite_connection.?;
    const owner = connection.owner;
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    if (owner.worker) |worker| worker.join();
    owner.worker = null;
    owner.stopping.store(false, .release);

    const writes = [_]h.capi.WriteIntent{.{ .key = .fromSlice("doc:seed"), .value = .fromSlice("{\"text\":\"seed\"}"), .is_delete = false }};
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch(id, &writes, writes.len, null, 0, 0, 0));
    // The write's own checkin parks the lane's lease (lite-writer-authority.md
    // W2) rather than releasing it.
    try std.testing.expect(owner.writer_lane.parked != null);

    try std.testing.expect(owner.writer_lane.pinForLocalRead());

    // Idle release: make the parked lease look long overdue for
    // `maintainParkedLease`'s own idle bound. With the pin held, it must
    // defer (returning a short retry) instead of closing the lease.
    owner.writer_lane.parked_since_ns = std.Io.Clock.awake.now(io).nanoseconds - WriterLane.parked_idle_release_ns - std.time.ns_per_s;
    const idle_retry = owner.writer_lane.maintainParkedLease();
    try std.testing.expect(idle_retry != null);
    try std.testing.expect(owner.writer_lane.parked != null);

    // Foreign-waiter-overdue release: `beginMaintenanceCheckout` acts on
    // `foreign_release_due` directly (set here exactly as a real overdue
    // foreign waiter's `checkin`/`returnLease` would). With the pin
    // held, it must hand the *same* lease to a local borrower (`.parked`,
    // never `.overdue`, which would close it) rather than releasing it.
    owner.writer_lane.foreign_release_due = true;
    switch (owner.writer_lane.beginMaintenanceCheckout()) {
        .parked => |lease| {
            // Hand it straight back, as a real local borrower eventually
            // would once its own turn finishes.
            var returned = lease;
            owner.writer_lane.lendLease(&returned);
            returned.close();
        },
        else => try std.testing.expect(false),
    }
    try std.testing.expect(owner.writer_lane.foreign_release_due);
    try std.testing.expect(owner.writer_lane.parked != null);

    // Releasing the pin lets the still-pending idle release through on
    // the very next call: the defer was never a permanent swallow.
    owner.writer_lane.unpinLocalRead();
    owner.writer_lane.parked_since_ns = std.Io.Clock.awake.now(io).nanoseconds - WriterLane.parked_idle_release_ns - std.time.ns_per_s;
    try std.testing.expect(owner.writer_lane.maintainParkedLease() == null);
    try std.testing.expect(owner.writer_lane.parked == null);
}

// Regression: `WriterLane.checkout`'s cold path marks the lane checked out
// (`checked_out = true`) *before* calling `connection.acquire()` to
// actually establish the lease -- so that same call's own
// `acquirePathLeaseAssumeLocked` -> `waitParkedForStore` used to see its
// *own* checkout as "someone else is checked out" and wait on a flag only
// its own return could ever clear, burning its entire wait budget on every
// single cold checkout (observed live: `TestSQLConformance` hanging on a
// `DROP INDEX` right after a run of `CREATE INDEX` statements, the
// admission retry loop re-entering this exact self-wait forever). Fixed by
// `Connection.acquireForOwnCheckout`: the cold path uses it instead of
// plain `acquire`, skipping `WriterLane.waitParkedForStore` entirely for
// this one call (it already knows `parked` is `null` and owns
// `checked_out` itself). `busy_timeout_ms` is deliberately large here so a
// regression would show up as this test hanging for the lane's own floor,
// not failing fast -- the elapsed-time assertion is the real check.
test "capi native cold lane checkout never waits on its own checkout" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var tmp = try api.TestDirectory.init("cold-checkout-self-wait");
    defer tmp.cleanup();
    const io = h.handleLockIo();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/database.aflite", .{tmp.path()});
    defer std.testing.allocator.free(path);

    const handle = try open(path, .{ .profile = .native, .no_sync = true, .busy_timeout_ms = 60_000 }, true);
    defer h.closeHandle(handle);
    const connection = handle.lite_connection.?;
    const owner = connection.owner;
    // Stop the background maintenance worker: this test isolates the
    // cold-path self-wait, not legitimate maintenance contention.
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    if (owner.worker) |worker| worker.join();
    owner.worker = null;
    owner.stopping.store(false, .release);

    const started = std.Io.Clock.awake.now(io).nanoseconds;
    const root = try owner.writer_lane.checkout(connection, .mutation);
    const elapsed_ns = std.Io.Clock.awake.now(io).nanoseconds - started;
    owner.writer_lane.checkin(connection, root.owned_lite_backend.?.native_docstore.?);
    try std.testing.expect(elapsed_ns < 100 * std.time.ns_per_ms);
}

// Cross-process writer starvation (perf/lwa-park-release): `WriterLane.
// checkin` parks the kernel-level path lease instead of releasing it, so a
// same-Store writer can reclaim it for free. Before this fix, nothing ever
// released a parked lease again except the lane's own next `checkin` or
// `Owner.deinit`: a process that wrote once and then went idle held the
// cross-process writer lease forever, and every other process's writer
// failed `FileBusy` once its own `busy_timeout_ms` ran out (observed live:
// Python `tests/test_concurrency.py::test_cross_process_writers_do_not_lose_each_others_rows`).
// Fixed by `WriterLane.maintainParkedLease`, driven every tick of
// `Owner.maintenanceLoop` (woken promptly by `checkinImpl`'s own park --
// see `wakeOwnerForParkedLease` -- rather than waiting for a durable
// consumer's already-scheduled poll, which may never come at all). This
// test leaves the maintenance worker running (unlike the cold-checkout
// test above): it is what has to actually perform the release.
test "capi native parked writer lease releases itself after the idle window with nobody checking out" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var tmp = try api.TestDirectory.init("parked-lease-idle-release");
    defer tmp.cleanup();
    const io = h.handleLockIo();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/database.aflite", .{tmp.path()});
    defer std.testing.allocator.free(path);

    const handle = try open(path, .{ .profile = .native, .no_sync = true, .busy_timeout_ms = 10_000 }, true);
    defer h.closeHandle(handle);
    const connection = handle.lite_connection.?;
    const owner = connection.owner;

    const root = try owner.writer_lane.checkout(connection, .mutation);
    owner.writer_lane.checkin(connection, root.owned_lite_backend.?.native_docstore.?);
    // Parked, exactly like the old (buggy) behavior: nobody has reclaimed
    // it yet, and nothing else is checking out.
    try std.testing.expect(owner.writer_lane.parked != null);

    const started = std.Io.Clock.awake.now(io).nanoseconds;
    const deadline = started + 3 * std.time.ns_per_s;
    while (owner.writer_lane.parked != null) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline) break;
        io.sleep(.fromMilliseconds(20), .awake) catch {};
    }
    const elapsed_ns = std.Io.Clock.awake.now(io).nanoseconds - started;
    try std.testing.expect(owner.writer_lane.parked == null);
    // Bounded well above `WriterLane.parked_idle_release_ns` (1s) to
    // absorb scheduling jitter, but far below the 3s deadline above: a
    // regression back to "never releases" would time out instead of
    // passing, not just run slow.
    try std.testing.expect(elapsed_ns < 2 * std.time.ns_per_s);
}

// Same bug as the idle-release test above, the other half of the bound:
// `checkin`'s own foreign-waiter release only ever runs on that lane's
// *next* `checkin` call, which an idle holder never makes again. A
// genuinely foreign waiter (a different process, or -- as simulated here,
// matching `shared_lock_region.zig`'s own "othersWaitingAcrossProcesses
// distinguishes a foreign ticket from this process's own" test -- a
// different pid's ticket on the same region) must still be served within
// `WriterLane.foreign_fairness_quantum_ns` of queuing, not only once the
// much longer idle window (`parked_idle_release_ns`) elapses.
test "capi native parked writer lease releases for a foreign waiter within the fairness quantum" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var tmp = try api.TestDirectory.init("parked-lease-foreign-waiter");
    defer tmp.cleanup();
    const io = h.handleLockIo();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/database.aflite", .{tmp.path()});
    defer std.testing.allocator.free(path);

    const handle = try open(path, .{ .profile = .native, .no_sync = true, .busy_timeout_ms = 10_000 }, true);
    defer h.closeHandle(handle);
    const connection = handle.lite_connection.?;
    const owner = connection.owner;

    const root = try owner.writer_lane.checkout(connection, .mutation);
    owner.writer_lane.checkin(connection, root.owned_lite_backend.?.native_docstore.?);
    try std.testing.expect(owner.writer_lane.parked != null);
    _ = owner.writer_lane.parked.?.region orelse return error.SkipZigTest;

    const own_pid = antfly_platform.process.currentId() orelse 0;
    const foreign_pid = own_pid +% 0x5a5a_5a5a;

    // A genuinely foreign process would hold its own independent mapping
    // of the same shared region file, not this process's own `*Region`:
    // `maintainParkedLease`'s release races to `PathWriterLock.close()`
    // the holder's mapping (unmap *and* free the heap `Region`) the
    // moment it is served, which would otherwise pull this simulated
    // waiter's still-running `acquireWriterFairBounded` out from under
    // it. Open a second mapping instead, exactly like a second process
    // opening the same path would.
    var foreign_region = try shared_lock_region.Region.open(std.testing.allocator, io, path);
    defer foreign_region.close();

    const Foreign = struct {
        region: *shared_lock_region.Region,
        pid: u32,
        acquired: bool = false,
        fn run(self: *@This()) void {
            const wio = h.handleLockIo();
            const deadline = std.Io.Clock.now(.awake, wio).addDuration(.fromSeconds(2));
            // `acquireWriterFairBounded`'s own return value says whether
            // *this particular* acquisition was contended (it took a real
            // ticket) versus winning the uncontended ticketless fast path
            // -- not whether it succeeded at all, which is exactly
            // "didn't error" (see its own doc comment). Discarding the
            // bool here and keying `acquired` off the absence of an error
            // is deliberate: both outcomes mean the foreign waiter got
            // served, and which one happens is itself a race between this
            // task's own scheduling delay and `maintainParkedLease`'s
            // release (see this test's own comment below for why that
            // race is exactly what made the bug below only ever surface
            // under `io.concurrent`, never under `std.Thread.spawn`).
            _ = self.region.acquireWriterFairBounded(wio, self.pid, deadline, null) catch return;
            self.acquired = true;
            self.region.releaseWriter();
        }
    };
    // `started` precedes spawning the foreign waiter: by the time it
    // calls `acquireWriterFairBounded`, test setup overhead (opening a
    // second `Region` mapping, task spawn) may already exceed
    // `foreign_fairness_quantum_ns` on its own, so `maintainParkedLease`
    // can release the instant the foreign ticket is claimed -- the
    // claim-then-release round trip can finish faster than any poll
    // interval could reliably observe the intermediate "queued" state.
    // The bound below is what actually matters: the lease is released
    // and the waiter served promptly, not on the much longer idle window.
    //
    // This test was briefly flaky (~1/3 of runs) when first converted from
    // `std.Thread.spawn` to `io.concurrent`, with `foreign.acquired` false
    // despite `acquireWriterFairBounded` never actually erroring. Root
    // cause: the bug above was already latent under `std.Thread.spawn` too
    // -- `self.acquired = ... catch false` conflated "did this error" with
    // "did this take the contended ticket path" -- but real OS thread
    // creation happened to always take longer than `maintainParkedLease`'s
    // own release, so the foreign call always lost the race to
    // `tryAcquireWriter`'s own uncontended CAS and took a ticket (`true`),
    // masking the bug. `io.concurrent`'s task dispatch has no such
    // consistent floor: sometimes the foreign task's first
    // `tryAcquireWriter` attempt runs *after* the parked lease is already
    // released, winning the fast, uncontended path (a legitimate, even
    // faster, success that the old assertion wrongly treated as a
    // failure).
    const started = std.Io.Clock.awake.now(io).nanoseconds;
    var foreign: Foreign = .{ .region = &foreign_region, .pid = foreign_pid };
    var foreign_task = try std.testing.io.concurrent(Foreign.run, .{&foreign});

    const deadline_ns = started + 3 * std.time.ns_per_s;
    while (owner.writer_lane.parked != null) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline_ns) break;
        io.sleep(.fromMilliseconds(5), .awake) catch {};
    }
    try std.testing.expect(owner.writer_lane.parked == null);
    foreign_task.await(std.testing.io);
    try std.testing.expect(foreign.acquired);
    const elapsed_ns = std.Io.Clock.awake.now(io).nanoseconds - started;
    // Served well within the fairness quantum plus this loop's own poll
    // granularity, not the much longer idle window -- a regression that
    // only released on the idle timer would still pass the idle-release
    // test above but blow well past this bound.
    try std.testing.expect(elapsed_ns < 500 * std.time.ns_per_ms);
}

// docs/design/lite-performance/throughput-profile.md §6: a sustained
// single-writer `sync_level: full_index` run eventually acknowledges a
// write that is never searchable again, even after close + reopen +
// run_until_idle(), with no crash involved. Root cause: `Owner.deinit()`
// (the real shutdown path behind `antfly_db_close` for the last Connection
// on an Owner -- the path every real SDK/capi caller, including the
// scripts/bench/lite_sqlite_comparison.py repro, goes through) marked its
// own still-current, still-authoritative generation `lease_snapshot = true`
// before closing it. That flag means "a NEWER generation already committed
// over this one, so discard whatever this one has pending" (see the
// `previous.lease_snapshot = true` call sites earlier in this file, where
// that is true). There is no newer generation at ordinary shutdown -- this
// generation IS the durable truth -- so `closeHandle` routed it through
// `DB.closeImmutableSnapshot()` (`discard_storage_writes = true`,
// `.query_readonly`), skipping the owner's own idle-flush/close durability
// barrier. Anything still riding along on the shared native file at that
// moment -- a background text-segment merge's catalog commit
// (`persistent.zig`'s `replaceSegmentsWithReservedIdOwned`,
// `txn.setDurable(false)`), a WS5 retirement turn (`DocStore.serviceRetirement`
// -> `commitRideAlong`), or a deferred progress-v2 record
// (`apply_state.contributeProgressDeferred`) -- never got published and was
// silently discarded, not rolled back to a consistent prior state: the
// write had already returned success to its caller. See
// `storage/lite/crash_consistency_test.zig`'s "a ride-along commit needs an
// explicit durability barrier before close, or it is lost" for a
// deterministic, native-file-level proof of exactly this mechanism (one
// `commitTransactionWithDurability(false)` that disappears on a plain
// close + reopen unless something -- a later durable commit, or an
// explicit `NativeFile.sync()` -- publishes it first), and the fix above
// in `Owner.deinit()`, which supplies that durability barrier
// (`Store.syncIndexContents()`, which reaches `NativeFile.sync()`) for the
// owner's own final generation instead of skipping it.

test "capi native writer authority W6 plain reads take no writer lease and run while another writer holds it" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("writer-authority-w6-reads");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    {
        var seed = try native.NativeFile.createWithIo(a, io, path, .{ .no_sync = true });
        seed.close();
    }
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, false);
    const id = try api.publishHandle(wrapper);
    defer api.antfly_db_close(id);
    const owner = wrapper.lite_connection.?.owner;
    const writes = [_]h.capi.WriteIntent{.{ .key = .fromSlice("doc"), .value = .fromSlice("{\"text\":\"written\"}"), .is_delete = false }};
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch(id, &writes, writes.len, null, 0, 0, 0));
    // Give up the lease the write parked, as an idle release would.
    {
        const lane = &owner.writer_lane;
        lane.mutex.lockUncancelable(io);
        var parked = lane.parked;
        const store = lane.parked_store;
        lane.parked = null;
        lane.parked_store = null;
        lane.mutex.unlock(io);
        if (parked) |*lease| {
            if (store) |held| held.ensureDurableForReuse();
            lease.close();
        }
    }
    // Another writer holds the cross-process lease; a plain read still runs
    // at once, and leaves this process holding no lease.
    var foreign = try native.lockWriterPathWithIo(a, io, path);
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&out);
    for (0..4) |_| {
        api.antfly_buffer_free_zero(&out);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_lookup_json(id, .fromSlice("doc"), &out));
        try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "written") != null);
    }
    try std.testing.expect(owner.writer_lane.parked == null);
    try std.testing.expect(!owner.writer_lane.checked_out and !owner.writer_lane.reading);
    foreign.close();
    // With the foreign writer gone, nothing in this process holds the lease.
    var probe = try native.lockWriterPathWithIo(a, io, path);
    probe.close();
}
