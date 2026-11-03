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
const alloc = std.heap.c_allocator;
const namespace_work = @import("namespace_work_queue.zig");

const memo_mod = @import("antfly_inference_query_embedding_cache");
const cache_budget = @import("antfly_cache_budget");
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
    metadata_identity: [16]u8,
    sql_commit_fingerprint: ?[32]u8,
    sql_fk_fingerprint: ?[32]u8,
    table_catalog_fingerprint: ?[32]u8,
    recovery_clean: bool,
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
        var result: PublicationCatalog = .{
            .inode = (try (try pin.readFile()).file.stat((try pin.readFile()).runtime())).inode,
            .metadata_identity = pin.checkpoint.logical.metadata,
            .sql_commit_fingerprint = try pin.valueFingerprint(@import("sql_commit.zig").key),
            .sql_fk_fingerprint = try pin.valueFingerprint("\x00antfly/embedded/sql-fk-publication"),
            .table_catalog_fingerprint = try pin.valueFingerprint(@import("../storage/lite/embedded_catalog.zig").key),
            .recovery_clean = false, // Structural capture alone grants no recovery authority.
            .paths = undefined,
            .fingerprints = undefined,
        };
        for (metadata_keys, 0..) |key, i| {
            result.paths[i] = try std.mem.concat(a, u8, &.{ prefix, key });
            result.fingerprints[i] = try pin.valueFingerprint(result.paths[i]);
        }
        return result;
    }
    /// A bounded database-wide sweep may still have unrelated work. Storage
    /// publication needs admission for its affected namespace and no global
    /// SQL decision, rather than a globally empty recovery agenda. Capture the
    /// witness only after the ordinary namespace fence succeeds. The no-ABA
    /// metadata identity and both decision fingerprints fence its later use.
    fn captureForStoragePublication(a: std.mem.Allocator, root: *h.Handle, database: *h.db_mod.DB, name: []const u8) !PublicationCatalog {
        try @import("sql_ddl.zig").requireRecovered(root, database, name);
        var proof = try capture(a, root, database);
        proof.recovery_clean = proof.sql_commit_fingerprint == null and proof.sql_fk_fingerprint == null;
        return proof;
    }
    fn verify(self: *PublicationCatalog, root: *h.Handle) !void {
        var live = try root.owned_lite_backend.?.native_docstore.?.beginRead();
        defer live.abort();
        const file = try live.readFile();
        if ((try file.file.stat(file.runtime())).inode != self.inode) return error.EnrichmentSourceChanged;
        for (self.paths, self.fingerprints) |path, captured|
            if (!std.meta.eql(captured, try live.valueFingerprint(path))) return error.EnrichmentSourceChanged;
    }
    /// The caller holds path admission, so native checkpoint reads cannot race
    /// reuse or replacement. A no-ABA metadata cut plus the SQL decision witness
    /// preserves the recovered catalog without constructing serving runtimes.
    fn verifyNative(self: *PublicationCatalog, file: *native.NativeFile) !void {
        if (!self.recovery_clean or !file.header.indexed_reclamation) return error.EnrichmentWaitCanceled;
        if ((try file.file.stat(file.runtime())).inode != self.inode or
            !std.mem.eql(u8, &self.metadata_identity, &file.activeCheckpoint().logical.metadata)) return error.EnrichmentSourceChanged;
        if (!std.meta.eql(self.sql_commit_fingerprint, try file.documentFingerprintAtCheckpoint(file.activeCheckpoint(), @import("sql_commit.zig").key))) return error.EnrichmentSourceChanged;
        if (!std.meta.eql(self.sql_fk_fingerprint, try file.documentFingerprintAtCheckpoint(file.activeCheckpoint(), "\x00antfly/embedded/sql-fk-publication"))) return error.EnrichmentSourceChanged;
        if (!std.meta.eql(self.table_catalog_fingerprint, try file.documentFingerprintAtCheckpoint(file.activeCheckpoint(), @import("../storage/lite/embedded_catalog.zig").key))) return error.EnrichmentSourceChanged;
        for (self.paths, self.fingerprints) |path, captured|
            if (!std.meta.eql(captured, try file.documentFingerprintAtCheckpoint(file.activeCheckpoint(), path))) return error.EnrichmentSourceChanged;
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
    publication_overhead_ns: ?*std.atomic.Value(u64) = null,
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
    fn validateStorageCurrent(self: *@This(), probe: *native.NativeFile) !void {
        if (self.shadow_scopes.len != 0 or self.resolver_dependency != null) {
            _ = try self.current();
            return;
        }
        if (self.catalog) |proof| if (proof.recovery_clean and probe.header.indexed_reclamation) {
            if (self.snapshot_database.root_incarnation != self.incarnation or
                !std.meta.eql(self.snapshot_database.core.identity_namespace, self.identity)) return error.EnrichmentSourceChanged;
            // Check already-loaded name bindings without opening a runtime.
            // The native proof below independently fences the durable catalog.
            if (self.owner.current) |root| if (!std.mem.eql(u8, self.name, "default")) {
                if (root.embedded_table_records.get(self.name)) |id|
                    if (id != self.identity.table_id) return error.EnrichmentSourceChanged;
            };
            try proof.verifyNative(probe);
            self.owner.publication_stats.storage_guard_validations +|= 1;
            return;
        };
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
        if (self.owner.options.profile == .native and self.snapshot_root.owned_lite_backend.?.native_docstore.?.file.header.indexed_reclamation and
            self.snapshot_database.enrichment_runtime != null and self.shadow_scopes.len == 0 and self.resolver_dependency == null and
            self.snapshot_root.maintenance_owner == self.owner and self.snapshot_root.maintenance_publication == self)
        {
            if (self.catalog) |proof| if (proof.recovery_clean) {
                // Control and output writes share one checked writer. Switching
                // to the serving DB for every output made the next control
                // commit invalidate that runtime and reopen all of its indexes.
                const held = try self.acquireCapturedStore(fence, 0);
                defer held.release(held.ptr);
                errdefer {
                    self.owner.publication_stats.refused +|= 1;
                    self.owner.publication_stats.generated_refused +|= 1;
                }
                const database = self.snapshot_database;
                const file = &self.snapshot_root.owned_lite_backend.?.native_docstore.?.file;
                // Admission rebases physical roots, while the DocStore owns
                // separate committed/reserved sequence caches. Reconcile them
                // under this same writer lease before allocating a sequence.
                {
                    var head_read = try database.core.store.beginReadTxn();
                    defer head_read.abort();
                    const committed = try database.core.store.lastReplaySequenceFromTxn(&head_read, 0);
                    database.core.store.observeExternalReplayCommit(committed);
                }
                const index_revision = file.loadedIndexPathRevision(self.index_prefix);
                const result = database.enqueuePreparedGeneratedBatch(batch, promotions, deletes, guards, fence) catch |err| switch (err) {
                    error.EnrichmentSourceChanged, error.EnrichmentLeaseFenceLost => return error.EnrichmentWaitCanceled,
                    else => return err,
                };
                // Reusing the writer must not erase evidence of its own index
                // mutations. Local path revisions ignore unrelated prefixes,
                // while failed revision capture conservatively fences indexes.
                if (index_revision) |before| {
                    if (file.loadedIndexPathRevision(self.index_prefix)) |after| {
                        if (!std.mem.eql(u8, &before, &after)) self.index_inputs_changed = true;
                    } else self.index_inputs_changed = true;
                } else self.index_inputs_changed = true;
                self.owner.publication_stats.generated_captured_commits +|= 1;
                self.snapshot_database.core.store.observeExternalReplayCommit(result.sequence);
                self.owner.notifyMutation();
                return result;
            };
        }
        const io = h.handleLockIo();
        self.owner.gate.mutex.lockUncancelable(io);
        defer self.owner.gate.mutex.unlock(io);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
        self.owner.publication_stats.attempts +|= 1;
        self.owner.publication_stats.generated_attempts +|= 1;
        errdefer {
            self.owner.publication_stats.refused +|= 1;
            self.owner.publication_stats.generated_refused +|= 1;
        }
        var lease = try self.owner.acquireMaintenancePathLeaseAssumeLocked();
        defer lease.close();
        const database = try self.current();
        // Namespace and catalog are stable under the owner fence. The native
        // writer validates the captured source digests/incarnations and lease
        // inside its live transaction, preserving unrelated primary commits.
        // This transaction runs on the live coordinator, not the detached
        // store marked as a consumer by the maintenance turn. Reserve progress
        // here too, or a full producer quota can fence the consumer which must
        // generate the output before that journal can be retired.
        const native_file = &self.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
        const old_consumer = native_file.replay_consumer_admission;
        native_file.replay_consumer_admission = true;
        defer native_file.replay_consumer_admission = old_consumer;
        const result = database.publishPreparedGeneratedBatch(batch, promotions, deletes, guards, fence) catch |err| switch (err) {
            error.EnrichmentSourceChanged, error.EnrichmentLeaseFenceLost => return error.EnrichmentWaitCanceled,
            else => return err,
        };
        self.refreshSnapshotCheckpoint();
        self.snapshot_database.core.store.observeExternalReplayCommit(result.sequence);
        self.owner.notifyMutation();
        return result;
    }

    const HeldStore = struct {
        publication: *PreparedPublication,
        path_lease: ?native.PathWriterLock = null,
        native_storage_admission: bool = false,
        database: *h.db_mod.DB,
        refusal_recorded: bool = false,

        fn release(ptr: *anyopaque) void {
            const held: *@This() = @ptrCast(@alignCast(ptr));
            const p = held.publication;
            if (held.native_storage_admission) {
                // This store committed through the captured NativeFile itself.
                // Its header/read generation already reflect the publication,
                // and the held path lease excludes foreign publications. A
                // second file open here merely rereads our own checkpoint.
                Owner.leaveMaintenanceWriter(p.snapshot_root);
            } else {
                // The serving writer is a different wrapper; mirror its
                // publication before releasing the common writer fence.
                p.refreshSnapshotCheckpoint();
                held.path_lease.?.close();
                p.owner.gate.mutex.unlock(h.handleLockIo());
            }
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
        if (self.snapshot_root.owned_lite_backend.?.native_docstore.?.index_reads_require_admission == true) {
            // Once the primary epoch ends, the old catalog pages may already
            // have been reused. Compare durable no-ABA identities without
            // dereferencing the unretained captured catalog.
            return captured.header.indexed_reclamation and latest.header.indexed_reclamation and
                std.mem.eql(u8, &captured.activeCheckpoint().logical.indexes, &latest.activeCheckpoint().logical.indexes);
        }
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
                if (snapshot.index_reads_require_admission or snapshot.file.activeCheckpoint().index_catalog_root_page != file.activeCheckpoint().index_catalog_root_page) {
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
        const native_file = &self.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
        const old_consumer = native_file.replay_consumer_admission;
        native_file.replay_consumer_admission = true;
        defer native_file.replay_consumer_admission = old_consumer;
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

    /// Provider control records need the current physical checkpoint, not a
    /// newly opened DB/index/provider runtime. Borrow the already pinned raw
    /// store under ordinary checked writer admission. Prepared output uses the
    /// same proof when its dependencies fit this captured namespace.
    fn acquireCapturedStore(self: *@This(), fence: ?WriteFence, now_ms: u64) !StoreLease {
        try Owner.enterMaintenanceWriter(self.snapshot_root, .primary);
        errdefer Owner.leaveMaintenanceWriter(self.snapshot_root);
        self.owner.publication_stats.generated_attempts +|= 1;
        errdefer {
            self.owner.publication_stats.refused +|= 1;
            self.owner.publication_stats.generated_refused +|= 1;
        }
        const database = self.snapshot_database;
        try self.validateStorageCurrent(&self.snapshot_root.owned_lite_backend.?.native_docstore.?.file);
        if (fence) |expected| {
            var ownership = try ProducerLease.init(alloc, database.core.store, expected.lease_key);
            defer ownership.deinit();
            const live = (try ownership.load(alloc)) orelse return error.EnrichmentLeaseFenceLost;
            defer alloc.free(live.owner_id);
            if (live.epoch != expected.epoch or live.expires_at_ms <= @max(now_ms, database.backend_runtime.clock().nowRealtimeMs()) or !std.mem.eql(u8, live.owner_id, expected.owner_id)) return error.EnrichmentLeaseFenceLost;
        }
        const held = try alloc.create(HeldStore);
        held.* = .{ .publication = self, .database = database, .native_storage_admission = true };
        self.owner.publication_stats.generated_storage_groups +|= 1;
        return .{ .store = database.enrichment_runtime.?.store, .ptr = held, .release = HeldStore.release, .guard_key = HeldStore.guardKey };
    }

    fn acquireStore(ptr: *anyopaque, fence: ?WriteFence, now_ms: u64) !StoreLease {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.owner.options.profile == .native and self.snapshot_root.owned_lite_backend.?.native_docstore.?.file.header.indexed_reclamation and
            self.snapshot_database.enrichment_runtime != null and self.shadow_scopes.len == 0 and self.resolver_dependency == null)
        {
            if (self.catalog) |proof| if (proof.recovery_clean)
                return self.acquireCapturedStore(fence, now_ms);
        }
        const io = h.handleLockIo();
        self.owner.gate.mutex.lockUncancelable(io);
        errdefer self.owner.gate.mutex.unlock(io);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
        self.owner.publication_stats.attempts +|= 1;
        self.owner.publication_stats.generated_attempts +|= 1;
        errdefer {
            self.owner.publication_stats.refused +|= 1;
            self.owner.publication_stats.generated_refused +|= 1;
        }
        var lease = try self.owner.acquireMaintenancePathLeaseAssumeLocked();
        errdefer lease.close();
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
        held.* = .{ .publication = self, .path_lease = lease, .database = database };
        return .{ .store = database.enrichment_runtime.?.store, .ptr = held, .release = HeldStore.release, .guard_key = HeldStore.guardKey };
    }

    fn heartbeat(ptr: *anyopaque, fence: WriteFence, now_ms: u64, ttl_ms: u64) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const io = h.handleLockIo();
        self.owner.gate.mutex.lockUncancelable(io);
        defer self.owner.gate.mutex.unlock(io);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
        var path_lease = try self.owner.acquireMaintenancePathLeaseAssumeLocked();
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

    pub fn lockUncancelable(self: *QueuedMutex, io: std.Io) void {
        self.state.lockUncancelable(io);
        defer self.state.unlock(io);
        const ticket = self.next;
        self.next +%= 1;
        while (ticket != self.serving) self.ready.waitUncancelable(io, &self.state);
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

// Connections in one process queue on a canonical file gate. The kernel
// sidecar remains the authority between processes and with CLI/server owners.
const PathGate = struct {
    path: []u8,
    mutex: QueuedMutex = .{},
    sql_preparation: @import("sql.zig").PreparationAdmission = .{},
    references: usize = 1,
    owners: std.ArrayList(*Owner) = .empty,
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
pub const Owner = struct {
    // Only bootstrap small files at close. Large files must finish their
    // existing worker proof; shutdown never starts another whole-file walk.
    const close_allocator_proof_max_bytes = 4 * 1024 * 1024;
    const MaintenanceKey = namespace_work.Key;
    const MaintenanceFailurePhase = enum { capture, source, retirement, artifacts, storage };
    const MaintenanceReadiness = [PublicationCatalog.metadata_keys.len + 1]?[32]u8;
    const MaintenanceSchedule = struct {
        name: ?[]u8 = null,
        readiness: ?MaintenanceReadiness = null,
        has_group_sources: bool = false,
        next_group_source_ns: i96 = 0,
        // A wakeup belongs to the owner; runnable source/artifact work belongs
        // to one namespace. Audit idle WAL/index lifecycle work periodically,
        // while durable replay/projection and TTL checks still run each visit.
        database_pending: bool = false,
        artifact_pending: bool = false,
        next_artifact_ns: i96 = 0,
        serviced_sequence: u64 = 0,
        next_database_audit_ns: i96 = 0,
        shadow_next: bool = true,
        startup_replay_pending: bool = true,
        replay_turns_since_artifacts: u8 = 0,
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
    connections: std.ArrayList(*Connection) = .empty,
    worker: ?std.Thread = null,
    // The path gate permits one foreground lease wait per owner. The global
    // debug Io cannot spawn tasks; this lazy pool supplies exactly the lock
    // waiter and its deadline timer, and is shared by compatible connections.
    path_lock_io: std.Io.Threaded,
    test_maintenance_wait_ns: if (h.builtin.is_test) ?i96 else void = if (h.builtin.is_test) null else {},
    stopping: std.atomic.Value(bool) = .init(false),
    // One graph walk per owner. Admission waits without the foreground gate;
    // the worker only try-locks under that gate and yields to an active proof.
    allocator_proof_mutex: std.Io.Mutex = .init,
    verification_cancel: @FieldType(NativeStore, "maintenance_cancel") = .{},
    allocator_certificate_pending: bool = false,
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
    maintenance_last_storage: bool = false,
    runnable_sources: namespace_work.Queue = .{},
    // Owner lifetime, rather than DB-runtime lifetime: snapshot refreshes must
    // not reset a table's replay/construction alternation.
    maintenance_schedules: std.AutoHashMapUnmanaged(MaintenanceKey, MaintenanceSchedule) = .empty,
    maintenance_root_incarnation: ?u128 = null,
    wake: std.Io.Event = .unset,
    current: ?*h.Handle = null,
    retired: std.ArrayList(*h.Handle) = .empty,
    maintenance_snapshot: ?*h.Handle = null,
    // Only immutable index/cache state survives between bounded artifact slices.
    // No primary read epoch or borrowed catalog proof is retained here.
    maintenance_retained_logical: ?native.LogicalGenerations = null,
    maintenance_table_cursor: usize = 0,
    maintenance_round_pending: bool = false,
    maintenance_round_ttl_pending: bool = false,
    maintenance_capture_misses: u8 = 0,
    // Connection-owner lifetime totals, protected by the path gate. They survive
    // foreground runtime refreshes and distinguish conflicts from a moving tip.
    publication_stats: struct {
        /// Namespace rounds reaching replay/idle completion (errors below).
        /// Lets observers distinguish quiescence from a worker that stopped.
        maintenance_rounds: u64 = 0,
        maintenance_database_turns: u64 = 0,
        maintenance_source_turns: u64 = 0,
        maintenance_priority_turns: u64 = 0,
        maintenance_audit_turns: u64 = 0,
        maintenance_progress_requeues: u64 = 0,
        maintenance_artifact_only_turns: u64 = 0,
        maintenance_idle_namespace_turns: u64 = 0,
        maintenance_readiness_skips: u64 = 0,
        maintenance_capture_ns: u64 = 0,
        maintenance_proof_ns: u64 = 0,
        maintenance_replay_ns: u64 = 0,
        maintenance_artifacts_ns: u64 = 0,
        maintenance_storage_ns: u64 = 0,
        retirement_work_pages: usize = 128,
        replay_budget_table_id: u64 = 0,
        replay_budget: @import("../storage/db/derived/replay_budget.zig").Snapshot = .{},
        producer_replay_budget: @import("../storage/db/derived/replay_budget.zig").ProducerSnapshot = .{},
        attempts: u64 = 0,
        refused: u64 = 0,
        rebased: u64 = 0,
        generated_attempts: u64 = 0,
        generated_storage_groups: u64 = 0,
        generated_captured_commits: u64 = 0,
        generated_refused: u64 = 0,
        storage_guard_validations: u64 = 0,
        serving_runtime_refreshes: u64 = 0,
        replay_backlog: ?native.replay_admission.State = null,
        replay_backpressure_waits: u64 = 0,
        replay_refused_preparation_ns: u64 = 0,
        replay_index_groups: u64 = 0,
        publication_hold_ns: u64 = 0,
        replay_metadata_groups: u64 = 0,
        replay_attempts: u64 = 0,
        replay_refused: u64 = 0,
        maintenance_errors: u64 = 0,
        maintenance_failures: struct {
            capture: u64 = 0,
            source: u64 = 0,
            retirement: u64 = 0,
            artifacts: u64 = 0,
            storage: u64 = 0,
        } = .{},
        maintenance_queue_timeouts: u64 = 0,
        allocator_refusals: u64 = 0,
        allocator_service_admissions: u64 = 0,
        index_input_refusals: u64 = 0,
        // Detached graph-walk totals, including foreground and maintenance.
        // These survive runtime refresh and exclude lease/queue wait time.
        allocator_proof_walks: u64 = 0,
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
        last_maintenance_error: ?[]const u8 = null,
        // Successful work in another namespace clears the current error, but
        // must not erase the evidence behind the cumulative failure count.
        last_maintenance_failure: ?[]const u8 = null,
        last_maintenance_failure_table_id: u64 = 0,
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
    test_before_replay_index_publication: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *h.Handle) void } else void = if (h.builtin.is_test) null else {},
    test_before_maintenance_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},

    /// Caller holds the path lease, so the checkpoint and catalog cannot race
    /// a different process's publication. Cursor continuation deliberately
    /// uses its pinned generation without reopening the connection.
    pub fn refresh(self: *Owner) !*h.Handle {
        // Publication uncertainty is file-wide even when the writer belongs
        // to a detached maintenance capture. No foreground operation may
        // silently continue through a different, apparently clean wrapper.
        if (self.maintenance_snapshot) |snapshot| if (needsOutcomeReconciliation(snapshot)) return error.OutcomeUnknown;
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
            if (current.maintenance_references == 0 and same_inode and std.meta.eql(probe.header, file.header)) {
                if (!file.free_pages_verified) if (self.maintenance_snapshot) |snapshot|
                    try file.inheritAllocatorVerification(&snapshot.owned_lite_backend.?.native_docstore.?.file);
                return current;
            }
            if (current.maintenance_references == 0 and same_inode and !needsOutcomeReconciliation(current) and
                try file.runtimeMetadataUnchanged(&probe) and try loadedIndexInputsUnchanged(current, file, &probe))
            {
                // Lease bookkeeping and unpublished shadow files do not change
                // a serving runtime. Reopening it would publish another lease
                // for every immutable input while foreground traffic waits.
                try current.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(probe.header);
                if (self.maintenance_snapshot) |snapshot| try file.inheritAllocatorVerification(&snapshot.owned_lite_backend.?.native_docstore.?.file);
                if (self.options.profile == .native and self.options.open_mode == .writer and !file.free_pages_verified)
                    _ = try file.inheritAllocatorCertificate();
                return current;
            }
            if (self.maintenance_snapshot) |snapshot| {
                const captured = &snapshot.owned_lite_backend.?.native_docstore.?.file;
                if (std.meta.eql(probe.header, captured.header) and
                    (try probe.file.stat(probe.runtime())).inode == (try captured.file.stat(captured.runtime())).inode) known_maintenance = snapshot;
            }
        }
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
                // detached work and proof readers retain independent caches.
                if (previous.query_borrows == 0 and previous.maintenance_references == 0 and
                    previous.allocator_verification_references == 0 and previous.sql_cursors.count() == 0)
                    _ = try certified.?.takeAllocatorRebaseBase(&previous.owned_lite_backend.?.native_docstore.?.file);
            }
            if (try certified.?.inheritAllocatorCertificate()) {
                options.allocator_verification_owner = &certified.?;
                options.allocator_state_donor = &certified.?;
            } else if (certified.?.header.indexed_reclamation and !try self.unindexedRuntimeOpen(&certified.?)) {
                retained_turn = try self.retainMaintenanceTurnAssumeLocked();
                if (!retained_turn) return error.FileBusy;
            }
        }
        const replacement = try api.openLiteHandleAllocWithSharedInference(alloc, self.path, options, false, h.handleLockIo(), null, self.current);
        self.publication_stats.serving_runtime_refreshes +|= 1;
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
            // namespace's shared native store reject publication immediately.
            if (previous.maintenance_references == 0) {
                previous.owned_lite_backend.?.native_docstore.?.read_only = true;
                previous.owned_lite_backend.?.native_docstore.?.file.read_only = true;
            }
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
                if (retired.query_borrows > 0 or retired.maintenance_references != 0 or retired.allocator_verification_references != 0) break :retained true;
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
        root.db.borrowed_group_source_state = try self.groupSourceStateNamed(&root.db, "default");
        var tables = root.embedded_tables.valueIterator();
        while (tables.next()) |table|
            table.*.db.borrowed_group_source_state = try self.groupSourceStateNamed(&table.*.db, table.*.name);
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
        // Retry a failed reservation before installing a committed observer.
        if (database.core.store.replay_mutation_observer == null) {
            try self.runnable_sources.reserve(alloc, h.handleLockIo(), self.maintenance_schedules.count());
            database.core.store.replay_mutation_observer = .{
                .ptr = self,
                .table_id = database.core.identity_namespace.table_id,
                .incarnation = database.root_incarnation,
                .notify = notifySourceMutation,
            };
        }
        if (entry.value_ptr.group_sources == null) {
            const state = try alloc.create(h.db_mod.DB.GroupSourceMaintenanceState);
            state.* = .{ .allocator = alloc };
            entry.value_ptr.group_sources = state;
        }
        return entry.value_ptr.group_sources.?;
    }

    pub fn groupSourceStateNamed(self: *Owner, database: *h.db_mod.DB, name: []const u8) !*h.db_mod.DB.GroupSourceMaintenanceState {
        const state = try self.groupSourceState(database);
        const schedule = self.maintenance_schedules.getPtr(.{ .table_id = database.core.identity_namespace.table_id, .incarnation = database.root_incarnation }).?;
        if (schedule.name == null) schedule.name = try alloc.dupe(u8, name);
        return state;
    }

    fn notifySourceMutation(ptr: *anyopaque, table_id: u64, incarnation: u128) void {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        if (self.stopping.load(.acquire)) return;
        self.runnable_sources.notify(h.handleLockIo(), .{ .table_id = table_id, .incarnation = incarnation });
        self.wake.set(h.handleLockIo());
    }

    pub fn notifyMutation(self: *Owner) void {
        self.allocator_certificate_pending = true;
        if (self.worker == null) return;
        self.maintenance_pending = true;
        self.wake.set(h.handleLockIo());
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

    /// Caller holds the owner gate. Captures and validated publications need
    /// turns alongside foreground writers, otherwise a continuously occupied
    /// kernel queue prevents index replay from ever acquiring its lease.
    /// Reclamation and detached-proof reacquisition remain nonblocking.
    fn acquireMaintenancePathLeaseAssumeLocked(self: *Owner) !native.PathWriterLock {
        if (self.stopping.load(.acquire)) return error.Canceled;
        const io = h.handleLockIo();
        if (native.lockWriterPathWithIo(alloc, io, self.path)) |held| return self.withReadRegistrationBatch(held) else |err| {
            if (err != error.WouldBlock and err != error.FileBusy) return err;
            // A full foreground queue can exceed one second while each caller
            // remains inside its normal admission budget. Give publications
            // the same default window, while bounding close-time gate waits.
            const default_budget = 5 * std.time.ns_per_s;
            const budget = if (h.builtin.is_test) self.test_maintenance_wait_ns orelse default_budget else default_budget;
            var held = native.waitPathLockWithIo(alloc, self.path_lock_io.io(), self.path, true, std.Io.Clock.awake.now(io).nanoseconds + budget) catch |wait_err| switch (wait_err) {
                error.FileBusy => {
                    self.publication_stats.maintenance_queue_timeouts +|= 1;
                    return wait_err;
                },
                error.FileNotFound, error.ConcurrencyUnavailable => return err,
                else => return wait_err,
            };
            errdefer held.close();
            if (self.stopping.load(.acquire)) return error.Canceled;
            return self.withReadRegistrationBatch(held);
        }
    }

    /// Probe only durable control records, before constructing any index or
    /// provider. This is a scheduling hint; admission still recovers and fences
    /// the selected namespace. Timed audits cover control-only work that does
    /// not append a source record (including unfinished startup recovery).
    fn namespaceReadiness(root: *h.Handle, namespace: []const u8) !MaintenanceReadiness {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const prefix = if (std.mem.eql(u8, namespace, "root")) "" else try root.owned_lite_backend.?.documentPrefixAlloc(a, namespace);
        var read = try root.owned_lite_backend.?.native_docstore.?.beginRead();
        defer read.abort();
        var result: MaintenanceReadiness = undefined;
        for (PublicationCatalog.metadata_keys, 0..) |key, i| {
            result[i] = try read.valueFingerprint(try std.mem.concat(a, u8, &.{ prefix, key }));
        }
        result[PublicationCatalog.metadata_keys.len] = try read.valueFingerprint(try std.mem.concat(a, u8, &.{
            prefix, &@import("../storage/internal_keys.zig").replay_meta_next_sequence_key,
        }));
        return result;
    }

    fn namespaceCanSleep(self: *Owner, root: *h.Handle, namespace: []const u8, table_id: u64) !bool {
        if (self.options.profile != .native) return false;
        const incarnation = (try root.owned_lite_backend.?.existingRootIncarnation(namespace)) orelse return false;
        const schedule = self.maintenance_schedules.get(.{ .table_id = table_id, .incarnation = incarnation }) orelse return false;
        const expected = schedule.readiness orelse return false;
        const now = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds;
        if (schedule.database_pending or schedule.startup_replay_pending or
            now >= schedule.next_database_audit_ns or now >= schedule.next_ttl_ns or
            (schedule.has_group_sources and now >= schedule.next_group_source_ns) or
            (schedule.artifact_pending and now >= schedule.next_artifact_ns)) return false;
        return std.meta.eql(expected, try namespaceReadiness(root, namespace));
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

    const ReplayProgress = struct {
        const Index = struct { name: []const u8, applied: u64 };
        indexes: []Index,
        generated: u64,
        resolution: u64,
        promotion: u64,
        resolver_cursors: ?[3]?[32]u8,

        fn cursors(database: *h.db_mod.DB) ![3]?[32]u8 {
            var read = try database.core.store.beginReadTxn();
            defer read.abort();
            const catalog = @import("../storage/db/catalog/resolver_catalog.zig");
            var result: [3]?[32]u8 = undefined;
            for ([_][]const u8{ catalog.reresolve_resume_key, catalog.reresolve_repair_resume_key, "\x00\x00__metadata__:resolution_replay_progress" }, 0..) |key, i| {
                const raw = read.get(key) catch |err| switch (err) {
                    error.NotFound => {
                        result[i] = null;
                        continue;
                    },
                    else => return err,
                };
                var digest: [32]u8 = undefined;
                std.crypto.hash.Blake3.hash(raw, &digest, .{});
                result[i] = digest;
            }
            return result;
        }

        fn lane(database: *h.db_mod.DB, scope: []const u8) !u64 {
            return h.db_mod.enrichment_state.loadAppliedSequence(alloc, database.core.store, scope);
        }

        fn capture(a: std.mem.Allocator, database: *h.db_mod.DB, managed: anytype) !ReplayProgress {
            const indexes = try a.alloc(Index, managed.len);
            for (managed, indexes) |index, *captured| captured.* = .{ .name = index.name, .applied = database.executor.appliedSequence(index.name) orelse 0 };
            return .{
                .indexes = indexes,
                .generated = if (database.core.hasGeneratedEnrichmentTargets()) try lane(database, "generated") else 0,
                .resolution = if (database.hasConfiguredResolvers()) try lane(database, "resolution") else 0,
                .promotion = if (database.hasConfiguredResolvers()) try lane(database, "promotion") else 0,
                .resolver_cursors = if (database.hasConfiguredResolvers()) try cursors(database) else null,
            };
        }

        fn advanced(self: ReplayProgress, database: *h.db_mod.DB) bool {
            for (self.indexes) |index| if ((database.executor.appliedSequence(index.name) orelse 0) > index.applied) return true;
            if (database.core.hasGeneratedEnrichmentTargets() and (lane(database, "generated") catch 0) > self.generated) return true;
            if (database.hasConfiguredResolvers()) {
                if ((lane(database, "resolution") catch 0) > self.resolution) return true;
                if ((lane(database, "promotion") catch 0) > self.promotion) return true;
                if (self.resolver_cursors) |before| {
                    const after = cursors(database) catch return false;
                    if (!std.meta.eql(before, after)) return true;
                }
            }
            return false;
        }
    };

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
    fn enterReplayIndexPublication(ptr: *anyopaque) !void {
        const root: *h.Handle = @ptrCast(@alignCast(ptr));
        if (comptime h.builtin.is_test) if (root.maintenance_owner.?.test_before_replay_index_publication) |hook| hook.notify(hook.ptr, root);
        try enterMaintenanceWriter(root, .index);
        root.maintenance_owner.?.publication_stats.replay_index_groups +|= 1;
    }

    fn enterReplayMetadataPublication(ptr: *anyopaque) !void {
        const root: *h.Handle = @ptrCast(@alignCast(ptr));
        try enterMaintenanceWriter(root, .primary);
        root.maintenance_owner.?.publication_stats.replay_metadata_groups +|= 1;
    }

    fn recordMaintenanceAdmission(root: *h.Handle, started: i96) void {
        const publication = root.maintenance_publication orelse return;
        const counter = publication.publication_overhead_ns orelse return;
        const elapsed = @max(0, std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds - started);
        _ = counter.fetchAdd(@intCast(@min(elapsed, std.math.maxInt(u64))), .monotonic);
    }

    fn enterMaintenanceWriter(ptr: *anyopaque, scope: PublicationScope) !void {
        const root: *h.Handle = @ptrCast(@alignCast(ptr));
        const self = root.maintenance_owner.?;
        const admission_started = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds;
        var count_admission = true;
        defer if (count_admission) recordMaintenanceAdmission(root, admission_started);
        root.maintenance_admission_mutex.lockUncancelable(h.handleLockIo());
        defer root.maintenance_admission_mutex.unlock(h.handleLockIo());
        if (root.maintenance_writer_references != 0) {
            count_admission = false; // Included in the outer publication's hold.
            root.maintenance_writer_references += 1;
            return;
        }
        if (self.stopping.load(.acquire)) return error.Canceled;
        if (needsOutcomeReconciliation(root)) return error.OutcomeUnknown;
        if (scope != .reclamation) self.gate.mutex.lockUncancelable(h.handleLockIo()) else if (!self.gate.mutex.tryLock()) return error.EnrichmentWaitCanceled;
        errdefer self.gate.mutex.unlock(h.handleLockIo());
        if (self.stopping.load(.acquire)) return error.Canceled;
        self.publication_stats.attempts +|= 1;
        errdefer self.publication_stats.refused +|= 1;
        // A resolver publication can change index inputs before its replay
        // watermark is saved. Fence subsequent immutable index work, while
        // allowing primary transactions to read the rebased checkpoint and
        // persist that completed progress. Otherwise replay repeats its own
        // output forever without advancing its durable watermark.
        if (scope == .index or scope == .index_read) if (root.maintenance_publication) |p| if (p.index_inputs_changed) {
            self.publication_stats.index_input_refusals +|= 1;
            return error.EnrichmentWaitCanceled;
        };
        if (self.current) |current| if (needsOutcomeReconciliation(current)) return error.EnrichmentWaitCanceled;
        var lease = blk: {
            break :blk (if (scope == .reclamation) native.lockWriterPathWithIo(alloc, h.handleLockIo(), self.path) else self.acquireMaintenancePathLeaseAssumeLocked()) catch |err| switch (err) {
                error.FileBusy, error.WouldBlock => return error.EnrichmentWaitCanceled,
                else => return err,
            };
        };
        errdefer lease.close();
        var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
        defer probe.close();
        const file = &root.owned_lite_backend.?.native_docstore.?.file;
        const probe_inode = (try probe.file.stat(probe.runtime())).inode;
        if (probe_inode != (try file.file.stat(file.runtime())).inode) return error.EnrichmentWaitCanceled;
        self.reader_coordinator.observeCurrent(probe_inode, probe.headerFingerprint());
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
            try publication.validateStorageCurrent(&probe);
            // Rebase before opening a primary transaction as well as before
            // publishing immutable index bytes. Lease acquisition and replay
            // checkpoints must not require a quiet interval between capture
            // and publication. Unchanged physical index inputs and catalog
            // identity keep detached computations valid; transactions opened
            // after this admission read the current primary checkpoint.
            var latest = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
            defer latest.close();
            if ((try latest.file.stat(latest.runtime())).inode != (try file.file.stat(file.runtime())).inode) return error.EnrichmentWaitCanceled;
            if (root.owned_lite_backend.?.native_docstore.?.index_reads_require_admission or file.activeCheckpoint().index_catalog_root_page != latest.activeCheckpoint().index_catalog_root_page) {
                if (!try publication.indexInputsUnchanged(file, &latest)) {
                    publication.index_inputs_changed = true;
                    if (scope == .index or scope == .index_read) {
                        self.publication_stats.index_input_refusals +|= 1;
                        return error.EnrichmentWaitCanceled;
                    }
                }
            }
            try root.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(latest.header);
            if (self.current) |current| try file.inheritAllocatorVerification(&current.owned_lite_backend.?.native_docstore.?.file);
            self.publication_stats.rebased +|= 1;
        }
        // A changed tip invalidates a detached proof. Retry outside the gate
        // instead of falling back to a whole-file scan in this publication.
        // Metadata-only validation need not open a serving runtime to donate
        // an allocator. Rebase this writer's checked ledger against the exact
        // admitted tip using the native certificate protocol.
        if (scope != .reclamation and !file.free_pages_verified)
            _ = try file.inheritAllocatorCertificate();
        if ((scope == .primary or scope == .index) and !file.free_pages_verified) {
            self.publication_stats.allocator_refusals +|= 1;
            return error.EnrichmentWaitCanceled;
        }
        if (scope == .allocator_service) self.publication_stats.allocator_service_admissions +|= 1;
        root.maintenance_writer_lease = lease;
        root.maintenance_writer_references = 1;
        root.maintenance_publication_started_ns = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds;
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
        if (root.maintenance_publication_started_ns) |started| {
            const held: u64 = @intCast(@max(0, std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds - started));
            self.publication_stats.publication_hold_ns +|= held;
            if (root.maintenance_publication) |publication| {
                if (publication.publication_overhead_ns) |counter| {
                    _ = counter.fetchAdd(held, .monotonic);
                }
            }
            root.maintenance_publication_started_ns = null;
        }
        // Admission alone is not a mutation. Reclamation probes also take
        // this lease; waking replay here makes each idle turn schedule itself.
        // Generated and replay publications notify at their commit boundary.
        lease.close();
        self.gate.mutex.unlock(h.handleLockIo());
    }

    fn closeMaintenanceSnapshot(root: *h.Handle) void {
        root.lease_snapshot = true;
        const store = root.owned_lite_backend.?.native_docstore.?;
        store.read_only = true;
        store.file.read_only = true;
        h.closeHandle(root);
    }

    fn needsOutcomeReconciliation(root: *h.Handle) bool {
        const store = root.owned_lite_backend.?.native_docstore.?;
        return root.sql_decision_uncertain or store.file.checkpoint_publication_uncertain or store.secret_store_uncertain;
    }

    fn noteMaintenanceFailure(self: *Owner, err: anyerror, table_id: u64, phase: MaintenanceFailurePhase) void {
        self.publication_stats.maintenance_errors +|= 1;
        switch (phase) {
            inline else => |kind| @field(self.publication_stats.maintenance_failures, @tagName(kind)) +|= 1,
        }
        self.publication_stats.last_maintenance_error = @errorName(err);
        self.publication_stats.last_maintenance_failure = @errorName(err);
        self.publication_stats.last_maintenance_failure_table_id = table_id;
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

    const MaintenanceCapture = enum { storage, artifacts, source };

    fn refreshMaintenanceSnapshot(self: *Owner, foreground: *h.Handle, capture: MaintenanceCapture) !*h.Handle {
        const need_database = capture != .storage;
        const file = &foreground.owned_lite_backend.?.native_docstore.?.file;
        if (self.maintenance_snapshot) |snapshot| {
            const cached_store = snapshot.owned_lite_backend.?.native_docstore.?;
            const captured = &cached_store.file;
            const same_inode = (try file.file.stat(file.runtime())).inode == (try captured.file.stat(captured.runtime())).inode;
            const logical = file.activeCheckpoint().logical;
            // Artifact continuations own immutable index inputs. Their catalog
            // proof does not validate source-runtime caches after a primary
            // rebase. Source replay always constructs its DB on the captured tip.
            const retained_inputs_match = if (self.maintenance_retained_logical) |expected|
                capture != .source and snapshot.db_live and file.header.indexed_reclamation and captured.header.indexed_reclamation and
                    !std.mem.allEqual(u8, &expected.metadata, 0) and !std.mem.allEqual(u8, &expected.indexes, 0) and
                    std.mem.eql(u8, &expected.metadata, &logical.metadata) and std.mem.eql(u8, &expected.indexes, &logical.indexes)
            else
                false;
            if (same_inode and (!need_database or snapshot.db_live) and
                ((std.meta.eql(file.header, captured.header) and !snapshot.db_live) or retained_inputs_match))
            {
                // Primary commits do not invalidate immutable index continuations.
                // Rebind before taking this turn's fresh primary read epoch; a
                // changed catalog/index identity or inode always opens anew.
                if (!std.meta.eql(file.header, captured.header)) try cached_store.refreshIndexPublicationCheckpoint(file.header);
                try captured.inheritAllocatorVerification(file);
                snapshot.sql_commit_recovery_needed = foreground.sql_commit_recovery_needed;
                snapshot.sql_ddl_recovery_needed = foreground.sql_ddl_recovery_needed;
                snapshot.sql_ddl_recovery_generation = foreground.sql_ddl_recovery_generation;
                self.configureRetirementBudget(cached_store);
                return snapshot;
            }
        }
        self.maintenance_retained_logical = null;
        var options = self.options;
        options.externally_locked = true;
        // Immutable artifact leases protect the exact input trees. A coarse
        // inode reader here would force every peer writer to append throughout
        // detached computation, even when it touches unrelated pages.
        options.artifact_reader_fence = false;
        options.allocator_verification_owner = file;
        // This owner gate and background-turn lease make the previous detached
        // cache quiescent. Authenticate its rebase before opening indexes can
        // publish leases; mutable allocator state is moved, never shared.
        var certified: ?native.NativeFile = null;
        defer if (certified) |*cache| cache.close();
        if (self.maintenance_snapshot) |old| {
            const previous = &old.owned_lite_backend.?.native_docstore.?.file;
            if (old.maintenance_references == 0 and previous.ledger != null) {
                certified = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
                certified.?.allocator_cancel_token = &self.verification_cancel;
                if (try certified.?.takeAllocatorRebaseBase(previous)) {
                    if (try certified.?.inheritAllocatorCertificate()) {
                        options.allocator_verification_owner = &certified.?;
                        options.allocator_state_donor = &certified.?;
                    }
                }
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
            if (options.allocator_state_donor) |donor|
                _ = try backend.native_docstore.?.file.takeVerifiedAllocator(donor);
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
        try store.file.inheritAllocatorVerification(file);
        if (self.maintenance_snapshot) |old| {
            try store.file.inheritAllocatorVerification(&old.owned_lite_backend.?.native_docstore.?.file);
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
        var lease = native.lockWriterPathWithIo(alloc, h.handleLockIo(), self.path) catch |err| switch (err) {
            error.FileBusy, error.WouldBlock => return,
            else => return err,
        };
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
        const turn_started = @import("antfly_platform").time.monotonicNs();
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
        if (self.options.profile == .native and self.reader_coordinator.hasArtifactReleases())
            self.maintenance_storage_pending = true;
        if (self.options.profile == .native and self.maintenance_storage_pending and !self.maintenance_last_storage) {
            // A physical maintenance error must remain observable without
            // preventing subsequent replay turns from making progress.
            self.maintenance_last_storage = true;
            self.maintenance_storage_pending = false;
            self.maintenance_storage_pending = !(self.storageMaintenanceTurn() catch |err| {
                self.gate.mutex.lockUncancelable(turn_io);
                self.noteMaintenanceFailure(err, 0, .storage);
                self.gate.mutex.unlock(turn_io);
                return true;
            });
            return true;
        }
        self.maintenance_last_storage = false;
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
        var lease = try self.acquireMaintenancePathLeaseAssumeLocked();
        var lease_held = true;
        defer if (lease_held) lease.close();
        const foreground = try self.refresh();
        if (needsOutcomeReconciliation(foreground)) return false;
        const foreground_file = &foreground.owned_lite_backend.?.native_docstore.?.file;
        // Marker service is directory-bounded and needs no allocator proof.
        // Reap crash/normal-exit leftovers even if this short-lived owner is
        // canceled before its detached graph walk can finish.
        if (self.options.open_mode == .writer)
            try self.artifact_marker_sweep.run(foreground_file);
        if (self.options.open_mode == .writer and !foreground_file.free_pages_verified) {
            if (!self.allocator_proof_mutex.tryLock()) return true;
            defer self.allocator_proof_mutex.unlock(io);
            var proof_lease: ?native.PathWriterLock = lease;
            lease_held = false;
            defer if (proof_lease) |*held| held.close();
            try self.verifyAllocatorAssumeLocked(&proof_lease, null);
            return true;
        }
        try @import("sql_commit.zig").recover(foreground);
        try @import("sql_ddl.zig").recover(foreground);
        // Catalog identities suffice for rotation and dropped-table pruning.
        // Opening every runtime here makes each bounded turn O(all indexes),
        // delaying the next group lease observation behind unrelated opens.
        try @import("tables.zig").loadCatalog(foreground);
        // Priority tickets are immutable hints. Resolve them against this
        // admitted catalog; dropped/recreated namespaces cannot borrow state
        // from the incarnation that originally queued the ticket.
        var priority_name: ?[]const u8 = null;
        if (self.runnable_sources.choose(io)) |key| {
            if (self.maintenance_schedules.get(key)) |schedule| {
                if (schedule.name) |name| {
                    const live_id = if (std.mem.eql(u8, name, "default")) foreground.db.core.identity_namespace.table_id else foreground.embedded_table_records.get(name);
                    if (live_id != null and live_id.? == key.table_id) {
                        const namespace = if (std.mem.eql(u8, name, "default")) "root" else try @import("../storage/lite/embedded_catalog.zig").namespaceAlloc(alloc, key.table_id);
                        defer if (!std.mem.eql(u8, name, "default")) alloc.free(namespace);
                        if ((try foreground.owned_lite_backend.?.existingRootIncarnation(namespace)) == key.incarnation)
                            priority_name = name;
                    }
                }
            }
        }
        const audit = priority_name == null;
        if (audit) {
            self.publication_stats.maintenance_audit_turns +|= 1;
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
                if (entry.value_ptr.name) |name| alloc.free(name);
                self.maintenance_schedules.removeByPtr(entry.key_ptr);
            };
        } else self.publication_stats.maintenance_priority_turns +|= 1;
        const count = foreground.embedded_table_records.count() + 1;
        const ordinal = self.maintenance_table_cursor % count;
        if (audit) self.maintenance_table_cursor = (ordinal + 1) % count;
        const audit_round_end = audit and self.maintenance_table_cursor == 0;
        var database = &foreground.db;
        var selected: ?*@import("tables.zig").Table = null;
        var selected_name: ?[]const u8 = priority_name;
        if (audit) {
            selected_name = null;
            if (ordinal != 0) {
                var names = foreground.embedded_table_records.keyIterator();
                for (0..ordinal) |_| selected_name = names.next().?.*;
            }
        }
        if (selected_name) |name| if (!std.mem.eql(u8, name, "default")) {
            const table_id = foreground.embedded_table_records.get(name).?;
            const namespace = try @import("../storage/lite/embedded_catalog.zig").namespaceAlloc(alloc, table_id);
            defer alloc.free(namespace);
            if (audit and try self.namespaceCanSleep(foreground, namespace, table_id)) {
                self.publication_stats.maintenance_idle_namespace_turns +|= 1;
                self.publication_stats.maintenance_readiness_skips +|= 1;
                return self.finishMaintenanceVisit(false, false, audit_round_end);
            }
            database = @import("tables.zig").get(foreground, name) catch return true;
            selected = foreground.embedded_tables.get(name).?;
        };
        var namespace_pending = self.options.open_mode == .writer and try hasPendingReplay(database);
        const entry_sequence = database.core.nextDerivedSequence();
        const readiness_namespace = if (selected) |table| table.namespace else "root";
        const entry_readiness = if (self.options.profile == .native) try namespaceReadiness(foreground, readiness_namespace) else null;
        var authority_arena = std.heap.ArenaAllocator.init(alloc);
        defer authority_arena.deinit();
        if (self.options.open_mode == .writer and try @import("resolver_authority.zig").hasProjectionDebt(foreground, database, authority_arena.allocator())) namespace_pending = true;
        const group_state = try self.groupSourceStateNamed(database, if (selected) |table| table.name else "default");
        if (foreground_file.ledger) |ledger| {
            group_state.replay_budget.updatePressure(ledger.data_pending, ledger.free_pages, foreground_file.allocator_work.reuse_blocked_append_pages);
            group_state.producer_replay_budget.updatePressure(ledger.data_pending, ledger.free_pages, foreground_file.allocator_work.reuse_blocked_append_pages);
            // Bound each service publication independently, increasing useful
            // work when foreground retirement is outpacing the baseline turn.
            const work_pages: usize = @intCast(@min(512, 128 + ledger.data_pending / 8));
            self.publication_stats.retirement_work_pages = work_pages;
            self.configureRetirementBudget(foreground.owned_lite_backend.?.native_docstore.?);
        }
        self.publication_stats.replay_budget_table_id = database.core.identity_namespace.table_id;
        self.publication_stats.replay_budget = group_state.replay_budget.snapshot();
        self.publication_stats.producer_replay_budget = group_state.producer_replay_budget.producerSnapshot();
        const schedule_entry = try self.maintenance_schedules.getOrPut(alloc, .{
            .table_id = database.core.identity_namespace.table_id,
            .incarnation = database.root_incarnation,
        });
        if (!schedule_entry.found_existing) schedule_entry.value_ptr.* = .{};
        const schedule = schedule_entry.value_ptr;
        schedule.has_group_sources = database.core.index_manager.hasGroupSources();
        // Reconcile deferred opening-time index/LSM work once per namespace
        // incarnation. Foreground LSM scores also include instance-local
        // retired objects; a fresh detached runtime cannot service those and
        // must not reopen forever on their behalf.
        if (self.options.open_mode == .writer and schedule.startup_replay_pending and
            database.core.hasManagedIndexes() and database.core.nextDerivedSequence() != 0) namespace_pending = true;
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
            if (recovered.remaining != 0) schedule.database_pending = true;
            if (recovered.attempted != 0 or recovered.repaired != 0) {
                schedule.source_recovery_next = false;
                self.runnable_sources.notify(io, .{ .table_id = database.core.identity_namespace.table_id, .incarnation = database.root_incarnation });
                schedule.database_pending = true;
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
        if (self.options.open_mode == .writer and schedule.group_source_next and
            now >= schedule.next_group_source_ns)
        {
            // Group queues can advance through cursor/lease observations
            // without appending a journal record. Their poll deadline is an
            // independent wake source, not evidence that providers must run.
            schedule.next_group_source_ns = now + 100 * std.time.ns_per_ms;
            if (try database.runGroupSourceMaintenanceSlice(group_state)) {
                schedule.group_source_next = false;
                self.runnable_sources.notify(io, .{ .table_id = database.core.identity_namespace.table_id, .incarnation = database.root_incarnation });
                schedule.database_pending = true;
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
                    if (schedule.last_ttl_error == null or schedule.last_ttl_error.? != @as(anyerror, err)) std.log.warn("native Lite TTL cleanup deferred table={s} err={s}", .{ if (selected) |table| table.name else "default", @errorName(err) });
                    schedule.last_ttl_error = err;
                    return err;
                };
                schedule.last_ttl_error = null;
                ttl_more = ttl.needsAnotherPage();
            }
        }
        self.maintenance_round_ttl_pending = self.maintenance_round_ttl_pending or ttl_more;
        if (namespace_pending or schedule.database_pending) self.maintenance_pending = true;
        const run_source = if (self.options.profile == .native)
            namespace_pending or schedule.database_pending or entry_sequence != schedule.serviced_sequence or
                database.core.nextDerivedSequence() != entry_sequence or now >= schedule.next_database_audit_ns
        else
            self.maintenance_pending;
        const run_database = run_source or (schedule.artifact_pending and now >= schedule.next_artifact_ns);
        const captured_artifact_pending = schedule.artifact_pending;
        if (run_source) schedule.database_pending = true;
        if (run_database) {
            // Artifact continuations need a catalog capture, not source replay.
            self.publication_stats.maintenance_database_turns +|= 1;
            if (run_source) self.publication_stats.maintenance_source_turns +|= 1 else self.publication_stats.maintenance_artifact_only_turns +|= 1;
        } else self.publication_stats.maintenance_idle_namespace_turns +|= 1;
        const replay_debt = try foreground_file.replayAdmission();
        const replay_urgent = if (replay_debt) |debt|
            debt.records >= debt.limits.records / 2 or debt.bytes >= debt.limits.bytes / 2
        else
            false;
        // Reserve source service under pressure without starving artifacts.
        // Physical retirement still receives its bounded turn every cycle.
        const artifact_due = !replay_urgent or schedule.replay_turns_since_artifacts >= 3;
        schedule.replay_turns_since_artifacts = if (artifact_due) 0 else schedule.replay_turns_since_artifacts + 1;

        var turn_arena = std.heap.ArenaAllocator.init(alloc);
        defer turn_arena.deinit();
        const turn_alloc = turn_arena.allocator();
        var shadow_scopes: std.ArrayList(ShadowScope) = .empty;
        if (run_source and schedule.shadow_next) {
            const jobs = try database.graphReplacementStatusAlloc(turn_alloc);
            defer jobs.deinit();
            for (jobs.value) |job| if (job.phase == .catching_up) {
                // Construction remains detached. Cutover uses the live
                // coordinator under this short owner/path fence, so its tail
                // target and catalog swap exclude foreground appends together.
                _ = try database.finishBackgroundGraphReplacement(job.name, job.generation);
                schedule.shadow_next = false;
                self.runnable_sources.notify(io, .{ .table_id = database.core.identity_namespace.table_id, .incarnation = database.root_incarnation });
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
        const root = if (independent) try self.refreshMaintenanceSnapshot(foreground, if (run_source) .source else if (run_database) .artifacts else .storage) else foreground;
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
        var source_read = if (run_source) try database.core.store.runtime_store.beginRead() else null;
        defer if (source_read) |*read| read.abort();
        var catalog = if (run_database) try PublicationCatalog.captureForStoragePublication(turn_alloc, root, database, publication_name) else null;
        defer if (catalog) |*proof| proof.deinit();
        var publication: PreparedPublication = .{ .owner = self, .name = publication_name, .identity = database.core.identity_namespace, .incarnation = database.root_incarnation, .catalog = if (catalog) |*proof| proof else null, .index_prefix = index_prefix, .shadow_scopes = shadow_scopes.items, .snapshot_root = root, .snapshot_database = database, .publication_overhead_ns = &group_state.publication_overhead_ns, .source_read = if (source_read) |*read| read else null, .relational_rows = if (database.enrichment_runtime) |runtime| runtime.config.relational_base_rows else false };
        const managed = try database.core.index_manager.managedIndexes(turn_alloc);
        const replay_progress = if (run_source) try ReplayProgress.capture(turn_alloc, database, managed) else null;
        // Keep the primary epoch until runBackgroundTurn returns and every
        // replay cursor has closed. Preparing the last text segment does not
        // end the journal cursor's borrowing lifetime.
        root.maintenance_references += 1;
        root.maintenance_owner = self;
        root.maintenance_publication = if (run_database) &publication else null;
        const store = root.owned_lite_backend.?.native_docstore.?;
        store.file.replay_consumer_admission = true;
        const split_index_work = independent and store.file.header.indexed_reclamation;
        defer store.primary_reads_require_admission = false;
        const foreground_header = foreground.owned_lite_backend.?.native_docstore.?.file.header;
        const maintenance_entry_header = store.file.header;
        if (run_source) if (database.enrichment_runtime) |runtime| {
            if (self.embedding_memo == null) self.embedding_memo = memo_mod.QueryEmbeddingCache.init(alloc, io, .{ .max_bytes = embedding_memo_bytes, .ttl_ns = std.time.ns_per_min });
            runtime.dense_result_cache = .{ .ptr = self, .lookup = lookupEmbedding, .remember = rememberEmbedding };
            runtime.generated_publication = .{ .ptr = &publication, .commit = PreparedPublication.commit, .heartbeat = PreparedPublication.heartbeat, .acquire_store = PreparedPublication.acquireStore };
        };
        database.borrowed_group_source_state = group_state;
        if (run_source) database.async_context.replay_index_publication = .{ .ptr = root, .enter = enterReplayIndexPublication, .leave = leaveMaintenanceWriter };
        if (run_source) database.async_context.replay_metadata_publication = .{ .ptr = root, .enter = enterReplayMetadataPublication, .leave = leaveMaintenanceWriter };
        if (run_source) database.async_context.replay_publication = .{ .ptr = &publication, .publish = PreparedPublication.publishReplay };
        store.writer_admission = .{ .ptr = root, .enter = enterMaintenanceWriter, .leave = leaveMaintenanceWriter };
        lease.close();
        lease_held = false;
        self.gate.mutex.unlock(io);
        gate_locked = false;

        if (comptime h.builtin.is_test) if (self.test_after_capture) |hook| hook.notify(hook.ptr, root);
        const detached_started = @import("antfly_platform").time.monotonicNs();
        const cancellation = h.db_mod.types.CancellationToken.fromAtomic(&self.stopping);
        var maintenance_error: ?anyerror = null;
        var maintenance_error_phase: MaintenanceFailurePhase = .source;
        if (run_source) store.prepareAllocatorVerification(&self.verification_cancel) catch |err| {
            maintenance_error = err;
        };
        const proof_finished = @import("antfly_platform").time.monotonicNs();
        // A shadow owns disjoint unpublished index inputs. Give it a bounded
        // turn before replay, whose serving-index publication invalidates the
        // broad capture. Unrelated serving writes may rebase this turn, while
        // changes to any selected shadow generation still reject publication.
        const projected = if (run_source and maintenance_error == null) @import("resolver_authority.zig").pullProjection(root, database, turn_alloc) catch |err| blk: {
            maintenance_error = err;
            break :blk false;
        } else false;
        var retain_artifact_runtime = !run_database and root.db_live and self.maintenance_retained_logical != null;
        var pending = if (maintenance_error != null) true else if (run_source and !projected) database.runBackgroundTurnWithCancellation(cancellation, if (shadow_scopes.items.len != 0) .shadow else if (split_index_work) .replay else .complete) catch |err| blk: {
            maintenance_error = err;
            break :blk true;
        } else projected;
        const replay_finished = @import("antfly_platform").time.monotonicNs();
        // Capture source eligibility before retirement/LSM housekeeping adds
        // its own continuation. An artifact-only turn must never re-run source
        // providers, replay, relational sweeps or status snapshot publication.
        const source_more = run_source and (pending or projected or publication.index_inputs_changed or maintenance_error != null);
        const source_completed = run_source and maintenance_error == null;
        const source_advanced = if (source_completed) replay_progress.?.advanced(database) else false;
        // The schedule map can grow while the owner gate is released.
        var artifact_more = !run_database and captured_artifact_pending;
        var artifact_delay_ns: u64 = 100 * std.time.ns_per_ms;
        if (run_source) if (database.enrichment_runtime) |runtime| {
            runtime.dense_result_cache = null;
            runtime.generated_publication = null;
        };
        if (run_source) database.async_context.replay_index_publication = null;
        if (run_source) database.async_context.replay_metadata_publication = null;
        if (run_source) database.async_context.replay_publication = null;
        if (source_read) |*read| read.abort();
        source_read = null;
        publication.source_read = null;
        if (split_index_work and run_database and !projected and shadow_scopes.items.len == 0 and maintenance_error == null) {
            // Merges need their leased immutable inputs, not every primary
            // page visible when replay began. Keep only short validated read
            // admissions while one artifact job advances outside the gate.
            store.index_reads_require_admission = true;
            defer store.index_reads_require_admission = false;
            // Revalidate the current catalog and consumer fences while holding
            // path admission through a bounded journal-retirement transaction.
            const retirement_more = blk: {
                enterMaintenanceWriter(root, .index) catch |err| {
                    maintenance_error = err;
                    maintenance_error_phase = .retirement;
                    break :blk true;
                };
                defer leaveMaintenanceWriter(root);
                break :blk database.runBackgroundTurnWithCancellation(cancellation, .retirement) catch |err| {
                    maintenance_error = err;
                    maintenance_error_phase = .retirement;
                    break :blk true;
                };
            };
            artifact_more = retirement_more;
            pending = pending or retirement_more;
            const more = if (artifact_due) database.runBackgroundTurnWithCancellation(cancellation, .artifacts) catch |err| blk: {
                maintenance_error = err;
                maintenance_error_phase = .artifacts;
                break :blk true;
            } else database.core.index_manager.lsmMaintenanceScore() != 0;
            artifact_more = artifact_more or more;
            pending = pending or more;
            retain_artifact_runtime = more and maintenance_error == null;
            if (!retirement_more) {
                if (database.core.index_manager.nextLsmMaintenanceWakeDelayNsBestEffort()) |delay| {
                    artifact_delay_ns = delay;
                }
            }
        }
        var database_audit_delay_ns: u64 = 30 * std.time.ns_per_s;
        if (run_database and independent) {
            if (database.core.index_manager.nextLsmMaintenanceWakeDelayNsBestEffort()) |delay| {
                if (delay != 0) database_audit_delay_ns = @min(database_audit_delay_ns, delay);
            }
            if (database.core.primary_store_owner.nextLsmMaintenanceWakeDelayNsBestEffort()) |delay| {
                if (delay != 0) database_audit_delay_ns = @min(database_audit_delay_ns, delay);
            }
        }
        root.maintenance_publication = null;
        // No detached computation/publication can use the catalog now. Drop
        // its checkpoint pin before physical service, which otherwise retains
        // an obsolete reader fence throughout its own reuse/vacuum attempt.
        if (catalog) |*proof| proof.deinit();
        catalog = null;
        const retained_logical: ?native.LogicalGenerations = if (retain_artifact_runtime) store.file.activeCheckpoint().logical else null;
        if (independent and root.db_live and !retain_artifact_runtime) {
            // End the input fences before reclamation. Keeping them in an
            // idle runtime would needlessly prevent in-place page service.
            @import("tables.zig").closeAll(root);
            root.embedded_tables = .empty;
            root.db.closeImmutableSnapshot();
            root.db_live = false;
        }
        // Physical-service failures must not turn every otherwise idle
        // namespace into source work. Keep its retry signal separate.
        const artifacts_finished = @import("antfly_platform").time.monotonicNs();
        const artifact_failed = maintenance_error != null;
        if (comptime h.builtin.is_test) if (self.test_before_storage_service) |hook| hook.notify(hook.ptr, root);
        store.maintainBackgroundOnce(&self.verification_cancel) catch |err| {
            if (maintenance_error == null) {
                maintenance_error = err;
                maintenance_error_phase = .storage;
            }
            pending = true;
        };

        self.gate.mutex.lockUncancelable(io);
        defer self.gate.mutex.unlock(io);
        self.publication_stats.maintenance_capture_ns +|= detached_started -| turn_started;
        self.publication_stats.maintenance_proof_ns +|= proof_finished -| detached_started;
        self.publication_stats.maintenance_replay_ns +|= replay_finished -| proof_finished;
        self.publication_stats.producer_replay_budget = group_state.producer_replay_budget.producerSnapshot();
        self.publication_stats.maintenance_artifacts_ns +|= artifacts_finished -| replay_finished;
        self.publication_stats.maintenance_storage_ns +|= @import("antfly_platform").time.monotonicNs() -| artifacts_finished;
        self.maintenance_retained_logical = retained_logical;
        if (maintenance_error) |err| {
            self.noteMaintenanceFailure(err, publication.identity.table_id, maintenance_error_phase);
        } else self.publication_stats.last_maintenance_error = null;
        if (source_more and source_advanced) {
            self.runnable_sources.notify(io, .{ .table_id = publication.identity.table_id, .incarnation = publication.incarnation });
            self.publication_stats.maintenance_progress_requeues +|= 1;
        }
        if (run_database) {
            // Foreground table admission can grow the map while this turn is
            // detached. Reacquire its entry after taking the gate again.
            if (self.maintenance_schedules.getPtr(.{ .table_id = publication.identity.table_id, .incarnation = publication.incarnation })) |completed| {
                completed.database_pending = source_more;
                completed.artifact_pending = artifact_more or artifact_failed;
                completed.next_artifact_ns = std.Io.Clock.awake.now(io).nanoseconds + artifact_delay_ns;
                if (source_completed) {
                    completed.startup_replay_pending = false;
                    completed.serviced_sequence = entry_sequence;
                    completed.readiness = entry_readiness;
                    completed.next_database_audit_ns = std.Io.Clock.awake.now(io).nanoseconds + database_audit_delay_ns;
                }
            }
        }
        store.writer_admission = null;
        root.maintenance_owner = null;
        root.maintenance_references -= 1;
        if (independent) inheritReclamationTelemetry(self.current.?.owned_lite_backend.?.native_docstore.?, store);
        if (!independent and self.current != root) {
            store.read_only = true;
            store.file.read_only = true;
        }
        const changed = if (independent)
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
        pending = source_more or (maintenance_error != null) or
            (run_database and artifact_more and artifact_delay_ns <= 100 * std.time.ns_per_ms);
        return self.finishMaintenanceVisit(pending, changed, audit_round_end);
    }

    fn finishMaintenanceVisit(self: *Owner, pending: bool, changed: bool, audit_round_end: bool) bool {
        self.maintenance_round_pending = self.maintenance_round_pending or pending;
        const ttl_pending = self.maintenance_round_ttl_pending;
        if (audit_round_end) {
            self.publication_stats.maintenance_rounds +|= 1;
            self.maintenance_round_ttl_pending = false;
            // Mutations arriving during the detached turn still wake the owner;
            // replay debt is inspected again under the next capture fence.
            self.maintenance_pending = self.maintenance_round_pending;
            self.maintenance_round_pending = false;
        }
        self.collectRetired();
        return self.runnable_sources.pending(h.handleLockIo()) or self.maintenance_storage_pending or self.maintenance_pending or self.maintenance_table_cursor != 0 or changed or ttl_pending;
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
        var lease_held = true;
        defer if (lease_held) lease.close();
        const foreground = try self.refresh();
        if (needsOutcomeReconciliation(foreground)) return true;
        // A changed tip can invalidate a previously scheduled storage turn.
        // Yield it to the shared proof lane before bounded reclamation can
        // require reuse, rather than proving a second pool under its gate.
        if (!foreground.owned_lite_backend.?.native_docstore.?.file.free_pages_verified) return true;
        const entry_header = foreground.owned_lite_backend.?.native_docstore.?.file.header;
        const root = try self.refreshMaintenanceSnapshot(foreground, .storage);
        const store = root.owned_lite_backend.?.native_docstore.?;
        const maintenance_entry_header = store.file.header;
        root.maintenance_references += 1;
        root.maintenance_owner = self;
        root.maintenance_publication = null;
        store.writer_admission = .{ .ptr = root, .enter = enterMaintenanceWriter, .leave = leaveMaintenanceWriter };
        lease.close();
        lease_held = false;
        self.gate.mutex.unlock(io);
        gate_locked = false;
        const service_started = @import("antfly_platform").time.monotonicNs();
        const result = store.maintainBackgroundOnce(&self.verification_cancel);
        self.gate.mutex.lockUncancelable(io);
        gate_locked = true;
        self.publication_stats.maintenance_storage_ns +|= @import("antfly_platform").time.monotonicNs() -| service_started;
        store.writer_admission = null;
        root.maintenance_owner = null;
        root.maintenance_references -= 1;
        inheritReclamationTelemetry(self.current.?.owned_lite_backend.?.native_docstore.?, store);
        try self.adoptIdleMaintenanceCheckpoint(foreground, entry_header, maintenance_entry_header, store);
        try result;
        return true;
    }

    pub fn maintenanceTurnForTest(self: *Owner) !bool {
        std.debug.assert(h.builtin.is_test);
        return self.maintenanceTurn();
    }

    fn maintenanceLoop(self: *Owner) void {
        const io = h.handleLockIo();
        var next_wait_ms: i64 = 1000;
        var earliest_turn_ns: i96 = 0;
        while (!self.stopping.load(.acquire)) {
            self.wake.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(next_wait_ms), .clock = .awake } }) catch {};
            self.wake.reset();
            if (self.stopping.load(.acquire)) break;
            const now = std.Io.Clock.awake.now(io).nanoseconds;
            if (now < earliest_turn_ns) {
                // Coalesce mutation wakes between rounds. Every selected
                // namespace still yields the gate to queued foreground calls.
                next_wait_ms = @intCast(@divCeil(earliest_turn_ns - now, std.time.ns_per_ms));
                continue;
            }
            const pending = self.maintenanceTurn() catch |err| blk: {
                // Capture/control failures otherwise disappear before the
                // detached replay diagnostics can observe them.
                self.gate.mutex.lockUncancelable(io);
                self.noteMaintenanceFailure(err, 0, .capture);
                self.gate.mutex.unlock(io);
                break :blk true;
            };
            // A scheduling quantum belongs to a fair round, not each idle
            // namespace. Otherwise 45 unrelated tables add 4.5 seconds to
            // every activation, delivery, claim and publication observation.
            next_wait_ms = self.maintenanceWaitMillis(pending);
            earliest_turn_ns = std.Io.Clock.awake.now(io).nanoseconds + if (pending) @as(i96, next_wait_ms) * std.time.ns_per_ms else 0;
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
        return if (self.runnable_sources.pending(h.handleLockIo()) or self.maintenance_storage_pending or self.maintenance_table_cursor != 0) 0 else if (pending) 100 else 1000;
    }

    pub fn maintenanceWaitMillisForTest(self: *Owner, pending: bool) i64 {
        std.debug.assert(h.builtin.is_test);
        return self.maintenanceWaitMillis(pending);
    }

    /// Caller holds the owner gate, path lease and proof mutex. Return with
    /// the gate held on every path; proof teardown releases the path lease.
    /// Runtime pins keep the reader's borrowed allocator/executor alive while
    /// the graph walk runs with both publication locks released.
    fn verifyAllocatorAssumeLocked(self: *Owner, lease: *?native.PathWriterLock, connection: ?*Connection) !void {
        const io = h.handleLockIo();
        if (!try self.retainMaintenanceTurnAssumeLocked()) return error.FileBusy;
        defer self.releaseMaintenanceTurnAssumeLocked();
        const foreground = try self.refresh();
        const file = &foreground.owned_lite_backend.?.native_docstore.?.file;
        if (needsOutcomeReconciliation(foreground)) return error.OutcomeUnknown;
        if (file.transaction_header != null or file.durable_header != null) return error.FileBusy;
        if (file.free_pages_verified) return;
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
        lease.*.?.close();
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
        if (verified) |_| {} else |_| {
            self.publication_stats.allocator_proof_failures +|= 1;
        }
        try verified;
        // A foreground operation has not started its mutation yet. Preserve
        // its connection-local busy policy when another process wins the
        // writer lease during the detached proof. Background turns stay
        // nonblocking and yield to that writer instead of consuming its budget.
        lease.* = if (connection) |caller| try caller.acquirePathLeaseAssumeLocked() else self.withReadRegistrationBatch(try native.lockWriterPathWithIo(alloc, io, self.path));
        var probe = try native.NativeFile.openWithIo(alloc, io, self.path, .{ .read_only = true, .internal_reader = true });
        defer probe.close();
        if (self.current) |current| {
            // A foreground publication may have failed while the walk ran.
            // Retrying the same immutable proof cannot reconcile that outcome.
            if (needsOutcomeReconciliation(current)) return error.OutcomeUnknown;
            const live = &current.owned_lite_backend.?.native_docstore.?.file;
            if (live.transaction_header != null or live.durable_header != null) return error.FileBusy;
            if (std.meta.eql(live.header, probe.header) and
                (try live.file.stat(live.runtime())).inode == (try probe.file.stat(probe.runtime())).inode)
                try live.inheritAllocatorVerification(reader);
        }
    }

    fn deinitMaintenanceSchedules(self: *Owner) void {
        var schedules = self.maintenance_schedules.valueIterator();
        while (schedules.next()) |schedule| {
            if (schedule.group_sources) |state| {
                state.deinit();
                alloc.destroy(state);
            }
            if (schedule.name) |name| alloc.free(name);
        }
        self.maintenance_schedules.deinit(alloc);
        self.runnable_sources.deinit(alloc);
    }

    fn certifyAllocatorOnClose(self: *Owner) !void {
        if (self.options.profile != .native or self.options.open_mode != .writer) return;
        const io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(io);
        defer self.gate.mutex.unlock(io);
        var lease: ?native.PathWriterLock = try native.lockWriterPathWithIo(alloc, io, self.path);
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
                    if (file.read_only or !std.meta.eql(file.header, tip.header) or
                        (try file.file.stat(file.runtime())).inode != (try tip.file.stat(tip.runtime())).inode) continue;
                    if (!file.free_pages_verified) {
                        if (self.current == root and !needsOutcomeReconciliation(root) and
                            file.transaction_header == null and file.durable_header == null) can_prove_current = true;
                        continue;
                    }
                    try file.sync();
                    _ = try file.certifyAllocator();
                    return;
                }
            }
            if (attempt != 0 or !can_prove_current or !self.allocator_certificate_pending or self.allocator_verification_started or
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
            try self.verifyAllocatorAssumeLocked(&lease, null);
            if (lease == null) lease = try native.lockWriterPathWithIo(alloc, io, self.path);
        }
    }

    fn deinit(self: *Owner) void {
        if (self.worker) |worker| worker.join();
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

    pub fn acquire(self: *Connection) !native.PathWriterLock {
        const io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(io);
        errdefer self.gate.mutex.unlock(io);
        return self.acquirePathLeaseAssumeLocked();
    }

    fn acquirePathLeaseAssumeLocked(self: *Connection) !native.PathWriterLock {
        const io = h.handleLockIo();
        const deadline = self.admission_deadline_ns orelse (std.Io.Clock.awake.now(io).nanoseconds + @as(i96, self.options.busy_timeout_ms) * std.time.ns_per_ms);
        var backoff: i96 = std.time.ns_per_ms;
        while (true) {
            // A slow detached proof may outlast the retry budget. Always try
            // the lease once: a stable verified tip must still be admitted if
            // no contention wait is needed. An expired budget forbids waiting,
            // not an uncontended acquisition.
            const lease = if (h.liteOpenModeCanWrite(self.options.open_mode))
                native.lockWriterPathWithIo(alloc, io, self.path)
            else
                native.lockReaderPathWithIo(alloc, io, self.path);
            if (lease) |held| return self.owner.withReadRegistrationBatch(held) else |err| {
                if (err != error.WouldBlock and err != error.FileBusy) return err;
                const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
                if (remaining <= 0) return error.FileBusy;
                if (comptime h.builtin.is_test) if (self.test_path_lease_busy) |hook| hook.notify(hook.ptr);
                if (native.waitPathLockWithIo(alloc, self.owner.path_lock_io.io(), self.path, h.liteOpenModeCanWrite(self.options.open_mode), deadline)) |held| return self.owner.withReadRegistrationBatch(held) else |wait_err| switch (wait_err) {
                    // Sidecar-free read-only snapshots retain their inode
                    // authority. Resource-limited executors retain bounded
                    // polling rather than making the deadline unbounded.
                    error.FileNotFound, error.ConcurrencyUnavailable => {},
                    else => return wait_err,
                }
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
    pub fn refresh(self: *Connection, lease: *?native.PathWriterLock, intent: api.StorageIntent) !*h.Handle {
        var root = try self.owner.refresh();
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
                self.owner.allocator_proof_mutex.lockUncancelable(io);
                self.gate.mutex.lockUncancelable(io);
            }
            defer self.owner.allocator_proof_mutex.unlock(io);
            if (waited) {
                lease.* = try self.acquirePathLeaseAssumeLocked();
                root = try self.owner.refresh();
            }
            if (try needsCapacityProof(root) or (intent == .mutation and try self.needsReuseAdmission(root))) {
                // Keep foreign unverified opens from publishing index leases
                // between the detached proof and reacquiring the writer lease.
                if (!try self.owner.retainMaintenanceTurnAssumeLocked()) return error.FileBusy;
                defer self.owner.releaseMaintenanceTurnAssumeLocked();
                try self.owner.verifyAllocatorAssumeLocked(lease, self);
                if (lease.* == null) lease.* = try self.acquirePathLeaseAssumeLocked();
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
    // A newly created runtime opens cached read views before the connection
    // owner is installed. Give those views the eventual owner's coordinator
    // from the start, rather than leaving unregistered inode fences behind.
    if (create) @import("io_lifetime.zig").acquire();
    defer if (create) @import("io_lifetime.zig").release();
    var initial_coordinator: ?*native.ReaderCoordinator = null;
    defer if (initial_coordinator) |coordinator| coordinator.release();
    if (create) initial_coordinator = try native.ReaderCoordinator.create(alloc, h.handleLockIo());
    var initial: ?*h.Handle = null;
    errdefer if (initial) |root| {
        root.lease_snapshot = true;
        h.closeHandle(root);
    };
    if (create) {
        var resolved = resolved_options;
        resolved.externally_locked = true;
        resolved.reader_coordinator = initial_coordinator;
        initial = try api.openLiteHandleAllocWithRuntime(alloc, path, resolved, true, h.handleLockIo(), null);
        initial.?.owned_lite_backend.?.native_docstore.?.file.releaseWriterOwnership();
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
        if (initial_coordinator) |coordinator| {
            coordinator.retain();
            owner.reader_coordinator = coordinator;
        } else owner.reader_coordinator = try native.ReaderCoordinator.create(alloc, io);
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
            const attempt = if (h.liteOpenModeCanWrite(options.open_mode))
                native.lockWriterPathWithIo(alloc, io, gate.path)
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

test "capi native create coordinates its initial read generation" {
    @import("io_lifetime.zig").acquire();
    defer @import("io_lifetime.zig").release();
    const a = std.testing.allocator;
    var directory = try api.TestDirectory.init("initial-reader-coordination");
    defer directory.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{directory.path()});
    defer a.free(path);
    const coordinator = try native.ReaderCoordinator.create(a, h.handleLockIo());
    defer coordinator.release();
    const root = try api.openLiteHandleAllocWithRuntime(alloc, path, .{ .profile = .native, .externally_locked = true, .no_sync = true, .reader_coordinator = coordinator }, true, h.handleLockIo(), null);
    defer h.closeHandle(root);
    const store = root.owned_lite_backend.?.native_docstore.?;
    var read = try store.beginRead();
    defer read.abort();
    const generation = read.read_generation orelse return error.TestExpectedReadGeneration;
    try std.testing.expectEqual(@as(?*native.ReaderCoordinator, coordinator), generation.file.reader_coordinator);
    try std.testing.expect(generation.file.reader_fence != null);
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
            const root = try connection.refresh(&lease, .read_only);
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
            if (scenario == .uncertain or scenario == .uncertain_during) {
                owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
                if (scenario == .uncertain) {
                    owner.current.?.owned_lite_backend.?.native_docstore.?.file.checkpoint_publication_uncertain = true;
                }
                try std.testing.expectError(error.OutcomeUnknown, connection.refresh(&lease, .mutation));
                try std.testing.expectEqual(@as(usize, if (scenario == .uncertain) 0 else 1), probe.captures);
                try std.testing.expect(!owner.allocator_verification_active);
                try std.testing.expect(!owner.verification_cancel.requested.load(.acquire));
                continue;
            }
            const root = try connection.refresh(&lease, .mutation);
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
        const result = owner.verifyAllocatorAssumeLocked(&lease, if (scenario == .background) null else connection);
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
                const root = try connection.refresh(&lease, .mutation);
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
            const root = try connection.refresh(&lease, .mutation);
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
    const result = connection.refresh(&lease, .mutation);
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
    const root = try connection.refresh(&lease, .mutation);
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
        try owner.verifyAllocatorAssumeLocked(&lease, connection);
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
    try std.testing.expectEqual(std.Io.Limit.limited(2), connection.owner.path_lock_io.concurrent_limit);
    // A fallback through debug_io would never have started these workers.
    try std.testing.expect(connection.owner.path_lock_io.worker_threads.load(.acquire) != null);
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
    try std.testing.expect(owner.path_lock_io.worker_threads.load(.acquire) != null);
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
    var catalog = try PublicationCatalog.captureForStoragePublication(arena.allocator(), root, database, "numbers");
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
    try publication.validateStorageCurrent(&root.owned_lite_backend.?.native_docstore.?.file);
    try std.testing.expect(!root.embedded_tables.contains("numbers"));
    try std.testing.expectEqualDeep(header, root.owned_lite_backend.?.native_docstore.?.file.header);
    const id = root.embedded_table_records.getPtr("numbers").?;
    const original_id = id.*;
    id.* += 1;
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateStorageCurrent(&root.owned_lite_backend.?.native_docstore.?.file));
    id.* = original_id;
    publication.incarnation +%= 1;
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateStorageCurrent(&root.owned_lite_backend.?.native_docstore.?.file));
    publication.incarnation -%= 1;
    var txn = try database.core.store.beginWriteTxn();
    errdefer txn.abort();
    try txn.put(keys.public_schema_json_key, "{\"changed\":true}");
    try txn.commit();
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateStorageCurrent(&root.owned_lite_backend.?.native_docstore.?.file));
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
        detached = try owner.refreshMaintenanceSnapshot(owner.current.?, .storage);
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
    const admitted = owner.publication_stats.allocator_service_admissions;
    try Owner.enterMaintenanceWriter(detached, .allocator_service);
    var held = true;
    defer if (held) Owner.leaveMaintenanceWriter(detached);
    try std.testing.expect(std.meta.eql(file.header, owner.current.?.owned_lite_backend.?.native_docstore.?.file.header));
    try std.testing.expectEqual(admitted + 1, owner.publication_stats.allocator_service_admissions);
    _ = try file.reclaimPagesForMaintenanceWithCancel(8, null);
    try std.testing.expect((try file.check()).valid);
    Owner.leaveMaintenanceWriter(detached);
    held = false;
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
            const file = std.Io.Dir.cwd().openFile(h.handleLockIo(), store.file.path, .{
                .mode = .read_write,
                .lock = .exclusive,
                .lock_nonblocking = true,
            }) catch |err| {
                self.failure = err;
                return;
            };
            defer file.close(h.handleLockIo());
            const epochs = @import("../storage/lite/reader_epochs.zig").scan(alloc, h.handleLockIo(), store.file.path, store.file.header.reader_epoch_namespace, (file.stat(h.handleLockIo()) catch |err| {
                self.failure = err;
                return;
            }).inode) catch |err| {
                self.failure = err;
                return;
            };
            if (during_replay != (epochs.data != null)) self.failure = error.IndexReplaySnapshotNotFenced;
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

test "capi native artifact maintenance releases primary history before merging" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("artifact-phase-primary-history", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    const Probe = struct {
        root: ?*h.Handle = null,
        calls: usize = 0,
        reused: bool = false,
        fn capture(ptr: *anyopaque, root: *h.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.root = root;
            @import("../storage/db/catalog/index_manager.zig").test_before_text_merge_step = .{ .manager = root.db.core.index_manager, .ptr = self, .notify = merge };
        }
        fn merge(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const root = self.root.?;
            const publication = root.maintenance_publication.?;
            try std.testing.expect(publication.source_read == null);
            const store = root.owned_lite_backend.?.native_docstore.?;
            try std.testing.expect(store.index_reads_require_admission);
            // A physical-root ABA must still reject changed logical inputs.
            // Poisoning both old roots proves this comparison reads no pages.
            var old = store.file;
            old.header.checkpoints[old.header.active_checkpoint].index_catalog_root_page = std.math.maxInt(u64);
            var latest = old;
            try std.testing.expect(try publication.indexInputsUnchanged(&old, &latest));
            latest.header.checkpoints[latest.header.active_checkpoint].logical.indexes[0] ^= 1;
            try std.testing.expect(!try publication.indexInputsUnchanged(&old, &latest));
            const io = h.handleLockIo();
            {
                const locked = try std.Io.Dir.cwd().openFile(io, store.file.path, .{ .mode = .read_write, .lock = .exclusive, .lock_nonblocking = true });
                defer locked.close(io);
                const epochs = try @import("../storage/lite/reader_epochs.zig").scan(alloc, io, store.file.path, store.file.header.reader_epoch_namespace, (try locked.stat(io)).inode);
                try std.testing.expect(epochs.data == null);
            }
            {
                var lease = try native.lockWriterPathWithIo(alloc, io, store.file.path);
                defer lease.close();
                var peer = try native.NativeFile.openWithIo(alloc, io, store.file.path, .{ .externally_locked = true, .no_sync = true });
                defer peer.close();
                try peer.inheritAllocatorVerification(&store.file);
                peer.allocator_certificate_on_commit = true;
                const before = (try peer.allocatorStats()).?.reused_pages;
                var value: [8192]u8 = @splat('v');
                for (0..128) |i| {
                    std.mem.writeInt(u64, value[0..8], i, .little);
                    try peer.putDocument("artifact-phase-peer", &value);
                }
                self.reused = (try peer.allocatorStats()).?.reused_pages > before;
                try std.testing.expect(self.reused);
                try std.testing.expect((try peer.check()).valid);
            }
            // A catalog read after peer reuse must first rebase under a short
            // admission, without touching the unretained old catalog pages.
            try Owner.enterMaintenanceWriter(root, .index_read);
            Owner.leaveMaintenanceWriter(root);
        }
    };
    var probe: Probe = .{};
    defer @import("../storage/db/catalog/index_manager.zig").test_before_text_merge_step = null;
    owner.test_after_capture = .{ .ptr = &probe, .notify = Probe.capture };
    defer owner.test_after_capture = null;
    for (0..8) |_| {
        owner.maintenance_pending = true;
        _ = try owner.maintenanceTurn();
        if (probe.calls != 0) break;
    }
    try std.testing.expect(probe.calls != 0);
    try std.testing.expect(probe.reused);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.read));
    try fixture.expectWritten(.batch, true);
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
    const first = try owner.refreshMaintenanceSnapshot(root, .storage);
    try std.testing.expectEqual(@as(usize, 256), first.owned_lite_backend.?.native_docstore.?.maintenance_policy.options.retirement_work_pages);
    owner.publication_stats.retirement_work_pages = 512;
    try root.owned_lite_backend.?.native_docstore.?.file.putCatalogRecord("budget-test", "next snapshot");
    const replacement = try owner.refreshMaintenanceSnapshot(root, .storage);
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
        const root = try connection.refresh(&lease, .read_only);
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
        const refreshed = try connection.refresh(&lease, .read_only);
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
        const changed = try connection.refresh(&lease, .read_only);
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
    const root = try connection.refresh(&lease, .read_only);
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
    const root = try connection.refresh(&lease, .read_only);
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
    const root = try connection.refresh(&lease, .read_only);
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
    const refreshed = try connection.refresh(&lease, .read_only);
    try std.testing.expect(!std.mem.eql(u8, &clean, &refreshed.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().logical.metadata));
    for (0..20) |_| {
        try @import("sql_ddl.zig").recover(refreshed);
        try std.testing.expect(refreshed.sql_ddl_recovery_needed);
        try std.testing.expectError(error.SqlStatementReadUnavailable, @import("sql_ddl.zig").requireRecovered(refreshed, &refreshed.db, "default"));
    }
}

test "capi native artifact continuation survives primary writes and rejects changed inputs" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("artifact-continuation-rebase", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    var retained: *h.Handle = undefined;
    var opened: u64 = undefined;
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const file = &owner.current.?.owned_lite_backend.?.native_docstore.?.file;
        try file.putIndexCatalogRecord("/continuation-test/input", "original");
        retained = try owner.refreshMaintenanceSnapshot(owner.current.?, .artifacts);
        owner.maintenance_retained_logical = retained.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().logical;
        opened = owner.test_runtime_snapshot_opens;
    }
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const resumed = try owner.refreshMaintenanceSnapshot(owner.current.?, .artifacts);
        try std.testing.expectEqual(retained, resumed);
        try std.testing.expectEqual(opened, owner.test_runtime_snapshot_opens);
        try std.testing.expectEqualDeep(owner.current.?.owned_lite_backend.?.native_docstore.?.file.header, resumed.owned_lite_backend.?.native_docstore.?.file.header);
        // A foreign physical index mutation fences cached input continuations.
        try owner.current.?.owned_lite_backend.?.native_docstore.?.file.putIndexCatalogRecord("/continuation-test/input", "changed");
        const replacement = try owner.refreshMaintenanceSnapshot(owner.current.?, .artifacts);
        try std.testing.expectEqual(opened + 1, owner.test_runtime_snapshot_opens);
        owner.maintenance_retained_logical = replacement.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().logical;
        // Catalog changes also invalidate a cached DB, even on the same inode.
        try owner.current.?.owned_lite_backend.?.native_docstore.?.file.putCatalogRecord("test/continuation-catalog", "changed");
        _ = try owner.refreshMaintenanceSnapshot(owner.current.?, .artifacts);
        try std.testing.expectEqual(opened + 2, owner.test_runtime_snapshot_opens);
    }
}

test "capi native replay publication guards preserve recovery and replacement fences without runtime open" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = try api.TestDirectory.init("native-replay-publication-guard");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    const root = try api.openLiteHandleAllocWithRuntime(alloc, path, .{ .profile = .native, .externally_locked = true, .no_sync = true }, true, null, null);
    defer h.closeHandle(root);
    const store = root.owned_lite_backend.?.native_docstore.?;
    store.maintenance_start_suppressed = true;
    try @import("sql_commit.zig").recover(root);
    try @import("sql_ddl.zig").recover(root);
    var proof = try PublicationCatalog.captureForStoragePublication(a, root, &root.db, "default");
    defer proof.deinit();
    try std.testing.expect(proof.recovery_clean);
    try store.file.putDocument("unrelated", "primary");
    try store.file.putIndexCatalogRecord("/unrelated/index", "artifact");
    try proof.verifyNative(&store.file);
    // An unresolved SQL decision must fence the fast path even though it
    // changes only primary identity, not the metadata identity.
    try store.file.putDocument(@import("sql_commit.zig").key, "pending decision");
    try std.testing.expectError(error.EnrichmentSourceChanged, proof.verifyNative(&store.file));
    try store.file.deleteDocument(@import("sql_commit.zig").key);
    try proof.verifyNative(&store.file);
    try store.file.putDocument(@import("../storage/lite/embedded_catalog.zig").key, "changed mapping");
    try std.testing.expectError(error.EnrichmentSourceChanged, proof.verifyNative(&store.file));
    try store.file.deleteDocument(@import("../storage/lite/embedded_catalog.zig").key);
    try proof.verifyNative(&store.file);
    // Recovery markers and replacement generations carry fresh no-ABA cuts.
    try store.file.putCatalogRecord("system/new-metadata", "changed");
    try std.testing.expectError(error.EnrichmentSourceChanged, proof.verifyNative(&store.file));
    var fresh = try PublicationCatalog.captureForStoragePublication(a, root, &root.db, "default");
    defer fresh.deinit();
    _ = try store.file.vacuum();
    try std.testing.expectError(error.EnrichmentSourceChanged, fresh.verifyNative(&store.file));
    proof.recovery_clean = false;
    try std.testing.expectError(error.EnrichmentWaitCanceled, proof.verifyNative(&store.file));
}

test "capi native replay admission refuses a transform before commit and returns credit after drain" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    for ([_]bool{ false, true }) |indexed| {
        var fixture = try AdmissionFollowupFixture.init("durable-replay-transform", 1000, .batch);
        defer fixture.deinit();
        if (indexed) try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"search\",\"type\":\"full_text\"}")));
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
        var output: h.capi.Buffer = .{};
        defer api.antfly_buffer_free_zero(&output);
        const seed = "{\"inserts\":{\"counter\":{\"body\":\"counter\",\"count\":0}},\"sync_level\":\"write\"}";
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice(seed), &output));
        api.antfly_buffer_free_zero(&output);
        {
            var lease = try fixture.connection.acquire();
            defer {
                lease.close();
                fixture.connection.gate.mutex.unlock(h.handleLockIo());
            }
            const file = &fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
            var state = (try file.replayAdmission()).?;
            try std.testing.expect(state.records > 0);
            state.limits.records = state.records;
            try file.putDocument(native.replay_admission.key, &state.encode());
        }
        const increment = "{\"transforms\":[{\"key\":\"counter\",\"operations\":[{\"op\":\"$inc\",\"path\":\"count\",\"value\":1}]}],\"sync_level\":\"write\"}";
        for (0..2) |_| {
            const refusals = fixture.connection.owner.publication_stats.replay_backpressure_waits;
            const polls = api.test_replay_capacity_polls.load(.monotonic);
            try std.testing.expectEqual(h.capi.ErrorCode.busy, api.antfly_db_batch_json(fixture.id, .fromSlice(increment), &output));
            // Capacity polling must not repeatedly enter mutation admission or
            // reconstruct serving runtimes while the same journal stays full.
            try std.testing.expectEqual(refusals + 1, fixture.connection.owner.publication_stats.replay_backpressure_waits);
            try std.testing.expect(api.test_replay_capacity_polls.load(.monotonic) > polls + 1);
            const raw = (try fixture.connection.owner.current.?.db.get(std.testing.allocator, "counter")).?;
            defer std.testing.allocator.free(raw);
            var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, raw, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(i64, 0), parsed.value.object.get("count").?.integer);
        }
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(fixture.id));
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice(increment), &output));
        const raw = (try fixture.connection.owner.current.?.db.get(std.testing.allocator, "counter")).?;
        defer std.testing.allocator.free(raw);
        var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, raw, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("count").?.integer);
    }
}

test "capi native replay admission preserves oversized record input ownership" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var fixture = try AdmissionFollowupFixture.init("replay-oversized-inputs", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"search\",\"type\":\"full_text\"}")));
    // A single record exceeds the initial 64-item collection budget. It must
    // remain one apply input boundary: releasing its source after a first
    // split callback would strand the remaining documents on every retry.
    var request: std.ArrayListUnmanaged(u8) = .empty;
    defer request.deinit(a);
    try request.appendSlice(a, "{\"inserts\":{");
    for (0..129) |i| {
        const item = try std.fmt.allocPrint(a, "{s}\"bulk-{d}\":{{\"text\":\"bulkfanoutcanary\"}}", .{ if (i == 0) "" else ",", i });
        defer a.free(item);
        try request.appendSlice(a, item);
    }
    try request.appendSlice(a, "}}");
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice(request.items), &output));
    api.antfly_buffer_free_zero(&output);
    var caught_up = false;
    for (0..64) |_| {
        _ = try fixture.connection.owner.maintenanceTurnForTest();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_stats_json(fixture.id, &output));
        var stats = try std.json.parseFromSlice(std.json.Value, a, output.ptr.?[0..output.len], .{});
        for (stats.value.object.get("indexes").?.array.items) |index| {
            const fields = index.object;
            if (!std.mem.eql(u8, fields.get("name").?.string, "search")) continue;
            const target = fields.get("replay_target_sequence").?.integer;
            caught_up = target > 0 and fields.get("replay_applied_sequence").?.integer >= target and
                fields.get("doc_count").?.integer == 129;
        }
        stats.deinit();
        api.antfly_buffer_free_zero(&output);
        if (caught_up) break;
    }
    try std.testing.expect(caught_up);
}

test "capi native replay admission capacity wait releases registry pins for close" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("replay-capacity-close", 5000, .batch);
    var closed = false;
    defer if (!closed) fixture.deinit() else {
        std.testing.allocator.free(fixture.path);
        fixture.tmp.cleanup();
    };
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const file = &fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
        var state = (try file.replayAdmission()).?;
        state.limits.records = state.records;
        try file.putDocument(native.replay_admission.key, &state.encode());
    }
    const Request = struct {
        id: *anyopaque,
        code: h.capi.ErrorCode = .ok,
        fn run(self: *@This()) void {
            var output: h.capi.Buffer = .{};
            defer api.antfly_buffer_free_zero(&output);
            self.code = api.antfly_db_batch_json(self.id, .fromSlice("{\"inserts\":{\"waiter\":{\"value\":1}},\"sync_level\":\"write\"}"), &output);
        }
    };
    var request = Request{ .id = fixture.id };
    const polls = api.test_replay_capacity_polls.load(.monotonic);
    const worker = try std.Thread.spawn(.{}, Request.run, .{&request});
    var joined = false;
    defer if (!joined) worker.join();
    const limit = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + 2 * std.time.ns_per_s;
    while (api.test_replay_capacity_polls.load(.monotonic) == polls) {
        if (std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds >= limit) return error.CapacityWaitDidNotStart;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    api.antfly_db_close(fixture.id);
    closed = true;
    // Joining synchronizes the worker's plain result field.
    worker.join();
    joined = true;
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, request.code);
}

test "capi native replay maintenance replacement transfers authenticated allocator cache" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("maintenance-allocator-transfer", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const owner = fixture.connection.owner;
    const first = try owner.refreshMaintenanceSnapshot(owner.current.?, .storage);
    const first_file = &first.owned_lite_backend.?.native_docstore.?.file;
    first_file.incremental_checkpoints = true;
    try first_file.beginTransaction();
    try first_file.putCatalogRecord("cache-transfer-first", "one");
    try first_file.commitTransaction();
    const cached = first_file.ledger.?;
    const loads = first_file.allocator_work.full_loads;
    try std.testing.expect(cached.digest_cache != null);
    _ = try owner.refresh();
    const foreground_file = &owner.current.?.owned_lite_backend.?.native_docstore.?.file;
    foreground_file.incremental_checkpoints = true;
    try foreground_file.beginTransaction();
    try foreground_file.putDocument("cache-transfer-doc", "two");
    try foreground_file.commitTransaction();
    const replacement = try owner.refreshMaintenanceSnapshot(owner.current.?, .storage);
    const adopted = &replacement.owned_lite_backend.?.native_docstore.?.file;
    try std.testing.expectEqual(cached, adopted.ledger.?);
    try std.testing.expect(adopted.free_pages_verified);
    try std.testing.expectEqual(loads, adopted.allocator_work.full_loads);
    try adopted.putCatalogRecord("cache-transfer-after", "three");
    try std.testing.expectEqual(loads, adopted.allocator_work.full_loads);
    try std.testing.expect((try adopted.check()).valid);
}

test "capi native replay publication storage recovery witness isolates unrelated DDL and fences affected namespaces" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("publication-recovery-scope", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const root = fixture.connection.owner.current.?;
    try @import("tables.zig").create(root, "pending", "{}", false);
    const pending = try @import("tables.zig").get(root, "pending");
    {
        var txn = try pending.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put("\x00antfly/embedded/sql-ddl", "unfinished");
        try txn.commit();
    }
    root.sql_ddl_recovery_needed = true;
    const snapshot = root.lease_snapshot;
    root.lease_snapshot = true;
    defer root.lease_snapshot = snapshot;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var proof = try PublicationCatalog.captureForStoragePublication(arena.allocator(), root, &root.db, "default");
    defer proof.deinit();
    try std.testing.expect(root.sql_ddl_recovery_needed);
    try std.testing.expect(proof.recovery_clean);
    try proof.verifyNative(&root.owned_lite_backend.?.native_docstore.?.file);
    try std.testing.expectError(error.SqlStatementReadUnavailable, PublicationCatalog.captureForStoragePublication(arena.allocator(), root, pending, "pending"));
    // Global decisions still invalidate a healthy namespace's witness even
    // when their primary-only publication leaves metadata identity unchanged.
    const file = &root.owned_lite_backend.?.native_docstore.?.file;
    try file.putDocument("\x00antfly/embedded/sql-fk-publication", "unfinished");
    try std.testing.expectError(error.EnrichmentSourceChanged, proof.verifyNative(file));
    try std.testing.expectError(error.SqlStatementReadUnavailable, PublicationCatalog.captureForStoragePublication(arena.allocator(), root, &root.db, "default"));
}

test "capi native publication provider metadata borrows captured storage and retains source guards" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("provider-metadata-storage", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var lease: ?native.PathWriterLock = try fixture.connection.acquire();
    var gate_held = true;
    defer if (gate_held) {
        if (lease) |*held| held.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    };
    const owner = fixture.connection.owner;
    const root = try owner.refreshMaintenanceSnapshot(owner.current.?, .source);
    const database = &root.db;
    try database.reconfigureEnrichmentRuntimePaused(.{ .enable_without_producers = true });
    const source_key = try keys.documentKeyAlloc(std.testing.allocator, "guarded");
    defer std.testing.allocator.free(source_key);
    {
        var txn = try database.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(source_key, "initial");
        try txn.commit();
    }
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var proof = try PublicationCatalog.captureForStoragePublication(arena.allocator(), root, database, "default");
    defer proof.deinit();
    var source_read = try database.core.store.runtime_store.beginRead();
    defer source_read.abort();
    var publication: PreparedPublication = .{
        .owner = owner,
        .name = "default",
        .identity = database.core.identity_namespace,
        .incarnation = database.root_incarnation,
        .catalog = &proof,
        .index_prefix = "unused/",
        .snapshot_root = root,
        .snapshot_database = database,
        .source_read = &source_read,
        .relational_rows = false,
    };
    const store = root.owned_lite_backend.?.native_docstore.?;
    root.maintenance_references += 1;
    root.maintenance_owner = owner;
    root.maintenance_publication = &publication;
    store.writer_admission = .{ .ptr = root, .enter = Owner.enterMaintenanceWriter, .leave = Owner.leaveMaintenanceWriter };
    defer {
        store.writer_admission = null;
        root.maintenance_publication = null;
        root.maintenance_owner = null;
        root.maintenance_references -= 1;
    }
    const refreshes = owner.publication_stats.serving_runtime_refreshes;
    lease.?.close();
    lease = null;
    fixture.connection.gate.mutex.unlock(h.handleLockIo());
    gate_held = false;
    {
        var held = try PreparedPublication.acquireStore(&publication, null, 0);
        defer held.release(held.ptr);
        try held.guard_key(held.ptr, source_key);
        var txn = try held.store.beginWrite();
        errdefer txn.abort();
        try txn.put("\x00metadata-publication-probe", "committed");
        try txn.commit();
    }
    try std.testing.expectEqual(refreshes, owner.publication_stats.serving_runtime_refreshes);
    try std.testing.expectEqual(@as(u64, 1), owner.publication_stats.generated_storage_groups);
    // A second lease reads the latest physical checkpoint, but its source
    // guard must still compare against the independently pinned old input.
    {
        var held = try PreparedPublication.acquireStore(&publication, null, 0);
        defer held.release(held.ptr);
        var txn = try held.store.beginWrite();
        errdefer txn.abort();
        try txn.put(source_key, "changed");
        try txn.commit();
    }
    {
        const held = try PreparedPublication.acquireStore(&publication, null, 0);
        defer held.release(held.ptr);
        try std.testing.expectError(error.EnrichmentWaitCanceled, held.guard_key(held.ptr, source_key));
    }
    try std.testing.expectEqual(refreshes, owner.publication_stats.serving_runtime_refreshes);
    try std.testing.expectEqual(@as(usize, 0), root.maintenance_writer_references);
}

test "capi native maintenance namespaces isolate active replay from idle indexed tables" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("namespace-ready-replay", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    const owner = fixture.connection.owner;
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const root = owner.current.?;
        for (0..8) |i| {
            var name: [40]u8 = undefined;
            const table_name = try std.fmt.bufPrint(&name, "idle-{d}", .{i});
            try @import("tables.zig").create(root, table_name, "{}", false);
            const table = try @import("tables.zig").get(root, table_name);
            try table.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
        }
    }
    var warmed = false;
    for (0..256) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.maintenance_table_cursor != 0 or owner.maintenance_storage_pending or owner.maintenance_schedules.count() != 9) continue;
        var pending = false;
        var schedules = owner.maintenance_schedules.valueIterator();
        while (schedules.next()) |schedule| pending = pending or schedule.database_pending or schedule.next_database_audit_ns == 0;
        if (!pending) {
            warmed = true;
            break;
        }
    }
    try std.testing.expect(warmed);
    // Isolate source scheduling from the independent timed idle audit.
    var schedules = owner.maintenance_schedules.valueIterator();
    while (schedules.next()) |schedule| {
        schedule.next_database_audit_ns = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + std.time.ns_per_hour;
        schedule.next_artifact_ns = schedule.next_database_audit_ns;
    }
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice("{\"inserts\":{\"active-new\":{\"body\":\"namespace readiness canary\"}},\"sync_level\":\"write\"}"), &output));
    const target = owner.current.?.db.core.nextDerivedSequence();
    const opened = owner.test_runtime_snapshot_opens;
    const turns = owner.publication_stats.maintenance_database_turns;
    const idle_turns = owner.publication_stats.maintenance_idle_namespace_turns;
    const readiness_skips = owner.publication_stats.maintenance_readiness_skips;
    owner.maintenance_pending = true; // A global wakeup is not per-table work.
    const initial_round = owner.publication_stats.maintenance_rounds;
    var wrapped = false;
    for (0..64) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.publication_stats.maintenance_rounds > initial_round and !owner.maintenance_storage_pending) {
            wrapped = true;
            break;
        }
    }
    try std.testing.expect(wrapped);
    try std.testing.expectEqual(@as(u64, 1), owner.publication_stats.maintenance_database_turns - turns);
    // The audit can also visit the already-serviced root; every unrelated
    // indexed namespace must remain idle without opening its runtime.
    try std.testing.expect(owner.publication_stats.maintenance_idle_namespace_turns - idle_turns >= 8);
    try std.testing.expectEqual(@as(u64, 8), owner.publication_stats.maintenance_readiness_skips - readiness_skips);
    try std.testing.expectEqual(@as(usize, 0), owner.current.?.embedded_tables.count());
    try std.testing.expect(owner.test_runtime_snapshot_opens - opened <= 1);
    try std.testing.expect(try owner.current.?.db.core.loadAppliedSequence(alloc, "text") >= target);
    // With no source debt, an expired audit still services index/WAL lifecycle.
    const key: Owner.MaintenanceKey = .{ .table_id = owner.current.?.db.core.identity_namespace.table_id, .incarnation = owner.current.?.db.root_incarnation };
    owner.maintenance_schedules.getPtr(key).?.next_database_audit_ns = 0;
    const before_audit = owner.publication_stats.maintenance_database_turns;
    _ = try owner.maintenanceTurnForTest();
    try std.testing.expectEqual(before_audit + 1, owner.publication_stats.maintenance_database_turns);
    // Catalog edits without a replay append also invalidate an idle hint.
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const current = try owner.refresh();
    const table_id = current.embedded_table_records.get("idle-0").?;
    const namespace = try @import("../storage/lite/embedded_catalog.zig").namespaceAlloc(alloc, table_id);
    defer alloc.free(namespace);
    try std.testing.expect(try owner.namespaceCanSleep(current, namespace, table_id));
    const incarnation = (try current.owned_lite_backend.?.existingRootIncarnation(namespace)).?;
    const idle_schedule = owner.maintenance_schedules.getPtr(.{ .table_id = table_id, .incarnation = incarnation }).?;
    idle_schedule.has_group_sources = true;
    idle_schedule.next_group_source_ns = 0;
    try std.testing.expect(!try owner.namespaceCanSleep(current, namespace, table_id));
    idle_schedule.next_group_source_ns = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + std.time.ns_per_hour;
    try std.testing.expect(try owner.namespaceCanSleep(current, namespace, table_id));
    idle_schedule.has_group_sources = false;
    const prefix = try current.owned_lite_backend.?.documentPrefixAlloc(alloc, namespace);
    defer alloc.free(prefix);
    const schema_key = try std.mem.concat(alloc, u8, &.{ prefix, keys.public_schema_json_key });
    defer alloc.free(schema_key);
    var schema_write = try current.owned_lite_backend.?.native_docstore.?.beginWrite();
    errdefer schema_write.abort();
    try schema_write.put(schema_key, "{\"type\":\"object\",\"properties\":{\"new_field\":{\"type\":\"string\"}}}");
    try schema_write.commit();
    try std.testing.expect(!try owner.namespaceCanSleep(current, namespace, table_id));
}

test "capi native maintenance namespaces service new unindexed source generations before idle audit" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("unindexed-source-readiness", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    const identity = owner.current.?.db.core.identity_namespace;
    const key: Owner.MaintenanceKey = .{ .table_id = identity.table_id, .incarnation = owner.current.?.db.root_incarnation };
    var warmed = false;
    for (0..16) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.maintenance_schedules.getPtr(key)) |state| {
            if (state.next_database_audit_ns != 0 and owner.maintenance_table_cursor == 0 and !owner.maintenance_storage_pending) {
                warmed = true;
                break;
            }
        }
    }
    try std.testing.expect(warmed);
    const schedule = owner.maintenance_schedules.getPtr(key).?;
    schedule.database_pending = false;
    schedule.next_database_audit_ns = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + std.time.ns_per_hour;
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice("{\"inserts\":{\"new-source\":{\"body\":\"unindexed readiness\"}},\"sync_level\":\"write\"}"), &output));
    const target = owner.current.?.db.core.nextDerivedSequence();
    try std.testing.expect(!owner.current.?.db.core.hasManagedIndexes());
    try std.testing.expect(target != schedule.serviced_sequence);
    const before = owner.publication_stats.maintenance_database_turns;
    for (0..16) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.publication_stats.maintenance_database_turns != before) break;
    }
    try std.testing.expectEqual(before + 1, owner.publication_stats.maintenance_database_turns);
    try std.testing.expectEqual(target, owner.maintenance_schedules.getPtr(key).?.serviced_sequence);
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    // Maintenance publishes through its detached store. Inspect the current
    // physical checkpoint rather than the foreground runtime's old header.
    var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), fixture.path, .{ .read_only = true, .internal_reader = true });
    defer probe.close();
    const debt = (try probe.replayAdmission()).?;
    try std.testing.expectEqual(@as(u64, 0), debt.records);
}

test "capi native maintenance namespaces artifact continuation does not restart source replay" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("artifact-only-source-scheduling", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    const owner = fixture.connection.owner;
    const key: Owner.MaintenanceKey = .{ .table_id = owner.current.?.db.core.identity_namespace.table_id, .incarnation = owner.current.?.db.root_incarnation };
    var ready = false;
    for (0..64) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.maintenance_schedules.getPtr(key)) |schedule| {
            if (!schedule.database_pending and schedule.next_database_audit_ns != 0 and !owner.maintenance_storage_pending) {
                ready = true;
                break;
            }
        }
    }
    try std.testing.expect(ready);
    const schedule = owner.maintenance_schedules.getPtr(key).?;
    schedule.next_database_audit_ns = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + std.time.ns_per_hour;
    schedule.artifact_pending = true;
    schedule.next_artifact_ns = 0;
    const sources = owner.publication_stats.maintenance_source_turns;
    const artifacts = owner.publication_stats.maintenance_artifact_only_turns;
    const Probe = struct {
        entered: bool = false,
        has_source_epoch: bool = false,
        fn captured(ptr: *anyopaque, root: *h.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered = true;
            self.has_source_epoch = root.maintenance_publication.?.source_read != null;
        }
    };
    var probe: Probe = .{};
    owner.test_after_capture = .{ .ptr = &probe, .notify = Probe.captured };
    defer owner.test_after_capture = null;
    _ = try owner.maintenanceTurnForTest();
    try std.testing.expect(probe.entered);
    try std.testing.expect(!probe.has_source_epoch);
    try std.testing.expectEqual(sources, owner.publication_stats.maintenance_source_turns);
    try std.testing.expectEqual(artifacts + 1, owner.publication_stats.maintenance_artifact_only_turns);
    try std.testing.expect(!owner.maintenance_schedules.getPtr(key).?.database_pending);
}

test "capi native maintenance namespaces source capture does not borrow an artifact runtime" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("source-artifact-capture-boundary", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const foreground = owner.current.?;
    try foreground.owned_lite_backend.?.native_docstore.?.file.putIndexCatalogRecord("/source-capture/input", "original");
    const artifacts = try owner.refreshMaintenanceSnapshot(foreground, .artifacts);
    owner.maintenance_retained_logical = artifacts.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().logical;
    const opened = owner.test_runtime_snapshot_opens;
    try std.testing.expectEqual(artifacts, try owner.refreshMaintenanceSnapshot(foreground, .artifacts));
    try std.testing.expectEqual(opened, owner.test_runtime_snapshot_opens);
    // Matching immutable inputs authorize continuation only in the artifact
    // lane. Replay requires its own runtime, source caches and read lifetime.
    const source = try owner.refreshMaintenanceSnapshot(foreground, .source);
    try std.testing.expect(source.db_live);
    try std.testing.expectEqual(opened + 1, owner.test_runtime_snapshot_opens);
}

test "capi native publication generated consumer uses live reserve at producer quota" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("generated-consumer-live-reserve", 1000, .batch);
    defer fixture.deinit();
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    // An unindexed fixture retires its journal at commit. Retain real index
    // consumer debt so this test actually reaches the producer quota.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    for (0..16) |i| {
        var json_buffer: [256]u8 = undefined;
        const json = try std.fmt.bufPrint(&json_buffer, "{{\"inserts\":{{\"source-{d}\":{{\"text\":\"source\"}}}},\"sync_level\":\"write\"}}", .{i});
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice(json), &output));
        api.antfly_buffer_free_zero(&output);
    }
    const owner = fixture.connection.owner;
    var lease: ?native.PathWriterLock = try fixture.connection.acquire();
    var gate_held = true;
    defer if (gate_held) {
        if (lease) |*held| held.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    };
    const foreground = owner.current.?;
    const file = &foreground.owned_lite_backend.?.native_docstore.?.file;
    var debt = (try file.replayAdmission()).?;
    try std.testing.expect(debt.records >= 16);
    debt.limits.records = debt.records;
    try file.putDocument(native.replay_admission.key, &debt.encode());
    try std.testing.expectError(error.LiteReplayBacklogExceeded, file.requireReplayProducerCapacity());
    const root = try owner.refreshMaintenanceSnapshot(foreground, .source);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var proof = try PublicationCatalog.captureForStoragePublication(arena.allocator(), root, &root.db, "default");
    defer proof.deinit();
    var publication: PreparedPublication = .{
        .owner = owner,
        .name = "default",
        .identity = root.db.core.identity_namespace,
        .incarnation = root.db.root_incarnation,
        .catalog = &proof,
        .index_prefix = "unused/",
        .snapshot_root = root,
        .snapshot_database = &root.db,
        .source_read = null,
        .relational_rows = false,
    };
    const batch: GeneratedBatch = .{ .documents = &.{.{ .key = "generated", .cleaned_value = "{\"text\":\"consumer output\"}" }} };
    // The same append without a consumer scope cannot exceed producer limits.
    try std.testing.expectError(error.LiteReplayBacklogExceeded, foreground.db.publishPreparedGeneratedBatch(batch, &.{}, &.{}, &.{}, null));
    lease.?.close();
    lease = null;
    fixture.connection.gate.mutex.unlock(h.handleLockIo());
    gate_held = false;
    const committed = try PreparedPublication.commit(&publication, batch, &.{}, &.{}, &.{}, null);
    try std.testing.expect(committed.sequence != 0);
    lease = try fixture.connection.acquire();
    gate_held = true;
    try std.testing.expect(!owner.current.?.owned_lite_backend.?.native_docstore.?.file.replay_consumer_admission);
    const live = try owner.refresh();
    const live_file = &live.owned_lite_backend.?.native_docstore.?.file;
    try std.testing.expect(!live_file.replay_consumer_admission);
    const after = (try live_file.replayAdmission()).?;
    try std.testing.expect(after.records <= after.limits.records + after.limits.records / 2);
    try std.testing.expectEqual(committed.sequence, root.db.core.nextDerivedSequence());
}

test "capi native replay admission excludes slow refused preparation from busy wait budget" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("replay-preparation-budget", 500, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice("{\"inserts\":{\"counter\":{\"count\":0}},\"sync_level\":\"write\"}"), &output));
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const file = &fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
        var state = (try file.replayAdmission()).?;
        state.limits.records = state.records + 1;
        try file.putDocument(native.replay_admission.key, &state.encode());
        // Admission succeeds; the actual two-lane append refuses later.
        try file.requireReplayProducerCapacity();
    }
    var owned = try api.batch_api.parseBatchRequest(std.testing.allocator, "{\"transforms\":[{\"key\":\"counter\",\"operations\":[{\"op\":\"$inc\",\"path\":\"count\",\"value\":1}]}],\"sync_level\":\"write\"}");
    defer owned.deinit(std.testing.allocator);
    const Request = struct {
        batch: *@TypeOf(owned),
        attempts: usize = 0,
        fn apply(ptr: *anyopaque, handle: *h.Handle) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.attempts += 1;
            if (self.attempts == 1) try h.handleLockIo().sleep(.fromMilliseconds(750), .awake);
            try handle.database().*.batch(self.batch.req);
        }
    };
    const Consumer = struct {
        fixture: *AdmissionFollowupFixture,
        polls: u64,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.releaseCapacity() catch |err| {
                self.failure = err;
            };
        }
        fn releaseCapacity(self: *@This()) !void {
            const io = h.handleLockIo();
            const limit = std.Io.Clock.awake.now(io).nanoseconds + 3 * std.time.ns_per_s;
            while (api.test_replay_capacity_polls.load(.monotonic) == self.polls) {
                if (std.Io.Clock.awake.now(io).nanoseconds >= limit) return error.CapacityWaitDidNotStart;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            var lease = try self.fixture.connection.acquire();
            defer {
                lease.close();
                self.fixture.connection.gate.mutex.unlock(io);
            }
            const root = try self.fixture.connection.owner.refresh();
            const file = &root.owned_lite_backend.?.native_docstore.?.file;
            var state = (try file.replayAdmission()).?;
            state.limits.records = 4096;
            try file.putDocument(native.replay_admission.key, &state.encode());
        }
    };
    var consumer = Consumer{ .fixture = &fixture, .polls = api.test_replay_capacity_polls.load(.monotonic) };
    const worker = try std.Thread.spawn(.{}, Consumer.run, .{&consumer});
    var joined = false;
    defer if (!joined) worker.join();
    var request = Request{ .batch = &owned };
    const code = api.executeBatchWithReplayAdmissionRetry(fixture.id, &request, Request.apply);
    worker.join();
    joined = true;
    try std.testing.expectEqual(@as(?anyerror, null), consumer.failure);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, code);
    try std.testing.expectEqual(@as(usize, 2), request.attempts);
    try std.testing.expect(fixture.connection.owner.publication_stats.replay_refused_preparation_ns >= 750 * std.time.ns_per_ms);
    const raw = (try fixture.connection.owner.current.?.db.get(std.testing.allocator, "counter")).?;
    defer std.testing.allocator.free(raw);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, raw, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("count").?.integer);
}

test "capi native replay transaction credits follow keys when a prepared value grows" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    for ([_]u8{ 1, 2 }) |decision| {
        var fixture = try AdmissionFollowupFixture.init("replay-intent-key-credits", 1000, .batch);
        defer fixture.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\",\"field\":\"text\"}")));
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
        const Probe = struct {
            fn state(f: *AdmissionFollowupFixture) !native.replay_admission.State {
                var lease = try f.connection.acquire();
                defer {
                    lease.close();
                    f.connection.gate.mutex.unlock(h.handleLockIo());
                }
                const root = try f.connection.owner.refresh();
                return (try root.owned_lite_backend.?.native_docstore.?.file.replayAdmission()).?;
            }
        };
        const before = try Probe.state(&fixture);
        try std.testing.expect(before.records != 0);
        const id: [16]u8 = @splat(0x71);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_begin_transaction_with_id(fixture.id, &id, 1, null, 0));
        var write = h.capi.WriteIntent{ .key = .fromSlice("large"), .value = .fromSlice("{\"text\":\"creditcanary\"}"), .is_delete = false };
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_write_transaction(fixture.id, &id, @ptrCast(&write), 1, null, 0));
        const small = try Probe.state(&fixture);
        try std.testing.expectEqual(before.records + 9, small.records);
        const padding = try a.alloc(u8, 4 * 1024 * 1024);
        defer a.free(padding);
        @memset(padding, 'v');
        const value = try std.fmt.allocPrint(a, "{{\"text\":\"creditcanary\",\"padding\":\"{s}\"}}", .{padding});
        defer a.free(value);
        write.value = .fromSlice(value);
        // The old full-value estimate exceeds the entire replay limit, and
        // the transaction's existing reservation prevents the empty exception.
        try std.testing.expect(value.len * 9 > before.limits.bytes);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_write_transaction(fixture.id, &id, @ptrCast(&write), 1, null, 0));
        const big = try Probe.state(&fixture);
        try std.testing.expectEqual(small.bytes, big.bytes);
        try std.testing.expectEqual(small.records, big.records);
        try std.testing.expect(big.bytes - before.bytes < 64 * 1024);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_intents(fixture.id, &id, decision, 2));
        const after = try Probe.state(&fixture);
        try std.testing.expect(after.bytes <= big.bytes);
        const raw = try fixture.connection.owner.current.?.db.get(a, "large");
        defer if (raw) |bytes| a.free(bytes);
        if (decision == 1) {
            try std.testing.expect(raw != null);
            try std.testing.expect(std.mem.eql(u8, value, raw.?));
        } else {
            try std.testing.expect(raw == null);
            try std.testing.expectEqual(before.bytes, after.bytes);
            try std.testing.expectEqual(before.records, after.records);
        }
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const file = &fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
        const scanned = try file.scanReplayAdmission();
        try std.testing.expectEqual(after.bytes, scanned.bytes);
        try std.testing.expectEqual(after.records, scanned.records);
        try std.testing.expect((try file.check()).valid);
    }
}

test "capi native runnable namespace queue receives batch and SQL replay commits" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    for ([_]AdmissionFollowupOperation{ .batch, .sql }) |operation| {
        var fixture = try AdmissionFollowupFixture.init("committed-runnable-source", 5000, operation);
        defer fixture.deinit();
        const owner = fixture.connection.owner;
        const io = h.handleLockIo();
        while (owner.runnable_sources.pending(io)) _ = owner.runnable_sources.choose(io);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(operation));
        try std.testing.expect(owner.runnable_sources.pending(io));
        // Every ticket must still name this admitted catalog's live identity.
        var found = false;
        while (owner.runnable_sources.pending(io)) {
            if (owner.runnable_sources.choose(io)) |key| {
                try std.testing.expect(owner.maintenance_schedules.contains(key));
                found = true;
            }
        }
        try std.testing.expect(found);
        var output: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&output);
        try std.testing.expect(api.antfly_db_batch_json(fixture.id, .fromSlice("invalid JSON"), &output) != .ok);
        try std.testing.expect(!owner.runnable_sources.pending(io));
    }
}

test "capi native publication uncertainty in a detached writer fences foreground refresh" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("detached-outcome-fence", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const snapshot = try owner.refreshMaintenanceSnapshot(owner.current.?, .source);
    const file = &snapshot.owned_lite_backend.?.native_docstore.?.file;
    file.checkpoint_publication_uncertain = true;
    defer file.checkpoint_publication_uncertain = false;
    try std.testing.expect(!Owner.needsOutcomeReconciliation(owner.current.?));
    try std.testing.expectError(error.OutcomeUnknown, owner.refresh());
}

test "capi native publication prepared generated output reuses checked captured writer" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("captured-generated-writer", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    var lease: ?native.PathWriterLock = try fixture.connection.acquire();
    var gate_held = true;
    defer if (gate_held) {
        if (lease) |*held| held.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    };
    const root = try owner.refreshMaintenanceSnapshot(owner.current.?, .source);
    try root.db.reconfigureEnrichmentRuntimePaused(.{ .enable_without_producers = true });
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var proof = try PublicationCatalog.captureForStoragePublication(arena.allocator(), root, &root.db, "default");
    defer proof.deinit();
    var publication: PreparedPublication = .{ .owner = owner, .name = "default", .identity = root.db.core.identity_namespace, .incarnation = root.db.root_incarnation, .catalog = &proof, .index_prefix = "unused/", .snapshot_root = root, .snapshot_database = &root.db, .source_read = null, .relational_rows = false };
    const store = root.owned_lite_backend.?.native_docstore.?;
    root.maintenance_references += 1;
    root.maintenance_owner = owner;
    root.maintenance_publication = &publication;
    store.writer_admission = .{ .ptr = root, .enter = Owner.enterMaintenanceWriter, .leave = Owner.leaveMaintenanceWriter };
    defer {
        store.writer_admission = null;
        root.maintenance_publication = null;
        root.maintenance_owner = null;
        root.maintenance_references -= 1;
    }
    store.file.replay_consumer_admission = true;
    lease.?.close();
    lease = null;
    fixture.connection.gate.mutex.unlock(h.handleLockIo());
    gate_held = false;
    for (0..3) |i| {
        // Foreground commits move the durable replay head after capture.
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
        const foreground_sequence = owner.current.?.db.core.nextDerivedSequence();
        var name: [32]u8 = undefined;
        var staged: [32]u8 = undefined;
        const staged_key = try std.fmt.bufPrint(&staged, "prepared-stage-{d}", .{i});
        try root.db.core.store.put(staged_key, "{\"text\":\"checked output\"}");
        const promotions: Promotions = &.{.{ .staged_key = staged_key, .final_key = try std.fmt.bufPrint(&name, "prepared-{d}", .{i}) }};
        const refreshes = owner.publication_stats.serving_runtime_refreshes;
        const committed = try PreparedPublication.commit(&publication, .{}, promotions, &.{}, &.{}, null);
        try std.testing.expectEqual(refreshes, owner.publication_stats.serving_runtime_refreshes);
        try std.testing.expect(committed.sequence > foreground_sequence);
    }
    try root.db.core.store.put("guarded-native-source", "after");
    var expected: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("before", &expected, .{});
    const guards: SourceGuards = &.{.{ .key = "guarded-native-source", .expected_digest = expected }};
    try std.testing.expectError(error.EnrichmentWaitCanceled, PreparedPublication.commit(&publication, .{}, &.{}, &.{}, guards, null));
    lease = try fixture.connection.acquire();
    gate_held = true;
    try std.testing.expectEqual(@as(u64, 3), owner.publication_stats.generated_captured_commits);
    const live = try owner.refresh();
    for (0..3) |i| {
        var name: [32]u8 = undefined;
        const value = try live.db.core.store.get(std.testing.allocator, try std.fmt.bufPrint(&name, "prepared-{d}", .{i}));
        defer std.testing.allocator.free(value);
        try std.testing.expectEqualStrings("{\"text\":\"checked output\"}", value);
    }
}

test "capi native replay publication retains primary epoch after preparing text input" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("replay-journal-epoch-lifetime", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    // Exercise the single-index preparation path which formerly released
    // the primary epoch while its replay cursor still borrowed the journal.
    if (fixture.connection.owner.current.?.db.core.index_manager.text_indexes.items.len == 0)
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    try std.testing.expectEqual(@as(usize, 1), fixture.connection.owner.current.?.db.core.index_manager.text_indexes.items.len);
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice("{\"inserts\":{\"later\":{\"text\":\"journal epoch retention canary\"}},\"sync_level\":\"write\"}"), &output));
    const owner = fixture.connection.owner;
    const Probe = struct {
        publications: usize = 0,
        missing_epoch: bool = false,
        fn notify(ptr: *anyopaque, root: *h.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.publications += 1;
            // The worker still owns its journal cursor here, even though
            // every text input for this chunk has already been prepared.
            self.missing_epoch = self.missing_epoch or root.maintenance_publication.?.source_read == null;
        }
    };
    var probe: Probe = .{};
    owner.test_before_replay_index_publication = .{ .ptr = &probe, .notify = Probe.notify };
    defer owner.test_before_replay_index_publication = null;
    for (0..32) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (probe.publications != 0) break;
    }
    try std.testing.expect(probe.publications != 0);
    try std.testing.expect(!probe.missing_epoch);
}
