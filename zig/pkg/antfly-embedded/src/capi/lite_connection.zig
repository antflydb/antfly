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
const IndexManager = @import("../storage/db/catalog/index_manager.zig").IndexManager;
const segment_mod = @import("../segment.zig");
const persistent_mod = @import("../storage/persistent.zig");
const artifact_source = @import("../storage/lite/artifact_source.zig");
var test_pause_private_merge: std.atomic.Value(bool) = .init(false);
var test_private_merge_entered: std.atomic.Value(bool) = .init(false);
var test_private_merge_budget_failures: std.atomic.Value(u32) = .init(0);

/// One immutable construction per file. The old runtime owns input readers and
/// resource accounting, but no primary epoch or catalog read pin spans the job.
/// Its worker never accesses mutable DB, scheduler, catalog, or logical IDs.
const PendingTextMerge = struct {
    owner: *Owner,
    root: *h.Handle,
    database: *h.db_mod.DB,
    arena: std.heap.ArenaAllocator,
    catalog: PublicationCatalog,
    name: []const u8,
    task: IndexManager.TextMergeTask,
    scratch: segment_mod.MergeScratchOptions,
    config_hash: u64,
    input_uses: std.ArrayListUnmanaged(segment_mod.SegmentSource) = .empty,
    thread: ?std.Thread = null,
    canceled: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    failure: ?anyerror = null,
    result: ?segment_mod.PrivateMergeResult = null,
    publication: ?IndexManager.TextMergeResult = null,
    publication_root: ?*h.Handle = null,
    lookup_needed: bool = false,
    key_range: ?persistent_mod.SegmentKeyRange = null,
    construction: ?artifact_source.Construction = null,
    ready: bool = false,
    applied: bool = false,
    elapsed_ns: u64 = 0,
    owns_root: bool = false,

    fn run(self: *@This()) void {
        self.prepare() catch |err| {
            self.failure = err;
        };
        self.done.store(true, .release);
        self.owner.wake.set(h.handleLockIo());
    }

    fn prepare(self: *@This()) !void {
        const started = @import("antfly_platform").time.monotonicNs();
        defer self.elapsed_ns = @import("antfly_platform").time.monotonicNs() -| started;
        if (comptime h.builtin.is_test) {
            test_private_merge_entered.store(true, .release);
            while (test_pause_private_merge.load(.acquire)) {
                if (self.canceled.load(.acquire)) return error.Canceled;
                try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
            }
            if (test_private_merge_budget_failures.swap(0, .acq_rel) != 0) return error.ResourceBudgetExceeded;
        }
        var result = try IndexManager.executePrivateTextMergeTask(&self.task, self.scratch, &self.canceled);
        errdefer result.deinit();
        if (result.run) |output| {
            self.key_range = try persistent_mod.PersistentIndex.privateMergeKeyRange(self.task.mergeAllocator(), (try output.sealedView()).source, &self.canceled);
        }
        if (self.canceled.load(.acquire)) return error.Canceled;
        self.result = result;
    }

    fn deinit(self: *@This()) void {
        self.canceled.store(true, .release);
        if (self.thread) |thread| thread.join();
        if (self.construction) |*construction| construction.deinit();
        if (self.key_range) |*range| range.deinit(self.task.mergeAllocator());
        if (self.publication) |*publication| publication.deinit(alloc);
        if (self.publication_root) |root| {
            self.owner.gate.mutex.lockUncancelable(h.handleLockIo());
            std.debug.assert(root.maintenance_references != 0);
            root.maintenance_references -= 1;
            self.owner.gate.mutex.unlock(h.handleLockIo());
            Owner.closeMaintenanceSnapshot(root);
        }
        if (self.result) |*result| result.deinit();
        for (self.input_uses.items) |source| source.releaseUse();
        self.input_uses.deinit(alloc);
        self.database.core.index_manager.cancelTextMergeTask(&self.task);
        self.task.deinit(alloc);
        self.arena.deinit();
        if (self.owns_root) Owner.closeMaintenanceSnapshot(self.root);
        alloc.destroy(self);
    }
};

/// A producer may await a model or remote provider for much longer than a
/// replay quantum. It owns a separate actor and stable publication frame, so
/// consumer replay and retirement never have to await that provider future.
const PendingProducer = struct {
    owner: *Owner,
    root: *h.Handle,
    database: *h.db_mod.DB,
    parent_state: *h.db_mod.DB.GroupSourceMaintenanceState,
    state: h.db_mod.DB.GroupSourceMaintenanceState = .{},
    arena: *std.heap.ArenaAllocator,
    catalog: PublicationCatalog,
    publication: PreparedPublication,
    source_read: ?SourceRead,
    progress: Owner.ReplayProgress,
    done: std.atomic.Value(bool) = .init(false),
    failure: ?anyerror = null,
    elapsed_ns: u64 = 0,
    advanced: bool = false,

    fn execute(self: *PendingProducer) void {
        const started = @import("antfly_platform").time.monotonicNs();
        const before = self.database.core.nextDerivedSequence();
        const applied = if (self.database.enrichment_runtime) |runtime| runtime.stats().applied_sequence else 0;
        if (comptime h.builtin.is_test) if (self.owner.test_after_capture) |hook| hook.notify(hook.ptr, self.root);
        if (comptime h.builtin.is_test) if (self.owner.test_before_producer_service) |hook| hook.run(hook.ptr, self);
        _ = self.database.runBackgroundTurnWithCancellation(h.db_mod.types.CancellationToken.fromAtomic(&self.owner.stopping), .producers) catch |err| failed: {
            self.failure = err;
            break :failed false;
        };
        self.advanced = self.database.core.nextDerivedSequence() > before or
            (if (self.database.enrichment_runtime) |runtime| runtime.stats().applied_sequence > applied else false) or
            self.progress.advanced(self.database);
        self.elapsed_ns = @import("antfly_platform").time.monotonicNs() -| started;
    }

    fn deinit(self: *PendingProducer) void {
        std.debug.assert(self.done.load(.acquire));
        if (self.database.enrichment_runtime) |runtime| {
            runtime.dense_result_cache = null;
            runtime.generated_publication = null;
        }
        self.database.async_context.replay_index_publication = null;
        self.database.async_context.replay_metadata_publication = null;
        self.database.async_context.replay_publication = null;
        self.database.borrowed_group_source_state = null;
        if (self.source_read) |*read| read.abort();
        self.source_read = null;
        self.root.maintenance_publication = null;
        self.root.owned_lite_backend.?.native_docstore.?.writer_admission = null;
        self.root.maintenance_owner = null;
        self.state.deinit();
        Owner.closeMaintenanceSnapshot(self.root);
        self.catalog.deinit();
        self.arena.deinit();
        alloc.destroy(self.arena);
        alloc.destroy(self);
    }
};

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
        &keys.identity_namespace_key,
        @import("../storage/db/table_catalog.zig").key,
        &keys.table_storage_settings_key,
        @import("../storage/db/relational_index_catalog.zig").head_key,
        @import("../storage/db/relational_integrity_catalog.zig").key,
        @import("../storage/db/row_policy_bundle.zig").key,
        @import("../storage/db/row_policy_bundle.zig").candidate_key,
        @import("../storage/db/row_policy_bundle.zig").pending_key,
        @import("../storage/db/relational_initial_child_publication.zig").key,
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
        if (!std.mem.eql(u8, &self.metadata_identity, &file.activeCheckpoint().logical.metadata)) return error.EnrichmentSourceChanged;
        try self.verifyContentsNative(file);
    }

    fn verifyContentsNative(self: *PublicationCatalog, file: *native.NativeFile) !void {
        if ((try file.file.stat(file.runtime())).inode != self.inode) return error.EnrichmentSourceChanged;
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

const TextRuntimeCacheDiagnostics = struct {
    capture_attempts: u64 = 0,
    captured: u64 = 0,
    capture_namespace_limit: u64 = 0,
    capture_control: u64 = 0,
    miss_no_cache: u64 = 0,
    miss_closed: u64 = 0,
    miss_inode: u64 = 0,
    miss_binding: u64 = 0,
    miss_borrow: u64 = 0,
    miss_namespace: u64 = 0,
    miss_indexes: u64 = 0,
    miss_metadata: u64 = 0,
    miss_config: u64 = 0,
    miss_control: u64 = 0,
    not_retained_settled: u64 = 0,
    not_retained_failed: u64 = 0,
    not_retained_index_inputs: u64 = 0,
    not_retained_storage_error: u64 = 0,
    retained_storage_yield: u64 = 0,
    immutable_yield_handoffs: u64 = 0,
    blocked_manual: u64 = 0,
    blocked_enrichment: u64 = 0,
    blocked_queries: u64 = 0,
    blocked_policy: u64 = 0,
    blocked_transition: u64 = 0,
    blocked_index_loads: u64 = 0,
    blocked_admission: u64 = 0,
    blocked_resolver: u64 = 0,
    blocked_split: u64 = 0,
    blocked_groups: u64 = 0,
    blocked_nontext_indexes: u64 = 0,
    blocked_source_epochs: u64 = 0,
    blocked_replacement: u64 = 0,
    blocked_repairs: u64 = 0,
    blocked_coalescer: u64 = 0,

    fn reject(self: *@This(), comptime reason: []const u8) bool {
        @field(self, reason) +|= 1;
        return false;
    }

    fn eligible(self: *@This(), db: *h.db_mod.DB) bool {
        const reason = db.detachedTextSourceBlock() orelse return true;
        switch (reason) {
            inline else => |tag| @field(self, "blocked_" ++ @tagName(tag)) +|= 1,
        }
        return false;
    }
};

/// Owns only configuration witnesses and immutable text runtimes. A source
/// read is acquired separately for each bounded turn and released before this
/// cache becomes idle. A bounded namespace working set avoids reopening every
/// index whenever source tickets or control audits rotate between tables.
const TextSourceRuntimeCache = struct {
    const max_namespaces = 8;
    const Control = struct { path: []const u8, fingerprint: ?[32]u8 };
    arena: std.heap.ArenaAllocator,
    root: *h.Handle,
    indexes: [16]u8,
    proofs: []PublicationCatalog,
    table_names: []const []const u8 = &.{},
    controls: []Control = &.{},

    fn capture(root: *h.Handle) !?*TextSourceRuntimeCache {
        var ignored: TextRuntimeCacheDiagnostics = .{};
        return captureObserved(root, &ignored);
    }

    fn captureObserved(root: *h.Handle, stats: *TextRuntimeCacheDiagnostics) !?*TextSourceRuntimeCache {
        stats.capture_attempts +|= 1;
        if (!stats.eligible(&root.db)) return null;
        if (root.embedded_tables.count() > max_namespaces) {
            _ = stats.reject("capture_namespace_limit");
            return null;
        }
        var tables = root.embedded_tables.valueIterator();
        while (tables.next()) |table| if (!stats.eligible(&table.*.db)) return null;
        const self = try alloc.create(TextSourceRuntimeCache);
        self.* = .{ .arena = .init(alloc), .root = root, .indexes = undefined, .proofs = &.{} };
        errdefer self.destroy();
        const a = self.arena.allocator();
        self.proofs = try a.alloc(PublicationCatalog, 1 + root.embedded_tables.count());
        const table_names = try a.alloc([]const u8, root.embedded_tables.count());
        self.table_names = table_names;
        self.proofs[0] = try PublicationCatalog.captureForStoragePublication(a, root, &root.db, "default");
        var controls: std.ArrayList(Control) = .empty;
        if (!try self.captureControls(a, &root.db, &controls)) {
            _ = stats.reject("capture_control");
            self.destroy();
            return null;
        }
        tables = root.embedded_tables.valueIterator();
        var i: usize = 1;
        while (tables.next()) |table| : (i += 1) {
            table_names[i - 1] = try a.dupe(u8, table.*.name);
            self.proofs[i] = try PublicationCatalog.captureForStoragePublication(a, root, &table.*.db, table.*.name);
            if (!try self.captureControls(a, &table.*.db, &controls)) {
                _ = stats.reject("capture_control");
                self.destroy();
                return null;
            }
        }
        self.controls = try controls.toOwnedSlice(a);
        self.indexes = root.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().logical.indexes;
        stats.captured +|= 1;
        return self;
    }

    fn captureControls(self: *TextSourceRuntimeCache, a: std.mem.Allocator, database: *h.db_mod.DB, controls: *std.ArrayList(Control)) !bool {
        const backend = &self.root.owned_lite_backend.?;
        const prefix = if (database == &self.root.db) "" else try backend.documentPrefixAlloc(a, try @import("../storage/lite/embedded_catalog.zig").namespaceAlloc(a, database.core.identity_namespace.table_id));
        const logical_keys = try database.core.index_manager.retainedTextRuntimeControlKeysAlloc(a);
        var read = try backend.native_docstore.?.beginRead();
        defer read.abort();
        const admission_prefix = keys.globalReplayPrefix(.managed_admission);
        for (logical_keys) |key| {
            const path = try std.mem.concat(a, u8, &.{ prefix, key });
            const fingerprint = try read.valueFingerprint(path);
            if (fingerprint != null and std.mem.startsWith(u8, key, &admission_prefix)) return false;
            try controls.append(a, .{ .path = path, .fingerprint = fingerprint });
        }
        return true;
    }

    fn destroy(self: *TextSourceRuntimeCache) void {
        self.arena.deinit();
        alloc.destroy(self);
    }

    fn matches(self: *TextSourceRuntimeCache, file: *native.NativeFile) !bool {
        var ignored: TextRuntimeCacheDiagnostics = .{};
        return self.matchesObserved(file, &ignored);
    }

    fn matchesObserved(self: *TextSourceRuntimeCache, file: *native.NativeFile, stats: *TextRuntimeCacheDiagnostics) !bool {
        if (!self.root.db_live) return stats.reject("miss_closed");
        if (self.root.maintenance_references != 0 or self.root.query_borrows != 0 or self.root.sql_cursors.count() != 0) return stats.reject("miss_borrow");
        if (!file.header.indexed_reclamation or std.mem.allEqual(u8, &self.indexes, 0) or !std.mem.eql(u8, &self.indexes, &file.activeCheckpoint().logical.indexes)) return stats.reject("miss_indexes");
        if (self.root.embedded_tables.count() + 1 != self.proofs.len) return stats.reject("miss_namespace");
        for (self.table_names) |name| if (!self.root.embedded_tables.contains(name)) return stats.reject("miss_namespace");
        if (!stats.eligible(&self.root.db)) return false;
        var tables = self.root.embedded_tables.valueIterator();
        while (tables.next()) |table| if (!stats.eligible(&table.*.db)) return false;
        for (self.proofs) |*proof| {
            if (!std.mem.eql(u8, &proof.metadata_identity, &file.activeCheckpoint().logical.metadata)) return stats.reject("miss_metadata");
            proof.verifyNative(file) catch |err| switch (err) {
                error.EnrichmentSourceChanged, error.EnrichmentWaitCanceled => return stats.reject("miss_config"),
                else => return err,
            };
        }
        for (self.controls) |control| if (!std.meta.eql(control.fingerprint, try file.documentFingerprintAtCheckpoint(file.activeCheckpoint(), control.path))) return stats.reject("miss_control");
        return true;
    }
};

const ImmutableTextHandoff = struct {
    const Pins = @import("immutable_text_handoff.zig").Handoff(@import("../index.zig").IndexSnapshot, @import("../index.zig").SegmentEntry.ImmutableLease);
    const max_bytes = 256 * 1024 * 1024;
    const max_segments = 1024;
    const lifetime_ns = 5 * std.time.ns_per_s;
    pins: Pins = .{ .allocator = alloc },
    resource_owner: *artifact_source.Registry,
    table_id: u64,
    incarnation: @FieldType(h.db_mod.DB, "root_incarnation"),
    expires_ns: u64,
    generation_bytes: u64,

    fn capture(root: *h.Handle, database: *h.db_mod.DB) !?ImmutableTextHandoff {
        const backend = &root.owned_lite_backend.?;
        const manager = backend.owned_resource_manager orelse return null;
        // A borrowed/external resource manager needs its own lifetime
        // capability. Keep those runtimes on the ordinary cold path.
        if (database.core.index_manager.resource_manager != manager or database.core.index_manager.owned_resource_manager != null) return null;
        const store = backend.native_docstore.?;
        // Only one handoff exists per file. Its ranges cannot exceed one
        // captured file generation, independently of the resident-memory cap.
        const generation_bytes = std.math.mul(u64, store.file.activeCheckpoint().page_count, store.file.header.page_size) catch return null;
        const resource_owner = store.artifact_registry orelse return null;
        resource_owner.retainOwner();
        var result: ImmutableTextHandoff = .{
            .resource_owner = resource_owner,
            .table_id = database.core.identity_namespace.table_id,
            .incarnation = database.root_incarnation,
            .expires_ns = @import("antfly_platform").time.monotonicNs() +| lifetime_ns,
            .generation_bytes = generation_bytes,
        };
        errdefer result.deinit();
        const indexes = database.core.index_manager;
        indexes.catalog_mutex.lockShared();
        defer indexes.catalog_mutex.unlockShared();
        for (indexes.text_indexes.items, 0..) |*entry, ordinal| {
            if (ordinal == 128 or result.pins.visited == max_segments or result.pins.bytes == max_bytes) break;
            _ = try result.pins.appendWithSourceLimit(entry.persistent.acquireSnapshot(), max_bytes, max_segments, @min(generation_bytes, artifact_source.WorkingSet.max_range_bytes));
        }
        if (result.pins.leases.items.len == 0) {
            result.deinit();
            return null;
        }
        return result;
    }

    fn deinit(self: *ImmutableTextHandoff) void {
        // The range may belong to an older pooled registry. Decoder memory
        // still belongs to this runtime's manager, including on second and
        // later handoffs after its DB and backend have already closed.
        self.pins.deinit();
        self.resource_owner.releaseOwner();
    }
};

// Completed producer bytes are delivered to the current runtime, rather than
// replaying inference whenever an unrelated primary commit moves the file tip.
pub const ShadowScope = struct { name: []const u8, generation: u64, prefix: []const u8 };

pub const PreparedPublication = struct {
    const IndexScope = struct {
        name: []const u8,
        domain: []const u8,
        identity: ?[16]u8,
        valid: bool = true,
    };
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
    // Owned by the turn arena; capture happens before releasing admission.
    index_scopes: []IndexScope = &.{},
    admitted_index_scope: ?usize = null,
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
            try self.validateNativeStorage(proof, probe);
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

    fn validateNativeStorage(self: *@This(), proof: *PublicationCatalog, file: *native.NativeFile) !void {
        if (self.snapshot_database.root_incarnation != self.incarnation or
            !std.meta.eql(self.snapshot_database.core.identity_namespace, self.identity)) return error.EnrichmentSourceChanged;
        // Check loaded name bindings without opening a runtime. The native
        // proof independently fences the durable catalog on this same cut.
        if (self.owner.current) |root| if (!std.mem.eql(u8, self.name, "default")) {
            if (root.embedded_table_records.get(self.name)) |id|
                if (id != self.identity.table_id) return error.EnrichmentSourceChanged;
        };
        try proof.verifyNative(file);
        self.owner.publication_stats.storage_guard_validations +|= 1;
    }

    fn inspectCapturedNativeStorage(ptr: *anyopaque, file: *native.NativeFile) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const proof = self.catalog orelse return false;
        if (!proof.recovery_clean or !file.header.indexed_reclamation) return false;
        try self.validateNativeStorage(proof, file);
        return true;
    }

    fn validateCapturedStorageCurrent(self: *@This()) !void {
        const store = self.snapshot_root.owned_lite_backend.?.native_docstore.?;
        if (self.shadow_scopes.len == 0 and self.resolver_dependency == null and
            try store.inspectAdmittedCheckpoint(self, inspectCapturedNativeStorage)) return;
        // Actor/recovery paths can publish or re-enter storage. They must run
        // after releasing the physical inspection mutex.
        try self.validateStorageCurrent(&store.file);
    }

    const CapturedEvent = enum { attempt, refusal, store, commit };

    fn recordCaptured(self: *@This(), event: CapturedEvent) void {
        // Compatible local publishers may share path admission. Their owner
        // counters and wake flags still require explicit mutual exclusion.
        self.snapshot_root.maintenance_admission_mutex.lockUncancelable(h.handleLockIo());
        defer self.snapshot_root.maintenance_admission_mutex.unlock(h.handleLockIo());
        switch (event) {
            .attempt => self.owner.publication_stats.generated_attempts +|= 1,
            .refusal => {
                self.owner.publication_stats.refused +|= 1;
                self.owner.publication_stats.generated_refused +|= 1;
            },
            .store => self.owner.publication_stats.generated_storage_groups +|= 1,
            .commit => {
                self.owner.publication_stats.generated_captured_commits +|= 1;
                self.owner.notifyMutation();
            },
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
                errdefer self.recordCaptured(.refusal);
                const database = self.snapshot_database;
                const store = self.snapshot_root.owned_lite_backend.?.native_docstore.?;
                // Admission rebases physical roots, while the DocStore owns
                // separate committed/reserved sequence caches. Reconcile them
                // under this same writer lease before allocating a sequence.
                {
                    var head_read = try database.core.store.beginReadTxn();
                    defer head_read.abort();
                    const committed = try database.core.store.lastReplaySequenceFromTxn(&head_read, 0);
                    database.core.store.observeExternalReplayCommit(committed);
                }
                const index_revision = store.admittedIndexPathRevision(self.index_prefix);
                const result = database.enqueuePreparedGeneratedBatch(batch, promotions, deletes, guards, fence) catch |err| switch (err) {
                    error.EnrichmentSourceChanged, error.EnrichmentLeaseFenceLost => return error.EnrichmentWaitCanceled,
                    else => return err,
                };
                // Reusing the writer must not erase evidence of its own index
                // mutations. Local path revisions ignore unrelated prefixes,
                // while failed revision capture conservatively fences indexes.
                if (index_revision) |before| {
                    if (store.admittedIndexPathRevision(self.index_prefix)) |after| {
                        if (!std.mem.eql(u8, &before, &after)) self.index_inputs_changed = true;
                    } else self.index_inputs_changed = true;
                } else self.index_inputs_changed = true;
                self.snapshot_database.core.store.observeExternalReplayCommit(result.sequence);
                self.recordCaptured(.commit);
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
        const held_from = nowNs();
        defer MaintenanceHoldSites.record(&self.owner.publication_stats.maintenance_hold_sites.generated_commit, held_from);
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

    pub fn publishPromotions(self: *@This(), allocator: std.mem.Allocator, entries: []const @import("../storage/db/runtime_callbacks.zig").EntityUpsert) !void {
        const io = h.handleLockIo();
        self.owner.gate.mutex.lockUncancelable(io);
        defer self.owner.gate.mutex.unlock(io);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
        var lease = try native.lockWriterPathWithIo(alloc, io, self.owner.path);
        defer lease.close();
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
        path_lease: ?native.PathWriterLock = null,
        native_storage_admission: bool = false,
        database: *h.db_mod.DB,
        refusal_recorded: bool = false,
        held_from: i96 = 0,

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
                MaintenanceHoldSites.record(&p.owner.publication_stats.maintenance_hold_sites.generated_store, held.held_from);
                p.owner.gate.mutex.unlock(h.handleLockIo());
            }
            alloc.destroy(held);
        }

        fn guardKey(ptr: *anyopaque, key: []const u8) !void {
            const held: *@This() = @ptrCast(@alignCast(ptr));
            errdefer {
                if (!held.refusal_recorded) {
                    held.refusal_recorded = true;
                    held.publication.recordCaptured(.refusal);
                }
            }
            try held.publication.guardKey(held.database, key);
        }
    };

    fn findIndexScope(self: *@This(), name: []const u8) ?usize {
        if (self.shadow_scopes.len != 0 or self.resolver_dependency != null or !(self.catalog orelse return null).recovery_clean) return null;
        for (self.index_scopes, 0..) |scope, i| if (scope.valid and std.mem.eql(u8, scope.name, name)) return i;
        return null;
    }

    fn indexScopeUnchanged(self: *@This(), i: usize, latest: *native.NativeFile) !bool {
        const scope = self.index_scopes[i];
        return scope.valid and latest.header.indexed_reclamation and latest.header.indexed_revision >= 12 and
            std.meta.eql(scope.identity, try latest.indexScopeIdentity(scope.domain));
    }

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

    /// The admitted native cut is checked against owned metadata fingerprints.
    /// No extra root/read snapshot survives a provider wait or artifact turn.
    fn requireCurrentCatalog(self: *@This()) !void {
        const proof = self.catalog orelse return error.EnrichmentWaitCanceled;
        const file = &self.snapshot_root.owned_lite_backend.?.native_docstore.?.file;
        if (proof.recovery_clean and file.header.indexed_reclamation)
            return self.validateNativeStorage(proof, file);
        try proof.verifyContentsNative(file);
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
    fn submitGroupSource(raw: *anyopaque, command: @import("../storage/db/group_source_contract.zig").Command) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (comptime h.builtin.is_test) if (self.owner.test_before_group_command) |hook| hook.notify(hook.ptr, command);
        // Every command validates its lease/revision and catalog against the
        // admitted primary cut. Discovery/fan-in and sibling commands never
        // borrow this lease. The detached DB's index actors are not authority
        // for a primary-only control publication.
        try Owner.enterMaintenanceWriter(self.snapshot_root, .primary);
        defer Owner.leaveMaintenanceWriter(self.snapshot_root);
        const database = try self.groupCommandDatabase();
        try database.batch(.{ .group_source = command, .sync_level = .write });
        self.owner.notifyMutation();
    }

    fn groupCommandDatabase(self: *@This()) !*h.db_mod.DB {
        const database = self.snapshot_database;
        try self.validateCapturedStorageCurrent();
        try database.core.store.invalidateDetachedSourceObservations();
        var read = try database.core.store.beginReadTxn();
        const sequence = database.core.store.lastReplaySequenceFromTxn(&read, 0) catch |err| {
            read.abort();
            return err;
        };
        read.abort();
        database.core.store.observeExternalReplayCommit(sequence);
        return database;
    }

    fn submitGroupSourceControls(raw: *anyopaque, commands: []const @import("../storage/db/group_source_contract.zig").Command) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (comptime h.builtin.is_test) if (self.owner.test_before_group_command) |hook| for (commands) |command| hook.notify(hook.ptr, command);
        try Owner.enterMaintenanceWriter(self.snapshot_root, .primary);
        defer Owner.leaveMaintenanceWriter(self.snapshot_root);
        const database = try self.groupCommandDatabase();
        const file = &self.snapshot_root.owned_lite_backend.?.native_docstore.?.file;
        const before = file.activeCheckpoint().commit_sequence;
        try database.batchGroupSourceControls(commands);
        const commits = file.activeCheckpoint().commit_sequence - before;
        std.debug.assert(commits <= 1);
        self.owner.publication_stats.group_source_control_batches +|= 1;
        self.owner.publication_stats.group_source_control_commands +|= commands.len;
        self.owner.publication_stats.group_source_control_commits +|= commits;
        self.owner.notifyMutation();
    }

    fn acquireCapturedStore(self: *@This(), fence: ?WriteFence, now_ms: u64) !StoreLease {
        try Owner.enterMaintenanceWriter(self.snapshot_root, .primary);
        errdefer Owner.leaveMaintenanceWriter(self.snapshot_root);
        self.recordCaptured(.attempt);
        errdefer self.recordCaptured(.refusal);
        const database = self.snapshot_database;
        try self.validateCapturedStorageCurrent();
        if (fence) |expected| {
            var ownership = try ProducerLease.init(alloc, database.core.store, expected.lease_key);
            defer ownership.deinit();
            const live = (try ownership.load(alloc)) orelse return error.EnrichmentLeaseFenceLost;
            defer alloc.free(live.owner_id);
            if (live.epoch != expected.epoch or live.expires_at_ms <= @max(now_ms, database.backend_runtime.clock().nowRealtimeMs()) or !std.mem.eql(u8, live.owner_id, expected.owner_id)) return error.EnrichmentLeaseFenceLost;
        }
        const held = try alloc.create(HeldStore);
        held.* = .{ .publication = self, .database = database, .native_storage_admission = true };
        self.recordCaptured(.store);
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
        const held_from = nowNs();
        errdefer lease.close();
        errdefer MaintenanceHoldSites.record(&self.owner.publication_stats.maintenance_hold_sites.generated_store, held_from);
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
        held.* = .{ .publication = self, .path_lease = lease, .database = database, .held_from = held_from };
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
const QueuedMutex = @import("admission_mutex.zig").QueuedMutexWithLease(h.handleLockIo, native.PathWriterLock);

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

test "capi path gate timed out handoff closes the retained safety lease" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const io = h.handleLockIo();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/timed-handoff.aflite", .{tmp.sub_path});
    defer alloc.free(path);
    try native.create(io, path);
    var gate: QueuedMutex = .{};
    gate.lockUncancelable(io);
    var gate_held = true;
    defer if (gate_held) gate.unlock(io);
    var lease = try native.tryFairWriterLockWithIo(alloc, io, path);
    var lease_owned = true;
    defer if (lease_owned) lease.close();
    const Actor = struct {
        gate: *QueuedMutex,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.gate.lockForLeaseUntil(h.handleLockIo(), std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + 250 * std.time.ns_per_ms) catch |err| {
                self.failure = err;
                return;
            };
            self.gate.unlock(h.handleLockIo());
        }
    };
    var actor: Actor = .{ .gate = &gate };
    const thread = try std.Thread.spawn(.{}, Actor.run, .{&actor});
    var joined = false;
    defer if (!joined) thread.join();
    const deadline = std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s;
    while (true) {
        gate.state.lockUncancelable(io);
        const queued = gate.head != null;
        gate.state.unlock(io);
        if (queued) break;
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline) return error.TestWatchdog;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(gate.retainForWaiter(io, &lease));
    lease_owned = false;
    thread.join();
    joined = true;
    try std.testing.expectEqual(@as(?anyerror, error.FileBusy), actor.failure);
    gate.unlock(io);
    gate_held = false;
    try std.testing.expect(gate.retained == null);
    var next = try native.tryFairWriterLockWithIo(alloc, io, path);
    next.close();
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

    // Nobody queued: the lease is not kept.
    gate.lockUncancelable(io);
    var lone = try native.fairWriterLockWithIo(alloc, io, threaded.io(), path, std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s);
    try std.testing.expect(!gate.retainForWaiter(io, &lone));
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
        try std.testing.expect(!gate.retainForWaiter(io, &lease));
        lease.close();
        gate.unlock(io);
        plain.join();
    }

    const Waiter = struct {
        gate: *QueuedMutex,
        took: bool = false,
        fn run(self: *@This()) void {
            const wio = h.handleLockIo();
            self.gate.lockForLease(wio);
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
    var waiter: Waiter = .{ .gate = &gate };
    const thread = try std.Thread.spawn(.{}, Waiter.run, .{&waiter});
    while (true) {
        gate.state.lockUncancelable(io);
        const queued = gate.next -% gate.serving;
        gate.state.unlock(io);
        if (queued > 1) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(gate.retainForWaiter(io, &lease));
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

// Connections in one process queue on a canonical file gate. The kernel
// sidecar remains the authority between processes and with CLI/server owners.
const PathGate = struct {
    path: []u8,
    mutex: QueuedMutex = .{},
    sql_preparation: @import("sql.zig").PreparationAdmission = .{},
    document_publications: @import("publication_queue.zig").Queue = .{},
    references: usize = 1,
    owners: std.ArrayList(*Owner) = .empty,
    lease_stats: native.PathLeaseStats = .{},
    writer_allocator_cache: native.WriterAllocatorCache = .{},
};
/// A file-lifetime capability, independent of every mutable Owner. Waiting
/// commands borrow neither the registry's Store nor a serving runtime.
const OwnedArtifactGate = struct {
    fn view(gate: *PathGate) @import("../storage/lite/publication_gate.zig").Gate {
        return .{ .ptr = gate, .retain_fn = retain, .release_fn = release, .acquire_fn = acquire };
    }
    fn retain(raw: *anyopaque) void {
        const gate: *PathGate = @ptrCast(@alignCast(raw));
        @import("io_lifetime.zig").acquire();
        registry_mutex.lockUncancelable(h.handleLockIo());
        defer registry_mutex.unlock(h.handleLockIo());
        gate.references += 1;
    }
    fn release(raw: *anyopaque) void {
        const gate: *PathGate = @ptrCast(@alignCast(raw));
        releaseGate(gate);
        @import("io_lifetime.zig").release();
    }
    fn acquire(raw: *anyopaque, lock_io: std.Io, deadline_ns: i96) !@import("../storage/lite/publication_gate.zig").Lease {
        const gate: *PathGate = @ptrCast(@alignCast(raw));
        const io = h.handleLockIo();
        const started = std.Io.Clock.awake.now(io).nanoseconds;
        try gate.mutex.lockForLeaseUntil(io, deadline_ns);
        errdefer gate.mutex.unlock(io);
        var lease = gate.mutex.takeRetained() orelse try native.fairWriterLockWithIo(alloc, io, lock_io, gate.path, deadline_ns);
        if (lease.acquired_ns == 0) lease.acquired_ns = std.Io.Clock.awake.now(io).nanoseconds;
        lease.admission_ns = @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started));
        lease.stats = &gate.lease_stats;
        return .{ .ptr = gate, .io = io, .allocator = alloc, .lock = lease, .allocator_cache = &gate.writer_allocator_cache, .release_fn = leave };
    }
    fn leave(raw: *anyopaque, lease: *native.PathWriterLock) void {
        const gate: *PathGate = @ptrCast(@alignCast(raw));
        lease.close();
        gate.mutex.unlock(h.handleLockIo());
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

test "capi owned native artifact service queues without a Store and survives last connection close" {
    try testOwnedArtifactService(false);
}

test "capi owned native artifact service exhausts one wait budget without stranding the queue" {
    try testOwnedArtifactService(true);
}

fn testOwnedArtifactService(timeout: bool) !void {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("owned-artifact-service");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(h.handleLockIo(), tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    const wrapper = try open(path, .{ .profile = .native, .no_sync = true }, true);
    const id = try api.publishHandle(wrapper);
    var live = true;
    defer if (live) api.antfly_db_close(id);
    var stager: @import("../storage/immutable_artifact.zig").Stager = undefined;
    var registry: *artifact_source.Registry = undefined;
    var target: @import("../storage/immutable_artifact.zig").AliasTarget = undefined;
    {
        const guard = api.enterHandle(id, .read).?;
        defer guard.leave();
        try std.testing.expect(guard.entry_error == null);
        const store = guard.handle.owned_lite_backend.?.native_docstore.?;
        var adapter = @import("../storage/lite/index_storage.zig").Store.init(a, store);
        stager = (try adapter.storage().acquireImmutableStager(a)).?;
        registry = store.artifact_registry.?;
        target = .{ .inode = (try store.file.file.stat(store.file.runtime())).inode, .file_namespace = store.file.header.reader_epoch_namespace, .prefix = "/indexes/service" };
    }
    defer stager.deinit();
    const gate: *PathGate = @ptrCast(@alignCast(registry.publication_gate.?.ptr));
    api.antfly_db_close(id);
    live = false;
    try std.testing.expect(registry.store == null);
    const bytes = try a.alloc(u8, 2 * 1024 * 1024 + 17);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i *% 31 +% 17);
    var held: ?PathReplacementGuard = try acquirePathReplacement(path);
    const before = gate.mutex.next;
    const Work = struct {
        result: ?@import("../storage/immutable_artifact.zig").Prepared = null,
        failure: ?anyerror = null,
        budget: @import("../storage/immutable_artifact.zig").AdmissionBudget,
        finished: std.Io.Event = .unset,
        fn run(self: *@This(), capability: @import("../storage/immutable_artifact.zig").Stager, input: []const u8) void {
            defer self.finished.set(h.handleLockIo());
            self.result = capability.stageDetachedWithBudget(input, &self.budget) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var work: Work = .{ .budget = .{ .remaining_ns = if (timeout) 100 * std.time.ns_per_ms else 5 * std.time.ns_per_s } };
    const thread = try std.Thread.spawn(.{}, Work.run, .{ &work, stager, bytes });
    defer {
        if (held) |guard| guard.release();
        thread.join();
    }
    const deadline = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + 5 * std.time.ns_per_s;
    while (true) {
        gate.mutex.state.lockUncancelable(h.handleLockIo());
        const queued = gate.mutex.next > before;
        gate.mutex.state.unlock(h.handleLockIo());
        if (queued) break;
        try std.testing.expect(!work.finished.isSet());
        if (std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds >= deadline) return error.TestWatchdog;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 0), registry.callbacks);
    if (timeout) {
        // Keep the gate held until the queued request reports refusal. This
        // cannot pass by merely running after the contending owner releases it.
        const watchdog = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + 5 * std.time.ns_per_s;
        while (!work.finished.isSet()) {
            if (std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds >= watchdog) return error.TestWatchdog;
            try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(error.FileBusy, work.failure.?);
        try std.testing.expect(work.result == null);
        try std.testing.expectEqual(@as(u64, 0), work.budget.remaining_ns);
        try std.testing.expect(work.budget.waited_ns >= 100 * std.time.ns_per_ms);
        try std.testing.expectEqual(@as(usize, 0), registry.callbacks);
        held.?.release();
        held = null;
        const next = try acquirePathReplacement(path);
        next.release();
        var file = try native.NativeFile.openWithIo(a, h.handleLockIo(), path, .{ .no_sync = true });
        defer file.close();
        try std.testing.expect((try file.check()).valid);
        return;
    }
    held.?.release();
    held = null;
    work.finished.waitUncancelable(h.handleLockIo());
    if (work.failure) |err| return err;
    var prepared = work.result.?;
    defer prepared.deinit();
    try std.testing.expectEqual(before + 3, gate.mutex.next);
    try std.testing.expectEqual(@as(usize, 0), registry.callbacks);
    var source = try prepared.prepareLinkAt(target, "/indexes/service/segments/1.seg");
    defer source.close();
    const got = try a.alloc(u8, bytes.len);
    defer a.free(got);
    try source.readInto(0, got);
    try std.testing.expectEqualSlices(u8, bytes, got);
    var file = try native.NativeFile.openWithIo(a, h.handleLockIo(), path, .{ .no_sync = true });
    defer file.close();
    try std.testing.expect((try file.check()).valid);
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
    gate.writer_allocator_cache.deinit();
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
    writer_index_read: Site = .{},
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
    capture_max_steps: CaptureSteps = .{},
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
            .index_read => &self.writer_index_read,
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
    setup_ns: u64 = 0,
    ready_merge_ns: u64 = 0,
    finish_ns: u64 = 0,
};

/// Splits one held interval into consecutive steps.
const StepClock = struct {
    last: i96,
    fn mark(self: *StepClock, total: *u64, maximum: *u64) void {
        const now = nowNs();
        const elapsed: u64 = @intCast(@max(0, now - self.last));
        total.* +|= elapsed;
        maximum.* = @max(maximum.*, elapsed);
        self.last = now;
    }
};

fn nowNs() i96 {
    return std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds;
}

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
        // A completed source pass may observe later fully covered native text
        // writes without reopening providers. This is scheduling authority only;
        // publication still takes its own exact catalog/consumer fences.
        covered_text_controls: ?[32]u8 = null,
        has_group_sources: bool = false,
        next_group_source_ns: i96 = 0,
        // A wakeup belongs to the owner; runnable source/artifact work belongs
        // to one namespace. Audit idle WAL/index lifecycle work periodically,
        // while durable replay/projection and TTL checks still run each visit.
        database_pending: bool = false,
        artifact_pending: bool = false,
        next_artifact_ns: i96 = 0,
        // Performance hints, never publication authority. Bound historical
        // configs; fresh runtimes retain the last failed plan's smaller width.
        text_merge_caps: std.AutoHashMapUnmanaged(u64, u32) = .empty,
        serviced_sequence: u64 = 0,
        next_database_audit_ns: i96 = 0,
        next_producer_ns: i96 = 0,
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
    connections: std.ArrayList(*Connection) = .empty,
    worker: ?std.Thread = null,
    producer_worker: ?std.Thread = null,
    producer_mutex: std.Io.Mutex = .init,
    producer_wake: std.Io.Event = .unset,
    producer_mailbox: ?*PendingProducer = null,
    pending_producer: ?*PendingProducer = null,
    producer_jobs_active: std.atomic.Value(usize) = .init(0),
    // The path gate permits one foreground lease wait per owner. The global
    // debug Io cannot spawn tasks; this lazy pool supplies exactly the lock
    // waiter and its deadline timer, and is shared by compatible connections.
    path_lock_io: std.Io.Threaded,
    test_maintenance_wait_ns: if (h.builtin.is_test) ?i96 else void = if (h.builtin.is_test) null else {},
    test_text_merge_segment_limit: if (h.builtin.is_test) ?u64 else void = if (h.builtin.is_test) null else {},
    test_before_producer_service: if (h.builtin.is_test) ?struct {
        ptr: *anyopaque,
        run: *const fn (*anyopaque, *PendingProducer) void,
    } else void = if (h.builtin.is_test) null else {},
    stopping: std.atomic.Value(bool) = .init(false),
    // One graph walk per owner. Admission waits without the foreground gate;
    // the worker only try-locks under that gate and yields to an active proof.
    allocator_proof_mutex: std.Io.Mutex = .init,
    /// A proof that completed but could not be transferred because the
    /// writer lease was busy. Guarded by the proof mutex.
    allocator_proof_receipt: ?native.NativeFile.AllocatorProofReceipt = null,
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
    maintenance_activity: @import("maintenance_activity.zig").Activity = .{},
    current: ?*h.Handle = null,
    retired: std.ArrayList(*h.Handle) = .empty,
    maintenance_snapshot: ?*h.Handle = null,
    pending_text_merge: ?*PendingTextMerge = null,
    maintenance_last_merge_stage: bool = false,
    // Only immutable index/cache state survives between bounded artifact slices.
    // No primary read epoch or borrowed catalog proof is retained here.
    maintenance_retained_logical: ?native.LogicalGenerations = null,
    text_source_runtime_cache: ?*TextSourceRuntimeCache = null,
    immutable_text_handoff: ?ImmutableTextHandoff = null,
    immutable_sources: @import("../storage/lite/artifact_source.zig").WorkingSet = .{},
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
        producer_jobs_started: u64 = 0,
        producer_jobs_completed: u64 = 0,
        producer_service_ns: u64 = 0,
        group_source_control_batches: u64 = 0,
        group_source_control_commands: u64 = 0,
        group_source_control_commits: u64 = 0,
        covered_source_observations: u64 = 0,
        serving_physical_turns: u64 = 0,
        admitted_merge_captures: u64 = 0,
        ready_merge_publication_turns: u64 = 0,
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
        // Preparation overlaps replay; do not mix it into actor phase time.
        text_merge_jobs_started: u64 = 0,
        text_merge_jobs_applied: u64 = 0,
        text_merge_jobs_discarded: u64 = 0,
        text_merge_in_flight: bool = false,
        text_merge_prepare_ns: u64 = 0,
        text_merge_staging_ns: u64 = 0,
        text_merge_staged_bytes: u64 = 0,
        text_merge_publication_ns: u64 = 0,
        maintenance_storage_ns: u64 = 0,
        sql_constraint_overlay_hits: u64 = 0,
        sql_constraint_overlay_scope_extensions: u64 = 0,
        sql_constraint_overlay_rebuilds: u64 = 0,
        sql_constraint_overlay_rebuild_entries: u64 = 0,
        sql_constraint_overlay_evictions: u64 = 0,
        sql_constraint_staged_statements: u64 = 0,
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
        /// Full replacement attempts, including allocator admission and index
        /// opens. In-place checkpoint checks are excluded.
        serving_runtime_refresh_ns: u64 = 0,
        maintenance_runtime_opens: u64 = 0,
        maintenance_namespace_opens: u64 = 0,
        maintenance_source_runtime_reuses: u64 = 0,
        text_runtime_cache: TextRuntimeCacheDiagnostics = .{},
        replay_backlog: ?native.replay_admission.State = null,
        replay_backpressure_waits: u64 = 0,
        replay_refused_preparation_ns: u64 = 0,
        replay_index_groups: u64 = 0,
        replay_capacity_yields: u64 = 0,
        replay_publication_overhead_ns: u64 = 0,
        replay_index_admission_ns: u64 = 0,
        replay_metadata_admission_ns: u64 = 0,
        maintenance_publication_overhead_ns: u64 = 0,
        publication_hold_ns: u64 = 0,
        replay_metadata_groups: u64 = 0,
        replay_attempts: u64 = 0,
        replay_refused: u64 = 0,
        maintenance_errors: u64 = 0,
        maintenance_page_errors: u64 = 0,
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
        path_leases: native.PathLeaseStats.Snapshot = .{},
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
        // Successful work in another namespace clears the current error, but
        // must not erase the evidence behind the cumulative failure count.
        last_maintenance_failure: ?[]const u8 = null,
        last_maintenance_failure_table_id: u64 = 0,
    } = .{},
    test_runtime_snapshot_opens: if (h.builtin.is_test) u64 else void = if (h.builtin.is_test) 0 else {},
    embedding_memo: ?memo_mod.QueryEmbeddingCache = null,
    embedding_memo_budget: cache_budget.CacheBudget = .init(embedding_memo_bytes),
    test_after_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *h.Handle) void } else void = if (h.builtin.is_test) null else {},
    test_before_group_source: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},
    test_before_group_command: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, @import("../storage/db/group_source_contract.zig").Command) void } else void = if (h.builtin.is_test) null else {},
    test_after_allocator_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *native.NativeFile) void } else void = if (h.builtin.is_test) null else {},
    test_before_allocator_reader_close: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *native.NativeFile) void } else void = if (h.builtin.is_test) null else {},
    test_allocator_wait: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},
    test_after_stop: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},
    test_before_storage_service: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *h.Handle, bool) void } else void = if (h.builtin.is_test) null else {},
    test_before_replay_index_publication: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *h.Handle) void } else void = if (h.builtin.is_test) null else {},
    test_before_maintenance_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},

    /// Caller holds the path lease, so the checkpoint and catalog cannot race
    /// a different process's publication. Cursor continuation deliberately
    /// uses its pinned generation without reopening the connection.
    pub fn refresh(self: *Owner) !*h.Handle {
        // Publication uncertainty is file-wide even when the writer belongs
        // to a detached maintenance capture. No foreground operation may
        // silently continue through a different, apparently clean wrapper.
        if (self.maintenance_snapshot) |snapshot| if (needsOutcomeReconciliation(snapshot)) return error.OutcomeReconciliationRequired;
        if (self.pending_text_merge) |pending| if (needsOutcomeReconciliation(pending.root)) return error.OutcomeReconciliationRequired;
        var known_maintenance: ?*h.Handle = null;
        if (self.current) |current| {
            // Existing handles remain fenced until they are closed. A new
            // connection can reconstruct the durable decision with a fresh
            // owner, without inheriting this wrapper's uncertain caches.
            if (needsOutcomeReconciliation(current)) return error.OutcomeReconciliationRequired;
            if (current.lite_scheduling_owner == null) try self.bindGroupSourceState(current);
            var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
            defer probe.close();
            const file = &current.owned_lite_backend.?.native_docstore.?.file;
            const probe_inode = (try probe.file.stat(probe.runtime())).inode;
            self.immutable_sources.observe(probe_inode, @import("antfly_platform").time.monotonicNs());
            // Establish the fenced tip before opening any new read pins.
            // Existing readers of an unknown peer publication remain poisoned.
            self.reader_coordinator.observeCurrent(probe_inode, probe.headerFingerprint());
            const same_inode = probe_inode == (try file.file.stat(file.runtime())).inode;
            // An uncertain publication can fail before writing anything, so an
            // unchanged header does not prove the runtime is reusable: reopen
            // to reconcile from durable state.
            if (current.immutable_query_borrows == 0 and current.maintenance_references == 0 and same_inode and !needsOutcomeReconciliation(current) and std.meta.eql(probe.header, file.header)) {
                if (!file.free_pages_verified) if (self.maintenance_snapshot) |snapshot|
                    try file.inheritAllocatorVerification(&snapshot.owned_lite_backend.?.native_docstore.?.file);
                try self.takeAvailableAllocator(file);
                return current;
            }
            if (current.immutable_query_borrows == 0 and current.maintenance_references == 0 and same_inode and !needsOutcomeReconciliation(current) and
                try file.runtimeMetadataUnchanged(&probe) and try loadedIndexInputsUnchanged(current, file, &probe))
            {
                // Lease bookkeeping and unpublished shadow files do not change
                // a serving runtime. Reopening it would publish another lease
                // for every immutable input while foreground traffic waits.
                try current.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(probe.header);
                if (self.maintenance_snapshot) |snapshot| try file.inheritAllocatorVerification(&snapshot.owned_lite_backend.?.native_docstore.?.file);
                try self.takeAvailableAllocator(file);
                if (self.options.profile == .native and self.options.open_mode == .writer and !file.free_pages_verified)
                    _ = try file.inheritAllocatorCertificate();
                return current;
            }
            if (self.maintenance_snapshot) |snapshot| {
                const captured = &snapshot.owned_lite_backend.?.native_docstore.?.file;
                if (std.meta.eql(probe.header, captured.header) and
                    (try probe.file.stat(probe.runtime())).inode == (try captured.file.stat(captured.runtime())).inode) known_maintenance = snapshot;
            }
            if (self.pending_text_merge) |pending| {
                const captured = &pending.root.owned_lite_backend.?.native_docstore.?.file;
                if (std.meta.eql(probe.header, captured.header) and probe_inode == (try captured.file.stat(captured.runtime())).inode) known_maintenance = pending.root;
            }
        }
        const refresh_started_ns = @import("antfly_platform").time.monotonicNs();
        defer self.publication_stats.serving_runtime_refresh_ns +|= @import("antfly_platform").time.monotonicNs() -| refresh_started_ns;
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
            try self.takeAvailableAllocator(&certified.?);
            if (certified.?.ledger == null) if (self.current) |previous| {
                // Only an idle serving wrapper can donate. Active cursors,
                // detached work and proof readers retain independent caches.
                if (previous.query_borrows == 0 and previous.maintenance_references == 0 and
                    previous.allocator_verification_references == 0 and previous.sql_cursors.count() == 0)
                    _ = try certified.?.takeAllocatorRebaseBase(&previous.owned_lite_backend.?.native_docstore.?.file);
            };
            // Cache handoff already authenticates the exact inode/checkpoint;
            // rebasing additionally checks the current certificate. Repeating
            // certificate admission here discards that cache's Merkle digest
            // and hashes the complete ledger under every runtime refresh.
            if (certified.?.free_pages_verified or try certified.?.inheritAllocatorCertificate()) {
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
        replacement.owned_lite_backend.?.native_docstore.?.setArtifactPublicationGate(OwnedArtifactGate.view(self.gate));
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
            if (previous.maintenance_references == 0 and previous.immutable_query_borrows == 0) {
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
                if (retired.query_borrows > 0 or retired.refresh_references != 0 or retired.maintenance_references != 0 or retired.allocator_verification_references != 0) break :retained true;
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
        if (self.options.profile == .native and database.open_mode != .query_readonly and database.open_mode != .status_only and database.core.store.runtime_store.capabilities().bounded_replay_admission) {
            database.async_context.replay_retirement_service = .{
                .ptr = self,
                .table_id = database.core.identity_namespace.table_id,
                .incarnation = database.root_incarnation,
                .request = notifySourceMutation,
            };
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

    /// Called without request admission. A workerless owner uses the same
    /// bounded service operation, rather than replaying under a caller's lease.
    pub fn requestVisibilityProgress(self: *Owner, table_id: u64, incarnation: u128) !void {
        if (self.stopping.load(.acquire)) return error.Canceled;
        self.runnable_sources.notify(h.handleLockIo(), .{ .table_id = table_id, .incarnation = incarnation });
        if (self.worker == null) {
            _ = self.maintenanceTurn() catch |err| {
                self.gate.mutex.lockUncancelable(h.handleLockIo());
                self.noteMaintenanceFailure(err, table_id, .capture);
                self.gate.mutex.unlock(h.handleLockIo());
                switch (err) {
                    error.FileBusy, error.WouldBlock, error.EnrichmentWaitCanceled, error.EnrichmentSourceChanged, error.SourceFileChanged, error.TextMergeProducerDeferred => return,
                    else => return err,
                }
            };
        } else self.wake.set(h.handleLockIo());
    }

    /// Shared and plain fallback leases have no queue acquisition timestamp.
    /// Every telemetry attachment, including initial open, establishes one.
    fn attachPathLeaseStats(self: *Owner, lease: *native.PathWriterLock) void {
        if (lease.acquired_ns == 0) lease.acquired_ns = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds;
        lease.stats = &self.gate.lease_stats;
    }

    /// Scope registration reuse to admission; closing the path lease also
    /// releases idle anchors before another process can publish.
    fn withReadRegistrationBatch(self: *Owner, lease: native.PathWriterLock) native.PathWriterLock {
        var held = lease;
        self.attachPathLeaseStats(&held);
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
        const stats = &self.publication_stats.maintenance_lease;
        if (self.gate.mutex.takeRetained()) |held| {
            var lease = held;
            stats.recordAcquired(.handoff, 0);
            lease.startHold(stats);
            return self.withReadRegistrationBatch(lease);
        }
        const started = std.Io.Clock.awake.now(io).nanoseconds;
        // Maintenance joins the same FIFO as foreground writers, so a
        // continuously busy foreground can delay it by at most one round of
        // queued turns instead of starving it.
        const default_budget = 5 * std.time.ns_per_s;
        const budget = if (h.builtin.is_test) self.test_maintenance_wait_ns orelse default_budget else default_budget;
        var held = native.fairWriterLockWithIo(alloc, io, self.path_lock_io.io(), self.path, std.Io.Clock.awake.now(io).nanoseconds + budget) catch |err| switch (err) {
            error.FileBusy => {
                self.publication_stats.maintenance_queue_timeouts +|= 1;
                stats.recordBusy(@intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started)));
                return err;
            },
            else => return err,
        };
        errdefer held.close();
        stats.recordAcquired(if (held.queue_turn != null) .queued else .uncontended, @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started)));
        held.admission_ns = @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started));
        held.startHold(stats);
        if (self.stopping.load(.acquire)) return error.Canceled;
        return self.withReadRegistrationBatch(held);
    }

    /// Caller holds the gate. Nonblocking: reuses a lease kept for a queued
    /// thread, otherwise refuses while another process is queued.
    fn tryWriterLeaseAssumeLocked(self: *Owner) !native.PathWriterLock {
        const stats = &self.publication_stats.maintenance_lease;
        var lease = if (self.gate.mutex.takeRetained()) |held| blk: {
            stats.recordAcquired(.handoff, 0);
            break :blk held;
        } else blk: {
            const held = try native.tryFairWriterLockWithIo(alloc, h.handleLockIo(), self.path);
            stats.recordAcquired(.uncontended, 0);
            break :blk held;
        };
        lease.startHold(stats);
        return lease;
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

    fn coveredTextControls(root: *h.Handle, database: *h.db_mod.DB, namespace: []const u8, readiness: MaintenanceReadiness) !?[32]u8 {
        if (database.detachedTextSourceBlock() != null) return null;
        var schema = database.core.acquireSchemaView();
        defer if (schema) |*view| view.release();
        if (schema) |view| if (view.storageMode() == .relational) return null;
        const indexes = database.core.index_manager;
        // With no consumers, a new primary tip is already covered. Keep the
        // same catalog/control/audit fences rather than constructing a source
        // runtime merely to discover there is nothing to replay.
        for (indexes.text_indexes.items) |*entry| if (!entry.persistent.hasNativeMetadata()) return null;
        const file = &root.owned_lite_backend.?.native_docstore.?.file;
        const metadata = file.activeCheckpoint().logical.metadata;
        if (std.mem.allEqual(u8, &metadata, 0)) return null;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const prefix = if (std.mem.eql(u8, namespace, "root")) "" else try root.owned_lite_backend.?.documentPrefixAlloc(a, namespace);
        var digest = std.crypto.hash.sha2.Sha256.init(.{});
        digest.update("antfly-covered-native-text-source-v1");
        digest.update(&metadata);
        // Exclude only the source sequence. All schema/resolver/table/policy
        // controls, including absent values, belong to the clean witness.
        for (readiness[0..PublicationCatalog.metadata_keys.len]) |value| {
            digest.update(&.{@intFromBool(value != null)});
            if (value) |bytes| digest.update(&bytes);
        }
        var read = try root.owned_lite_backend.?.native_docstore.?.beginRead();
        defer read.abort();
        const control_keys = try indexes.retainedTextRuntimeControlKeysAlloc(a);
        const admission_prefix = keys.globalReplayPrefix(.managed_admission);
        for (control_keys) |key| {
            const value = try read.valueFingerprint(try std.mem.concat(a, u8, &.{ prefix, key }));
            if (value != null and std.mem.startsWith(u8, key, &admission_prefix)) return null;
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, @intCast(key.len), .little);
            digest.update(&length);
            digest.update(key);
            digest.update(&.{@intFromBool(value != null)});
            if (value) |bytes| digest.update(&bytes);
        }
        return digest.finalResult();
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
            const resolution_target = if (database.resolution_runtime) |runtime| @max(target, runtime.stats().target_sequence) else target;
            if (try state.loadAppliedSequence(alloc, database.core.store, "resolution") < resolution_target) return true;
            const promotion_target = if (database.promotion_runtime) |runtime| @max(target, runtime.stats().target_sequence) else target;
            if (try state.loadAppliedSequence(alloc, database.core.store, "promotion") < promotion_target) return true;
            // Catalog backfills also advance durable cursors without adding
            // replay records. Inspect both keys in one committed view only
            // after the cheap watermark checks have ruled out other debt.
            if (try @import("../storage/db/resolution_runtime.zig").hasDurableReresolveBacklog(database.core.store)) return true;
        }
        return false;
    }

    const ReplayProgress = struct {
        const Fragment = @import("../storage/db/derived/replay_fragment.zig").Position;
        const Index = struct { name: []const u8, applied: u64, fragment: ?Fragment };
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
            errdefer a.free(indexes);
            for (managed, indexes) |index, *captured| captured.* = .{ .name = index.name, .applied = try database.core.loadAppliedSequence(a, index.name), .fragment = try fragment(database, index.name) };
            return .{
                .indexes = indexes,
                .generated = if (database.core.hasGeneratedEnrichmentTargets()) try lane(database, "generated") else 0,
                .resolution = if (database.hasConfiguredResolvers()) try lane(database, "resolution") else 0,
                .promotion = if (database.hasConfiguredResolvers()) try lane(database, "promotion") else 0,
                .resolver_cursors = if (database.hasConfiguredResolvers()) try cursors(database) else null,
            };
        }

        fn advanced(self: ReplayProgress, database: *h.db_mod.DB) bool {
            for (self.indexes) |index| {
                if ((database.core.loadAppliedSequence(alloc, index.name) catch index.applied) > index.applied) return true;
                if (fragment(database, index.name) catch null) |now| {
                    const before = index.fragment;
                    // Durable partial progress earns another fair source
                    // slice, without pretending the whole record is applied.
                    if (before == null or now.sequence > before.?.sequence or
                        (now.sequence == before.?.sequence and now.next_item > before.?.next_item)) return true;
                }
            }
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

        fn fragment(database: *h.db_mod.DB, name: []const u8) !?Fragment {
            const cfg = database.core.index_manager.get(name) orelse return null;
            if (cfg.kind != .full_text or cfg.coverage_generation == 0) return null;
            return database.loadReplayFragmentState(name);
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
    fn enterReplayIndexPublication(ptr: *anyopaque, index_name: []const u8) !void {
        const root: *h.Handle = @ptrCast(@alignCast(ptr));
        if (comptime h.builtin.is_test) if (root.maintenance_owner.?.test_before_replay_index_publication) |hook| hook.notify(hook.ptr, root);
        const started = @import("antfly_platform").time.monotonicNs();
        try enterMaintenanceWriterForIndex(root, .index, index_name);
        root.maintenance_owner.?.publication_stats.replay_index_admission_ns +|= @import("antfly_platform").time.monotonicNs() -| started;
        root.maintenance_owner.?.publication_stats.replay_index_groups +|= 1;
    }

    fn enterReplayMetadataPublication(ptr: *anyopaque) !void {
        const root: *h.Handle = @ptrCast(@alignCast(ptr));
        const started = @import("antfly_platform").time.monotonicNs();
        try enterMaintenanceWriter(root, .primary);
        root.maintenance_owner.?.publication_stats.replay_metadata_admission_ns +|= @import("antfly_platform").time.monotonicNs() -| started;
        root.maintenance_owner.?.publication_stats.replay_metadata_groups +|= 1;
    }

    fn recordMaintenanceAdmission(root: *h.Handle, started: i96) void {
        const publication = root.maintenance_publication orelse return;
        const counter = publication.publication_overhead_ns orelse return;
        const elapsed = @max(0, std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds - started);
        _ = counter.fetchAdd(@intCast(@min(elapsed, std.math.maxInt(u64))), .monotonic);
    }

    fn enterReplacementWriter(ptr: *anyopaque) !void {
        try enterMaintenanceWriter(ptr, .index);
    }

    fn enterMaintenanceWriter(ptr: *anyopaque, scope: PublicationScope) !void {
        return enterMaintenanceWriterForIndex(ptr, scope, null);
    }

    fn enterMaintenanceWriterForIndex(ptr: *anyopaque, scope: PublicationScope, index_name: ?[]const u8) !void {
        const root: *h.Handle = @ptrCast(@alignCast(ptr));
        const self = root.maintenance_owner.?;
        const admission_started = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds;
        var count_admission = true;
        defer if (count_admission) recordMaintenanceAdmission(root, admission_started);
        root.maintenance_admission_mutex.lockUncancelable(h.handleLockIo());
        defer root.maintenance_admission_mutex.unlock(h.handleLockIo());
        if (self.stopping.load(.acquire)) return error.Canceled;
        if (needsOutcomeReconciliation(root)) return error.OutcomeReconciliationRequired;
        const publication_context = root.maintenance_publication;
        const scoped_index = if (publication_context) |p|
            if (index_name) |name| p.findIndexScope(name) else if (root.maintenance_writer_references != 0) p.admitted_index_scope else null
        else
            null;
        // Nested storage writes borrow only the already validated index domain.
        if (root.maintenance_writer_references != 0 and index_name != null) {
            if (publication_context) |p| if (p.admitted_index_scope != scoped_index) return error.EnrichmentWaitCanceled;
        }
        // A primary publication can rebase onto changed index inputs while
        // recording completed replay progress. Nesting shares its writer
        // lease, but cannot grant its stale index runtimes new authority.
        if (scope == .index or scope == .index_read) if (root.maintenance_publication) |p| if (p.index_inputs_changed and scoped_index == null) {
            self.publication_stats.index_input_refusals +|= 1;
            return error.EnrichmentWaitCanceled;
        };
        if (root.maintenance_writer_references != 0) {
            count_admission = false; // Included in the outer publication's hold.
            root.maintenance_writer_references += 1;
            return;
        }
        if (scope != .reclamation) self.gate.mutex.lockUncancelable(h.handleLockIo()) else if (!self.gate.mutex.tryLock()) return error.EnrichmentWaitCanceled;
        errdefer self.gate.mutex.unlock(h.handleLockIo());
        if (self.stopping.load(.acquire)) return error.Canceled;
        self.publication_stats.attempts +|= 1;
        errdefer self.publication_stats.refused +|= 1;
        // Bounded allocator service runs after detached publication/catalog
        // pins have been released and computes no document/index output.
        const requires_publication = scope != .reclamation and scope != .allocator_service;
        if (requires_publication and root.maintenance_publication == null) return error.Canceled;
        if (self.current) |current| if (needsOutcomeReconciliation(current)) return error.EnrichmentWaitCanceled;
        var lease = (if (scope == .reclamation) self.tryWriterLeaseAssumeLocked() else self.acquireMaintenancePathLeaseAssumeLocked()) catch |err| switch (err) {
            error.FileBusy, error.WouldBlock => return error.EnrichmentWaitCanceled,
            else => return err,
        };
        errdefer lease.close();
        // The hold starts here: admission checks and rebases run under it.
        const held_from = nowNs();
        errdefer MaintenanceHoldSites.record(self.publication_stats.maintenance_hold_sites.writer(scope), held_from);
        var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
        defer probe.close();
        const file = &root.owned_lite_backend.?.native_docstore.?.file;
        const probe_inode = (try probe.file.stat(probe.runtime())).inode;
        if (probe_inode != (try file.file.stat(file.runtime())).inode) return error.EnrichmentWaitCanceled;
        self.reader_coordinator.observeCurrent(probe_inode, probe.headerFingerprint());
        // Allocator-only adoption can advance this file without validating
        // its captured catalog. Scoped permission must recheck the durable
        // recovery proof even when the file already carries the current tip.
        // findIndexScope permits only the non-mutating native proof path.
        if (scoped_index != null) try publication_context.?.validateStorageCurrent(&probe);
        if (!std.meta.eql(probe.header, file.header) and scope == .allocator_service) {
            // Bounded allocator/orphan service computes no detached output.
            // Start it on the current ledger under the fair writer lease;
            // vacuum assessment/copy publication still rejects a moved tip.
            try root.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(probe.header);
            if (self.current) |current| try file.inheritAllocatorVerification(&current.owned_lite_backend.?.native_docstore.?.file);
            try root.owned_lite_backend.?.native_docstore.?.prepareWriterAllocatorAssumeAdmitted();
            if (!file.free_pages_verified) _ = try file.inheritAllocatorCertificate();
            self.publication_stats.rebased +|= 1;
        } else if (!std.meta.eql(probe.header, file.header)) {
            if (scope == .reclamation) return error.EnrichmentWaitCanceled;
            const publication = root.maintenance_publication orelse return error.EnrichmentWaitCanceled;
            if (scoped_index == null) try publication.validateStorageCurrent(&probe);
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
            if (root.owned_lite_backend.?.native_docstore.?.index_reads_require_admission or file.activeCheckpoint().index_catalog_root_page != latest.activeCheckpoint().index_catalog_root_page) {
                if (!try publication.indexInputsUnchanged(file, &latest)) {
                    publication.index_inputs_changed = true;
                    if ((scope == .index or scope == .index_read) and scoped_index == null) {
                        self.publication_stats.index_input_refusals +|= 1;
                        return error.EnrichmentWaitCanceled;
                    }
                }
            }
            if (scoped_index) |i| if (!try publication.indexScopeUnchanged(i, &latest)) {
                self.publication_stats.index_input_refusals +|= 1;
                return error.EnrichmentWaitCanceled;
            };
            try root.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(latest.header);
            if (self.current) |current| try file.inheritAllocatorVerification(&current.owned_lite_backend.?.native_docstore.?.file);
            self.publication_stats.rebased +|= 1;
        }
        if (scoped_index) |i| if (!try publication_context.?.indexScopeUnchanged(i, file)) {
            self.publication_stats.index_input_refusals +|= 1;
            return error.EnrichmentWaitCanceled;
        };
        if (requires_publication) {
            const publication = root.maintenance_publication orelse return error.Canceled;
            publication.requireCurrentCatalog() catch |err| {
                publication.index_inputs_changed = true;
                return err;
            };
        }
        // A changed tip invalidates a detached proof. Retry outside the gate
        // instead of falling back to a whole-file scan in this publication.
        // Metadata-only validation need not open a serving runtime to donate
        // an allocator. Rebase this writer's checked ledger against the exact
        // admitted tip using the native certificate protocol.
        if (scope != .reclamation) try root.owned_lite_backend.?.native_docstore.?.prepareWriterAllocatorAssumeAdmitted();
        if (scope != .reclamation and !file.free_pages_verified)
            _ = try file.inheritAllocatorCertificate();
        if ((scope == .primary or scope == .index) and !file.free_pages_verified) {
            self.publication_stats.allocator_refusals +|= 1;
            return error.EnrichmentWaitCanceled;
        }
        if (scope == .allocator_service) self.publication_stats.allocator_service_admissions +|= 1;
        root.maintenance_writer_lease = lease;
        if (publication_context) |p| p.admitted_index_scope = scoped_index;
        root.maintenance_writer_references = 1;
        root.maintenance_writer_scope = @backingInt(scope);
        root.maintenance_writer_held_from = held_from;
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
        // Refresh the owned witness before releasing the path lease. Never
        // turn a completed publication into a retryable error if this read
        // fails: invalidate future use and discard the cached runtime instead.
        if (root.maintenance_publication) |p| if (p.admitted_index_scope) |i| {
            p.index_scopes[i].identity = root.owned_lite_backend.?.native_docstore.?.file.indexScopeIdentity(p.index_scopes[i].domain) catch blk: {
                p.index_scopes[i].valid = false;
                p.index_inputs_changed = true;
                break :blk null;
            };
            p.admitted_index_scope = null;
        };
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
        // Status readers serialize publication_stats under the gate; record
        // before releasing it.
        MaintenanceHoldSites.record(self.publication_stats.maintenance_hold_sites.writer(@fromBackingInt(@intCast(root.maintenance_writer_scope))), root.maintenance_writer_held_from);
        self.gate.mutex.unlock(h.handleLockIo());
    }

    fn retainMaintenanceSnapshot(root: *h.Handle) void {
        const before = root.maintenance_runtime_references.fetchAdd(1, .monotonic);
        std.debug.assert(before != 0 and before < std.math.maxInt(usize));
    }

    fn closeMaintenanceSnapshot(root: *h.Handle) void {
        const before = root.maintenance_runtime_references.fetchSub(1, .acq_rel);
        std.debug.assert(before != 0);
        if (before != 1) return;
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

    fn hasUncertainOutcome(self: *Owner) bool {
        if (self.current) |root| if (needsOutcomeReconciliation(root)) return true;
        if (self.maintenance_snapshot) |root| if (needsOutcomeReconciliation(root)) return true;
        if (self.pending_text_merge) |pending| if (needsOutcomeReconciliation(pending.root)) return true;
        if (self.pending_producer) |pending| if (needsOutcomeReconciliation(pending.root)) return true;
        return false;
    }

    /// File-wide uncertainty outlives a detached runtime. Preserve its fence
    /// on the serving wrapper before retiring cached immutable readers.
    fn preserveMaintenanceOutcomeFence(self: *Owner) void {
        self.immutable_sources.clear();
        const current = self.current orelse return;
        const current_store = current.owned_lite_backend.?.native_docstore.?;
        for ([_]?*h.Handle{ self.maintenance_snapshot, if (self.pending_text_merge) |pending| pending.root else null, if (self.pending_producer) |pending| pending.root else null }) |maybe_root| {
            const root = maybe_root orelse continue;
            const source_store = root.owned_lite_backend.?.native_docstore.?;
            current.sql_decision_uncertain = current.sql_decision_uncertain or root.sql_decision_uncertain;
            current_store.secret_store_uncertain = current_store.secret_store_uncertain or source_store.secret_store_uncertain;
            current_store.file.checkpoint_publication_uncertain = current_store.file.checkpoint_publication_uncertain or source_store.file.checkpoint_publication_uncertain;
        }
    }

    fn noteMaintenanceFailure(self: *Owner, err: anyerror, table_id: u64, phase: MaintenanceFailurePhase) void {
        self.publication_stats.maintenance_errors +|= 1;
        if (@import("antfly_runtime_abi").error_abi.isNativePageFailure(err)) {
            self.publication_stats.maintenance_page_errors +|= 1;
            // A later retry cannot erase evidence of a failed storage read.
            std.log.warn("antfly_native_maintenance_page_failure phase={s} error={s}", .{ @tagName(phase), @errorName(err) });
        }
        switch (phase) {
            inline else => |kind| @field(self.publication_stats.maintenance_failures, @tagName(kind)) +|= 1,
        }
        self.publication_stats.last_maintenance_error = @errorName(err);
        self.publication_stats.last_maintenance_failure = @errorName(err);
        self.publication_stats.last_maintenance_failure_table_id = table_id;
        if (qualificationMetricsEnabled()) std.log.info("antfly_bench_native_maintenance_failure phase={s} error={s}", .{ @tagName(phase), @errorName(err) });
    }

    fn inheritReclamationTelemetry(destination: anytype, source: anytype) void {
        destination.file.allocator_work.absorb(&source.file.allocator_work);
        // The detached physical service owns this deadline. Preserve it when
        // the serving wrapper later becomes the next maintenance source.
        destination.maintenance_policy.retirement_batch = source.maintenance_policy.retirement_batch;
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

    /// A cold proof receiver may copy a quiescent snapshot's ledger. Prefer
    /// takeAvailableAllocator, which moves ownership without an O(ledger) copy.
    fn copyIdleSnapshotLedger(self: *Owner, file: *native.NativeFile) !bool {
        if (file.ledger != null) return false;
        const snapshot = self.maintenance_snapshot orelse return false;
        if (snapshot.query_borrows != 0 or snapshot.maintenance_references != 0 or
            snapshot.allocator_verification_references != 0 or snapshot.sql_cursors.count() != 0) return false;
        return file.copyAllocatorRebaseBase(&snapshot.owned_lite_backend.?.native_docstore.?.file);
    }

    const MaintenanceCapture = enum { storage, artifacts, source };

    fn refreshMaintenanceSnapshot(self: *Owner, foreground: *h.Handle, capture: MaintenanceCapture) !*h.Handle {
        return self.refreshMaintenanceSnapshotAt(foreground, capture, null);
    }

    fn refreshMaintenanceSnapshotNamed(self: *Owner, foreground: *h.Handle, capture: MaintenanceCapture, name: ?[]const u8) !*h.Handle {
        return self.refreshMaintenanceSnapshotNamedAt(foreground, capture, null, name);
    }

    fn clearTextSourceRuntimeCache(self: *Owner) void {
        if (self.text_source_runtime_cache) |cache| cache.destroy();
        self.text_source_runtime_cache = null;
    }

    fn clearImmutableTextHandoff(self: *Owner) void {
        if (self.immutable_text_handoff) |*handoff| handoff.deinit();
        self.immutable_text_handoff = null;
    }

    fn preserveImmutableTextHandoff(self: *Owner, root: *h.Handle, database: *h.db_mod.DB) void {
        // Caller owns the owner gate. Snapshot acquisition and Source uses
        // are bounded metadata/refcount operations; they perform no I/O.
        // This is one file-wide optional budget, not one per namespace. An
        // allocation or resource refusal must not alter a committed outcome.
        self.clearImmutableTextHandoff();
        if (self.stopping.load(.acquire)) return;
        if (needsOutcomeReconciliation(root)) {
            self.immutable_sources.clear();
            return;
        }
        if (self.current) |current| if (needsOutcomeReconciliation(current)) {
            self.immutable_sources.clear();
            return;
        };
        const store = root.owned_lite_backend.?.native_docstore.?;
        if (store.artifact_registry) |registry| {
            const inode = (store.file.file.stat(store.file.runtime()) catch return).inode;
            self.immutable_sources.capture(registry, inode, store.file.activeCheckpoint().page_count *| store.file.header.page_size, @import("antfly_platform").time.monotonicNs());
        }
        self.immutable_text_handoff = ImmutableTextHandoff.capture(root, database) catch null;
    }

    fn consumeImmutableTextHandoff(self: *Owner, database: *h.db_mod.DB) void {
        if (self.immutable_text_handoff) |handoff| {
            if (handoff.table_id == database.core.identity_namespace.table_id and handoff.incarnation == database.root_incarnation)
                self.clearImmutableTextHandoff();
        }
    }

    fn expireImmutableTextHandoff(self: *Owner, now_ns: u64) void {
        if (now_ns >= self.immutable_sources.expires_ns) self.immutable_sources.clear();
        if (self.immutable_text_handoff) |handoff| if (now_ns >= handoff.expires_ns)
            self.clearImmutableTextHandoff();
    }

    /// Caller owns the gate and a quiescent local turn. A merge owns its own
    /// reference, so retiring this cache cannot close its inputs.
    fn discardMaintenanceRuntime(self: *Owner) void {
        self.clearImmutableTextHandoff();
        self.clearTextSourceRuntimeCache();
        self.maintenance_retained_logical = null;
        // A failed physical publication can already have cleared its source
        // witness while leaving the detached DB and its artifact readers live.
        const root = self.maintenance_snapshot orelse return;
        if (root.db_live and root.maintenance_references == 0 and root.query_borrows == 0 and
            !needsOutcomeReconciliation(root)) self.preserveImmutableTextHandoff(root, &root.db);
        self.maintenance_snapshot = null;
        closeMaintenanceSnapshot(root);
        if (self.reader_coordinator.hasArtifactReleases()) self.maintenance_storage_pending = true;
    }

    fn openMaintenanceNamespace(self: *Owner, root: *h.Handle, name: []const u8) !*h.db_mod.DB {
        if (!root.embedded_tables.contains(name)) {
            self.publication_stats.maintenance_namespace_opens +|= 1;
            self.clearTextSourceRuntimeCache();
            // Rotation is not an invalidation. Retain a small working set,
            // evicting its least recently selected runtime only at capacity.
            // No source epoch survives between turns. A merge borrowing this
            // root must have been retired by the selection path first.
            if (self.pending_text_merge) |job| std.debug.assert(job.root != root);
            if (root.embedded_tables.count() >= TextSourceRuntimeCache.max_namespaces) {
                var oldest: ?*@import("tables.zig").Table = null;
                var resident = root.embedded_tables.valueIterator();
                while (resident.next()) |table| {
                    if (oldest == null or table.*.maintenance_last_use < oldest.?.maintenance_last_use)
                        oldest = table.*;
                }
                @import("tables.zig").evictMaintenanceRuntime(root, oldest.?);
            }
        }
        const database = try @import("tables.zig").get(root, name);
        root.embedded_tables.get(name).?.maintenance_last_use = @import("antfly_platform").time.monotonicNs();
        return database;
    }

    /// Caller owns the path gate and writer lease. Mutable allocator state is
    /// moved from a quiescent wrapper, including the retained merge owner. The
    /// merge thread only reads independently leased immutable artifact sources.
    fn takeAvailableAllocator(self: *Owner, file: *native.NativeFile) !void {
        const donors = [_]?*h.Handle{ self.maintenance_snapshot, if (self.pending_text_merge) |pending| pending.root else null, self.current };
        for (donors) |maybe_owner| if (maybe_owner) |owner| {
            if (&owner.owned_lite_backend.?.native_docstore.?.file == file and
                (owner.query_borrows != 0 or owner.maintenance_references != 0 or owner.allocator_verification_references != 0 or owner.sql_cursors.count() != 0)) return;
        };
        try self.gate.writer_allocator_cache.prepare(file);
        for (donors) |maybe_donor| if (maybe_donor) |donor| {
            if (donor.query_borrows != 0 or donor.maintenance_references != 0 or donor.allocator_verification_references != 0 or donor.sql_cursors.count() != 0) continue;
            if (try file.takeVerifiedAllocator(&donor.owned_lite_backend.?.native_docstore.?.file)) return;
        };
        var inherited_base = false;
        if (file.ledger == null) for (donors) |maybe_donor| {
            const donor = maybe_donor orelse continue;
            if (donor.query_borrows != 0 or donor.maintenance_references != 0 or donor.allocator_verification_references != 0 or donor.sql_cursors.count() != 0) continue;
            if (try file.takeAllocatorRebaseBase(&donor.owned_lite_backend.?.native_docstore.?.file)) {
                inherited_base = true;
                break;
            }
        };
        // Preserve an explicit proof request when no cache was inherited.
        // In particular, never reset a recipient whose detached proof is live.
        if (inherited_base) _ = try file.inheritAllocatorCertificate();
    }

    /// Physical service can capture a newer file tip without opening a serving
    /// DB. The optional authority is borrowed under path admission and may
    /// donate only its exact verified allocator cache, never logical runtimes.
    fn refreshMaintenanceSnapshotAt(self: *Owner, foreground: *h.Handle, capture: MaintenanceCapture, physical_tip: ?*native.NativeFile) !*h.Handle {
        return self.refreshMaintenanceSnapshotNamedAt(foreground, capture, physical_tip, null);
    }

    fn refreshMaintenanceSnapshotNamedAt(self: *Owner, foreground: *h.Handle, capture: MaintenanceCapture, physical_tip: ?*native.NativeFile, name: ?[]const u8) !*h.Handle {
        const need_database = capture != .storage;
        std.debug.assert(physical_tip == null or !need_database);
        std.debug.assert(name == null or need_database);
        const file = physical_tip orelse &foreground.owned_lite_backend.?.native_docstore.?.file;
        self.immutable_sources.observe((try file.file.stat(file.runtime())).inode, @import("antfly_platform").time.monotonicNs());
        if (self.maintenance_snapshot) |snapshot| {
            const cached_store = snapshot.owned_lite_backend.?.native_docstore.?;
            const captured = &cached_store.file;
            const same_inode = (try file.file.stat(file.runtime())).inode == (try captured.file.stat(captured.runtime())).inode;
            const logical = file.activeCheckpoint().logical;
            const source_inputs_match = source: {
                if (capture != .source) break :source false;
                const stats = &self.publication_stats.text_runtime_cache;
                if (!same_inode) break :source stats.reject("miss_inode");
                if (!snapshot.db_live) break :source stats.reject("miss_closed");
                const cache = self.text_source_runtime_cache orelse break :source stats.reject("miss_no_cache");
                if (cache.root != snapshot) break :source stats.reject("miss_binding");
                break :source try cache.matchesObserved(file, stats);
            };
            // Artifact-only continuations cannot authorize source reuse. The
            // separate source cache additionally fences configurations and
            // reconstructs all mutable primary observations below.
            const retained_inputs_match = if (self.maintenance_retained_logical) |expected|
                capture != .source and snapshot.db_live and file.header.indexed_reclamation and captured.header.indexed_reclamation and
                    !std.mem.allEqual(u8, &expected.metadata, 0) and !std.mem.allEqual(u8, &expected.indexes, 0) and
                    std.mem.eql(u8, &expected.metadata, &logical.metadata) and std.mem.eql(u8, &expected.indexes, &logical.indexes)
            else
                false;
            const merge_borrows = if (name) |selected_name|
                !snapshot.embedded_tables.contains(selected_name) and if (self.pending_text_merge) |job| job.root == snapshot else false
            else
                false;
            if (!merge_borrows and same_inode and (!need_database or snapshot.db_live) and
                ((std.meta.eql(file.header, captured.header) and !snapshot.db_live) or retained_inputs_match or source_inputs_match))
            {
                // Primary commits do not invalidate immutable index continuations.
                // Rebind before taking this turn's fresh primary read epoch; a
                // changed catalog/index identity or inode always opens anew.
                if (!std.meta.eql(file.header, captured.header)) try cached_store.refreshIndexPublicationCheckpoint(file.header);
                if (source_inputs_match) {
                    errdefer {
                        self.clearTextSourceRuntimeCache();
                        self.maintenance_retained_logical = null;
                    }
                    try snapshot.db.refreshDetachedTextSource();
                    var tables = snapshot.embedded_tables.valueIterator();
                    while (tables.next()) |table| try table.*.db.refreshDetachedTextSource();
                    self.publication_stats.maintenance_source_runtime_reuses +|= 1;
                }
                try captured.inheritAllocatorVerification(file);
                if (physical_tip != null) _ = try captured.takeVerifiedAllocator(file);
                // A physical tip authenticates allocator state, never the
                // previous logical wrapper's SQL recovery observations.
                snapshot.sql_commit_recovery_needed = physical_tip != null or foreground.sql_commit_recovery_needed;
                snapshot.sql_ddl_recovery_needed = physical_tip != null or foreground.sql_ddl_recovery_needed;
                snapshot.sql_ddl_recovery_generation = foreground.sql_ddl_recovery_generation;
                self.configureRetirementBudget(cached_store);
                const selected_database = if (name) |selected_name| try self.openMaintenanceNamespace(snapshot, selected_name) else &snapshot.db;
                if (need_database) self.consumeImmutableTextHandoff(selected_database);
                return snapshot;
            }
        }
        self.clearTextSourceRuntimeCache();
        self.maintenance_retained_logical = null;
        var options = self.options;
        options.externally_locked = true;
        // Immutable artifact leases protect the exact input trees. A coarse
        // inode reader here would force every peer writer to append throughout
        // detached computation, even when it touches unrelated pages.
        options.artifact_reader_fence = false;
        options.allocator_verification_owner = file;
        if (physical_tip != null) options.allocator_state_donor = file;
        if (physical_tip == null and foreground.query_borrows == 0 and foreground.maintenance_references == 0 and
            foreground.allocator_verification_references == 0 and foreground.sql_cursors.count() == 0 and file.free_pages_verified)
            options.allocator_state_donor = file;
        // This owner gate and background-turn lease make the previous detached
        // cache quiescent. Authenticate its rebase before opening indexes can
        // publish leases; mutable allocator state is moved, never shared.
        var certified: ?native.NativeFile = null;
        defer if (certified) |*cache| cache.close();
        if (self.maintenance_snapshot) |old| {
            const previous = &old.owned_lite_backend.?.native_docstore.?.file;
            if (options.allocator_state_donor == null and old.maintenance_references == 0 and previous.ledger != null) {
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
            self.publication_stats.maintenance_runtime_opens +|= 1;
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
        snapshot.maintenance_runtime_references.store(1, .release);
        errdefer closeMaintenanceSnapshot(snapshot);
        snapshot.lease_snapshot = true;
        snapshot.sql_commit_recovery_needed = physical_tip != null or foreground.sql_commit_recovery_needed;
        snapshot.sql_ddl_recovery_needed = physical_tip != null or foreground.sql_ddl_recovery_needed;
        snapshot.sql_ddl_recovery_generation = foreground.sql_ddl_recovery_generation;
        const store = snapshot.owned_lite_backend.?.native_docstore.?;
        store.file.reader_coordinator = self.reader_coordinator;
        store.file.writer_allocator_cache = &self.gate.writer_allocator_cache;
        store.setArtifactPublicationGate(OwnedArtifactGate.view(self.gate));
        try store.file.inheritAllocatorVerification(file);
        // Named indexes must open before the old wrapper closes. The source
        // pool can then hand unchanged immutable ranges to the new runtime
        // without republishing one durable lease per segment. Mutable actors
        // are still freshly constructed, and inode/root identities fence each
        // pooled range. A failed open leaves the old snapshot owned here.
        const selected_database = if (name) |selected_name| try self.openMaintenanceNamespace(snapshot, selected_name) else &snapshot.db;
        if (need_database) self.consumeImmutableTextHandoff(selected_database);
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

    fn beginPendingTextMerge(self: *Owner, root: *h.Handle, database: *h.db_mod.DB, name: []const u8, catalog: *const PublicationCatalog, minimum_segments: u64) !bool {
        return self.beginPendingTextMergeImpl(root, database, name, catalog, minimum_segments, false);
    }

    /// Capture only immutable inventories/deletion state under the existing
    /// admitted checkpoint. The worker performs segment construction after this
    /// short capture; publication retains its independent exact-input checks.
    fn beginPendingTextMergeAssumeAdmitted(self: *Owner, root: *h.Handle, database: *h.db_mod.DB, name: []const u8, catalog: *const PublicationCatalog, minimum_segments: u64) !bool {
        return self.beginPendingTextMergeImpl(root, database, name, catalog, minimum_segments, true);
    }

    fn beginPendingTextMergeImpl(self: *Owner, root: *h.Handle, database: *h.db_mod.DB, name: []const u8, catalog: *const PublicationCatalog, minimum_segments: u64, comptime admission_held: bool) !bool {
        if (self.pending_text_merge != null) return false;
        if (!admission_held) self.gate.mutex.lockUncancelable(h.handleLockIo());
        if (self.maintenance_schedules.get(.{ .table_id = database.core.identity_namespace.table_id, .incarnation = database.root_incarnation })) |schedule| {
            for (database.core.index_manager.text_indexes.items) |*entry| {
                if (schedule.text_merge_caps.get(h.db_mod.types.indexConfigHash(entry.config))) |cap| entry.merge_segment_cap.store(cap, .release);
            }
        }
        if (!admission_held) self.gate.mutex.unlock(h.handleLockIo());
        var scratch: segment_mod.MergeScratchOptions = undefined;
        var config_hash: u64 = undefined;
        var scratch_directory: ?[]u8 = null;
        defer if (scratch_directory) |directory| alloc.free(directory);
        try database.lockApplyForPortableRuntime();
        var task = capture: {
            defer database.core.unlockApply();
            var captured = (try database.core.index_manager.beginTextMergeTaskAboveSegmentCount(minimum_segments)) orelse return false;
            errdefer {
                database.core.index_manager.cancelTextMergeTask(&captured);
                captured.deinit(alloc);
            }
            // Copy every mutable-catalog borrow before releasing apply. The
            // resource manager and IO belong to the retained runtime owner;
            // the path bytes belong to the pending job's arena below.
            config_hash = h.db_mod.types.indexConfigHash(database.core.index_manager.textIndexEntry(captured.index_name).?.config);
            scratch_directory = try alloc.dupe(u8, captured.persistent.privateScratchDirectory(".") orelse return error.Unsupported);
            scratch = .{
                .io = captured.persistent.io orelse return error.Unsupported,
                .directory = scratch_directory.?,
                .resource_manager = captured.persistent.writer.resource_manager,
            };
            // Fresh runtimes reconstruct deletion deltas from current active
            // inputs at publication; no scheduler registration stays borrowed.
            database.core.index_manager.cancelTextMergeTask(&captured);
            break :capture captured;
        };
        var task_owned = true;
        errdefer if (task_owned) task.deinit(alloc);
        const pending = try alloc.create(PendingTextMerge);
        pending.* = .{ .owner = self, .root = root, .database = database, .arena = .init(alloc), .catalog = catalog.*, .name = undefined, .task = task, .scratch = scratch, .config_hash = config_hash };
        task_owned = false;
        errdefer pending.deinit();
        const owned = pending.arena.allocator();
        pending.name = try owned.dupe(u8, name);
        pending.scratch.directory = try owned.dupe(u8, scratch.directory);
        for (&pending.catalog.paths, catalog.paths) |*path, original| path.* = try owned.dupe(u8, original);
        try pending.input_uses.ensureTotalCapacity(alloc, task.source.len);
        for (task.merge_indices) |index| {
            const source = task.snapshot.segments[index].reader.immutableSource();
            if (source != .ranges or source.ranges.immutable_identity == null) return error.Unsupported;
            if (!source.acquireUse()) return error.EnrichmentWaitCanceled;
            pending.input_uses.appendAssumeCapacity(source);
        }
        if (!admission_held) self.gate.mutex.lockUncancelable(h.handleLockIo());
        const retained = self.retainMaintenanceTurnAssumeLocked();
        if (!admission_held) self.gate.mutex.unlock(h.handleLockIo());
        if (!try retained) return error.EnrichmentWaitCanceled;
        errdefer {
            if (!admission_held) self.gate.mutex.lockUncancelable(h.handleLockIo());
            self.releaseMaintenanceTurnAssumeLocked();
            if (!admission_held) self.gate.mutex.unlock(h.handleLockIo());
        }
        pending.thread = try std.Thread.spawn(.{}, PendingTextMerge.run, .{pending});
        retainMaintenanceSnapshot(root);
        pending.owns_root = true;
        if (!admission_held) self.gate.mutex.lockUncancelable(h.handleLockIo());
        self.pending_text_merge = pending;
        self.publication_stats.text_merge_jobs_started +|= 1;
        self.publication_stats.text_merge_in_flight = true;
        if (!admission_held) self.gate.mutex.unlock(h.handleLockIo());
        return true;
    }

    fn adjustTextMergeCap(self: *Owner, pending: *PendingTextMerge, failed: bool) !void {
        return self.adjustTextMergeCapImpl(pending, failed, false);
    }

    fn adjustTextMergeCapImpl(self: *Owner, pending: *PendingTextMerge, failed: bool, comptime admission_held: bool) !void {
        if (!admission_held) self.gate.mutex.lockUncancelable(h.handleLockIo());
        defer if (!admission_held) self.gate.mutex.unlock(h.handleLockIo());
        const schedule = self.maintenance_schedules.getPtr(.{ .table_id = pending.database.core.identity_namespace.table_id, .incarnation = pending.database.root_incarnation }) orelse return;
        if (failed) {
            if (qualificationMetricsEnabled()) {
                const memory = pending.task.memoryObservation();
                std.log.info("antfly_bench_text_merge_budget_refusal sources={d} reserved_bytes={d} peak_task_bytes={d} denied={any} phase={s}", .{
                    pending.task.source.len,
                    memory.reserved_bytes,
                    memory.peak_task_bytes,
                    memory.denied,
                    if (pending.failure != null) "prepare" else if (pending.publication != null) "publication" else "staging",
                });
            }

            if (schedule.text_merge_caps.count() >= 64) schedule.text_merge_caps.clearRetainingCapacity();
            try schedule.text_merge_caps.put(alloc, pending.config_hash, @intCast(@max(2, pending.task.source.len / 2)));
            // The failed private worker still owes a merge. Recording its
            // smaller cap does not make an idle namespace runnable. Requeue
            // that debt explicitly rather than waiting for an unrelated write
            // or an opportunistic idle poll to rediscover it.
            schedule.artifact_pending = true;
            schedule.next_artifact_ns = 0;
            self.runnable_sources.notify(h.handleLockIo(), .{ .table_id = pending.database.core.identity_namespace.table_id, .incarnation = pending.database.root_incarnation });
            self.maintenance_pending = true;
            // Keep the work runnable without turning persistent memory
            // pressure into a zero-delay private-worker retry loop.
            self.maintenance_turn_deferred = true;
            self.wake.set(h.handleLockIo());
        } else _ = schedule.text_merge_caps.remove(pending.config_hash);
    }

    fn takePendingTextMergeAssumeLocked(self: *Owner) ?*PendingTextMerge {
        const pending = self.pending_text_merge orelse return null;
        self.pending_text_merge = null;
        self.publication_stats.text_merge_in_flight = false;
        if (pending.done.load(.acquire)) self.publication_stats.text_merge_prepare_ns +|= pending.elapsed_ns;
        if (pending.applied) self.publication_stats.text_merge_jobs_applied +|= 1 else self.publication_stats.text_merge_jobs_discarded +|= 1;
        self.releaseMaintenanceTurnAssumeLocked();
        self.maintenance_storage_pending = true;
        return pending;
    }

    fn discardPendingTextMerge(self: *Owner) void {
        const io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(io);
        const pending = self.takePendingTextMergeAssumeLocked();
        self.gate.mutex.unlock(io);
        // The cache and active turns have independent strong references.
        // Join immutable work and release its reference outside the gate.
        if (pending) |retired| retired.deinit();
    }

    /// One bounded lease append alternates with replay/physical service. The
    /// private output read stays outside admission. Only this construction's
    /// native value is changed; no stale DB/index cache is consulted.
    fn stagePendingTextMerge(self: *Owner, pending: *PendingTextMerge) !void {
        const started = @import("antfly_platform").time.monotonicNs();
        if (pending.failure) |err| return err;
        const result = &pending.result.?;
        if (result.run == null) {
            pending.ready = true;
            return;
        }
        // This is only an optimistic inode check. Reading an unfenced native
        // header here could race a foreground header write; actual checkpoint
        // reads and all publication validation follow path admission below.
        const tip = try std.Io.Dir.cwd().openFile(h.handleLockIo(), self.path, .{ .mode = .read_only });
        defer tip.close(h.handleLockIo());
        if ((try tip.stat(h.handleLockIo())).inode != pending.catalog.inode) return error.EnrichmentSourceChanged;
        const run = result.run.?;
        const offset = if (pending.construction) |construction| construction.length else 0;
        var bytes: [1024 * 1024]u8 = undefined;
        const count = @min(bytes.len, run.len() - offset);
        try (try run.sealedView()).source.readInto(offset, bytes[0..count]);
        const store = pending.root.owned_lite_backend.?.native_docstore.?;
        pending.root.maintenance_owner = self;
        store.writer_admission = .{ .ptr = pending.root, .enter = enterMaintenanceWriter, .leave = leaveMaintenanceWriter };
        defer {
            store.writer_admission = null;
            pending.root.maintenance_owner = null;
        }
        try enterMaintenanceWriter(pending.root, .allocator_service);
        defer leaveMaintenanceWriter(pending.root);
        try self.takeAvailableAllocator(&store.file);
        try pending.catalog.verifyNative(&store.file);
        if (!store.file.free_pages_verified) return error.EnrichmentWaitCanceled;
        if (pending.construction == null) pending.construction = try store.beginArtifactConstruction();
        try pending.construction.?.append(store, bytes[0..count]);
        self.publication_stats.text_merge_staged_bytes +|= count;
        self.publication_stats.text_merge_staging_ns +|= @import("antfly_platform").time.monotonicNs() -| started;
        if (pending.construction.?.length == run.len()) {
            try pending.construction.?.seal(run.len());
            pending.ready = true;
        }
        // Private construction changes no logical index/catalog identity.
        // Keep the runtime's original witnesses: allocator-service admission
        // may have adopted a foreign index publication without reloading its
        // LSM state. Certifying that newer header here would hide a stale
        // journal sequence from the next source/artifact reuse check.
    }

    fn finishPendingTextMerge(self: *Owner, root: *h.Handle, database: *h.db_mod.DB, name: []const u8) !bool {
        return self.finishPendingTextMergeImpl(root, database, name, false);
    }

    fn finishPendingTextMergeImpl(self: *Owner, root: *h.Handle, database: *h.db_mod.DB, name: []const u8, comptime admission_held: bool) !bool {
        const pending = self.pending_text_merge orelse return false;
        if (!pending.done.load(.acquire) or !pending.ready or !std.mem.eql(u8, pending.name, name)) return false;
        const started = @import("antfly_platform").time.monotonicNs();
        defer {
            if (!admission_held) self.gate.mutex.lockUncancelable(h.handleLockIo());
            self.publication_stats.text_merge_publication_ns +|= @import("antfly_platform").time.monotonicNs() -| started;
            if (!admission_held) self.gate.mutex.unlock(h.handleLockIo());
        }
        // Only admission owns the current file cut. Reject DDL/replacement
        // before linking any output to a logical segment name.
        try enterMaintenanceWriter(root, .index);
        pending.catalog.verifyNative(&root.owned_lite_backend.?.native_docstore.?.file) catch |err| {
            leaveMaintenanceWriter(root);
            return err;
        };
        leaveMaintenanceWriter(root);
        try database.lockApplyForPortableRuntime();
        const adopted = database.core.index_manager.adoptDetachedTextMergeTask(&pending.task) catch |err| {
            database.core.unlockApply();
            return err;
        };
        database.core.unlockApply();
        if (!adopted) return error.EnrichmentSourceChanged;
        defer database.core.index_manager.cancelTextMergeTask(&pending.task);
        const task_alloc = pending.task.mergeAllocator();
        if (pending.publication == null) {
            pending.publication = .{
                .owned_alloc = task_alloc,
                .source_map = pending.result.?.source_map,
                .publication_lookup_built = true,
                .unlinked_output_count = @intFromBool(pending.result.?.run != null),
                .elapsed_ns = pending.elapsed_ns,
            };
            pending.result.?.source_map = null;
        }
        const publication = &pending.publication.?;
        if (admission_held and !IndexManager.textMergePublicationLookupCurrent(&pending.task, publication)) {
            pending.lookup_needed = true;
            return false;
        }
        if (pending.construction) |*construction| {
            const entry = database.core.index_manager.textIndexEntry(pending.task.index_name) orelse return error.EnrichmentSourceChanged;
            if (pending.publication_root == null) {
                if (!admission_held) self.gate.mutex.lockUncancelable(h.handleLockIo());
                retainMaintenanceSnapshot(root);
                root.maintenance_references += 1;
                pending.publication_root = root;
                if (!admission_held) self.gate.mutex.unlock(h.handleLockIo());
            }
            publication.prepared_segments = try entry.persistent.prepareLinkedMergeSegment(task_alloc, root.owned_lite_backend.?.native_docstore.?, construction, &pending.key_range.?);
            publication.prepared_owner = &entry.persistent;
            pending.key_range = null;
        }
        // Resolve private provenance off the database-wide apply lock. Final
        // publication validates the source identities and deletion counters.
        if (!admission_held) try IndexManager.prepareTextMergeTaskPublicationLookup(&pending.task, publication);
        try database.lockApplyForPortableRuntime();
        defer database.core.unlockApply();
        const applied = try database.core.index_manager.finishTextMergeTask(&pending.task, publication);
        pending.applied = applied;
        if (applied) try self.adjustTextMergeCapImpl(pending, false, admission_held);
        return true;
    }

    /// Captures one table under the connection fence. The pinned runtime owns
    /// all provider/catalog state while computation runs outside that fence.
    /// Every short publication revalidates its inode and checkpoint.
    fn producerLoop(self: *Owner) void {
        const io = h.handleLockIo();
        while (true) {
            self.producer_wake.wait(io) catch {};
            self.producer_mutex.lockUncancelable(io);
            const pending = self.producer_mailbox;
            self.producer_mailbox = null;
            self.producer_wake.reset();
            self.producer_mutex.unlock(io);
            if (pending) |job| {
                job.execute();
                // No access to job follows this release. All provider futures
                // and their borrowed frames have drained before reclamation.
                job.done.store(true, .release);
                // A workerless owner may be waiting to drain this actor.
                // Reaping cannot depend on another owner-loop iteration.
                self.collectCompletedProducer();
                self.wake.set(io);
            }
            if (self.stopping.load(.acquire)) return;
        }
    }

    fn producerNeedsWork(database: *h.db_mod.DB) !bool {
        if (database.enrichment_runtime) |runtime| {
            const stats = runtime.stats();
            if (stats.retrying or stats.applied_sequence < @max(stats.target_sequence, database.core.nextDerivedSequence())) return true;
        }
        if (database.core.index_manager.resolvers.items.len != 0) {
            const state = h.db_mod.enrichment_state;
            const target = database.core.nextDerivedSequence();
            if (database.resolution_runtime) |runtime| {
                if (runtime.stats().catch_up_required) return true;
            } else if (try state.loadAppliedSequence(alloc, database.core.store, "resolution") < target) return true;
            if (database.promotion_runtime) |runtime| {
                if (runtime.stats().catch_up_required) return true;
            } else if (try state.loadAppliedSequence(alloc, database.core.store, "promotion") < target) return true;
            if (database.resolution_runtime == null and
                try @import("../storage/db/resolution_runtime.zig").hasDurableReresolveBacklog(database.core.store)) return true;
        }
        return false;
    }

    /// Transfer this admitted capture to the independently owned producer lane.
    /// Every fallible operation precedes exposing the frame to its worker.
    fn beginProducerAssumeAdmitted(self: *Owner, root: *h.Handle, database: *h.db_mod.DB, parent_state: *h.db_mod.DB.GroupSourceMaintenanceState, arena: *std.heap.ArenaAllocator, catalog: PublicationCatalog, source_read: ?SourceRead, publication: PreparedPublication, progress: ReplayProgress) !void {
        std.debug.assert(self.pending_producer == null and !parent_state.producer_inflight.load(.acquire));
        const job = try alloc.create(PendingProducer);
        errdefer alloc.destroy(job);
        job.* = .{ .owner = self, .root = root, .database = database, .parent_state = parent_state, .arena = arena, .catalog = catalog, .publication = publication, .source_read = source_read, .progress = progress };
        job.state.producer_replay_budget = parent_state.producer_replay_budget;
        job.state.resolution_backfill_turn.store(parent_state.resolution_backfill_turn.fetchXor(1, .monotonic), .monotonic);
        if (self.producer_worker == null)
            self.producer_worker = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Owner.producerLoop, .{self});
        if (!try self.retainMaintenanceTurnAssumeLocked()) return error.FileBusy;
        retainMaintenanceSnapshot(root);
        job.publication.catalog = &job.catalog;
        job.publication.source_read = if (job.source_read) |*read| read else null;
        job.publication.publication_overhead_ns = &job.state.publication_overhead_ns;
        root.maintenance_publication = &job.publication;
        database.borrowed_group_source_state = &job.state;
        database.async_context.replay_publication = .{ .ptr = &job.publication, .publish = PreparedPublication.publishReplay };
        if (database.enrichment_runtime) |runtime|
            runtime.generated_publication = .{ .ptr = &job.publication, .commit = PreparedPublication.commit, .heartbeat = PreparedPublication.heartbeat, .acquire_store = PreparedPublication.acquireStore };
        parent_state.producer_inflight.store(true, .release);
        self.pending_producer = job;
        _ = self.producer_jobs_active.fetchAdd(1, .release);
        self.publication_stats.producer_jobs_started +|= 1;
        if (qualificationMetricsEnabled()) std.log.info("antfly_bench_native_producer_start table_id={d}", .{publication.identity.table_id});
        // The selected actor is now producing, not consuming. Give its
        // independent consumer actor the next bounded ticket immediately.
        self.runnable_sources.notify(h.handleLockIo(), .{ .table_id = publication.identity.table_id, .incarnation = publication.incarnation });
        // The producer owns this actor exclusively. Consumer service must open
        // or reuse a separate actor rather than adopting its mutable file view.
        self.clearTextSourceRuntimeCache();
        self.maintenance_retained_logical = null;
        if (self.maintenance_snapshot == root) {
            self.maintenance_snapshot = null;
            closeMaintenanceSnapshot(root);
        }
        const io = h.handleLockIo();
        self.producer_mutex.lockUncancelable(io);
        std.debug.assert(self.producer_mailbox == null);
        self.producer_mailbox = job;
        self.producer_wake.set(io);
        self.producer_mutex.unlock(io);
    }

    /// The worker has drained before done is published. Retire actor/epoch
    /// resources outside admission; preserve uncertainty before losing a root.
    pub fn collectCompletedProducer(self: *Owner) void {
        const io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(io);
        const job = self.pending_producer orelse {
            self.gate.mutex.unlock(io);
            return;
        };
        if (!job.done.load(.acquire)) {
            self.gate.mutex.unlock(io);
            return;
        }
        if (needsOutcomeReconciliation(job.root)) self.preserveMaintenanceOutcomeFence();
        if (!needsOutcomeReconciliation(job.root)) self.preserveImmutableTextHandoff(job.root, job.database);
        self.pending_producer = null;
        self.publication_stats.producer_jobs_completed +|= 1;
        self.publication_stats.producer_service_ns +|= job.elapsed_ns;
        if (qualificationMetricsEnabled()) std.log.info("antfly_bench_native_producer_done table_id={d} advanced={any} elapsed_ns={d} error={s}", .{ job.publication.identity.table_id, job.advanced, job.elapsed_ns, if (job.failure) |err| @errorName(err) else "none" });
        _ = job.parent_state.publication_overhead_ns.fetchAdd(job.state.publication_overhead_ns.load(.monotonic), .monotonic);
        job.parent_state.producer_replay_budget = job.state.producer_replay_budget;
        job.parent_state.producer_inflight.store(false, .release);
        const key: MaintenanceKey = .{ .table_id = job.publication.identity.table_id, .incarnation = job.publication.incarnation };
        if (self.maintenance_schedules.getPtr(key)) |schedule| {
            schedule.database_pending = true;
            schedule.next_producer_ns = std.Io.Clock.awake.now(io).nanoseconds + if (job.advanced) @as(i96, 0) else std.time.ns_per_s;
        }
        if (job.failure) |err| self.noteMaintenanceFailure(err, key.table_id, .source);
        self.runnable_sources.notify(io, key);
        self.maintenance_pending = true;
        std.debug.assert(job.root.maintenance_references != 0);
        job.root.maintenance_references -= 1;
        self.releaseMaintenanceTurnAssumeLocked();
        self.gate.mutex.unlock(io);
        job.deinit();
        self.gate.mutex.lockUncancelable(io);
        self.maintenance_activity.finish(io);
        _ = self.producer_jobs_active.fetchSub(1, .release);
        self.gate.mutex.unlock(io);
    }

    fn maintenanceTurn(self: *Owner) !bool {
        self.collectCompletedProducer();
        const turn_started = @import("antfly_platform").time.monotonicNs();
        if (self.stopping.load(.acquire)) return false;
        // Independent background snapshots must not repeatedly invalidate each
        // other's index inputs between data publication and replay progress.
        // This lease coordinates background turns only; foreground callers
        // continue using their short path-writer leases during computation.
        const turn_io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(turn_io);
        self.maintenance_turn_deferred = false;
        if (self.hasUncertainOutcome()) {
            // Closing private readers needs no peer's background turn. Keep
            // the refusal on the serving wrapper after its source is retired,
            // including when refresh() would throw before returning a wrapper.
            self.preserveMaintenanceOutcomeFence();
            if (self.maintenance_turn_references == 0) self.discardMaintenanceRuntime();
            self.maintenance_turn_deferred = true;
            self.gate.mutex.unlock(turn_io);
            return error.OutcomeReconciliationRequired;
        }
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
        if (self.pending_text_merge) |pending| {
            if (pending.ready and pending.lookup_needed and !self.maintenance_last_merge_stage) {
                self.maintenance_last_merge_stage = true;
                IndexManager.prepareTextMergeTaskPublicationLookup(&pending.task, &pending.publication.?) catch |err| {
                    if (err == error.ResourceBudgetExceeded or err == error.MergeOutputTooLarge) try self.adjustTextMergeCap(pending, true);
                    self.discardPendingTextMerge();
                    return err;
                };
                pending.lookup_needed = false;
                return true;
            }
            if (pending.done.load(.acquire) and !pending.ready and !self.maintenance_last_merge_stage) {
                self.maintenance_last_merge_stage = true;
                self.stagePendingTextMerge(pending) catch |err| switch (err) {
                    error.EnrichmentWaitCanceled, error.FileBusy, error.WouldBlock => self.maintenance_turn_deferred = true,
                    else => {
                        if (needsOutcomeReconciliation(pending.root)) return err;
                        if (err == error.ResourceBudgetExceeded or err == error.MergeOutputTooLarge) try self.adjustTextMergeCap(pending, true);
                        self.discardPendingTextMerge();
                        return err;
                    },
                };
                if (self.pending_text_merge) |active| if (active.ready) {
                    self.gate.mutex.lockUncancelable(turn_io);
                    if (self.maintenance_schedules.getPtr(.{ .table_id = active.database.core.identity_namespace.table_id, .incarnation = active.database.root_incarnation })) |schedule| {
                        schedule.artifact_pending = true;
                        schedule.next_artifact_ns = 0;
                    }
                    self.runnable_sources.notify(turn_io, .{ .table_id = active.database.core.identity_namespace.table_id, .incarnation = active.database.root_incarnation });
                    self.gate.mutex.unlock(turn_io);
                };
                return true;
            }
        }
        self.maintenance_last_merge_stage = false;
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
        var retired_merge: ?*PendingTextMerge = null;
        defer if (retired_merge) |pending| pending.deinit();
        var gate_locked = true;
        defer if (gate_locked) self.gate.mutex.unlock(io);
        if (self.stopping.load(.acquire)) return false;
        self.expireImmutableTextHandoff(@import("antfly_platform").time.monotonicNs());
        // A foreground proof owns the admission lane. Refresh can open index
        // sources and publish lease metadata, so yield before even capturing
        // a runtime. The gate protects this flag through the capture boundary.
        if (self.allocator_verification_active) return true;
        var lease = try self.acquireMaintenancePathLeaseAssumeLocked();
        const turn_held_from = nowNs();
        var clock: StepClock = .{ .last = turn_held_from };
        var lease_held = true;
        defer if (lease_held) {
            clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.finish_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.finish_ns);
            lease.close();
            MaintenanceHoldSites.record(&self.publication_stats.maintenance_hold_sites.turn_capture, turn_held_from);
        };
        const foreground = try self.refresh();
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.refresh_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.refresh_ns);
        if (needsOutcomeReconciliation(foreground)) {
            self.discardMaintenanceRuntime();
            self.maintenance_turn_deferred = true;
            return false;
        }
        const foreground_file = &foreground.owned_lite_backend.?.native_docstore.?.file;
        // Settled caches own only immutable artifact inputs, never a primary
        // epoch. Check even idle audit turns before their readiness fast path:
        // a peer publication must release stale leases without needing another
        // source mutation or waiting for the periodic database audit.
        if (self.text_source_runtime_cache) |cache| {
            if (!try cache.matchesObserved(foreground_file, &self.publication_stats.text_runtime_cache))
                self.discardMaintenanceRuntime();
        }
        // Marker service is directory-bounded and needs no allocator proof.
        // Reap crash/normal-exit leftovers even if this short-lived owner is
        // canceled before its detached graph walk can finish.
        if (self.options.open_mode == .writer)
            try self.artifact_marker_sweep.run(foreground_file);
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.marker_sweep_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.marker_sweep_ns);
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
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.recover_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.recover_ns);
        // A ready result can outlive its namespace. Validate before selection
        // so a dropped table cannot strand its job forever. Publish ready
        // merges before source retries which may need their segment capacity.
        const publish_ready_merge = if (self.pending_text_merge) |active| active.ready else false;
        if (publish_ready_merge) {
            self.pending_text_merge.?.catalog.verifyNative(foreground_file) catch |err| {
                lease.close();
                lease_held = false;
                self.gate.mutex.unlock(io);
                gate_locked = false;
                self.discardPendingTextMerge();
                return err;
            };
        }
        // Catalog identities suffice for rotation and dropped-table pruning.
        // Opening every runtime here makes each bounded turn O(all indexes),
        // delaying the next group lease observation behind unrelated opens.
        try @import("tables.zig").loadCatalog(foreground);
        // Priority tickets are immutable hints. Resolve them against this
        // admitted catalog; dropped/recreated namespaces cannot borrow state
        // from the incarnation that originally queued the ticket.
        var priority_name: ?[]const u8 = if (publish_ready_merge) self.pending_text_merge.?.name else null;
        if (!publish_ready_merge) if (self.runnable_sources.choose(io)) |key| {
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
        };
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
                // The independent producer owns this scheduling state until
                // its old-incarnation publication has drained or been refused.
                if (self.pending_producer) |job| if (job.parent_state == entry.value_ptr.group_sources) continue;
                if (entry.value_ptr.group_sources) |state| {
                    state.deinit();
                    alloc.destroy(state);
                }
                if (entry.value_ptr.name) |name| alloc.free(name);
                entry.value_ptr.text_merge_caps.deinit(alloc);
                self.maintenance_schedules.removeByPtr(entry.key_ptr);
            };
        } else self.publication_stats.maintenance_priority_turns +|= 1;
        const count = foreground.embedded_table_records.count() + 1;
        const ordinal = self.maintenance_table_cursor % count;
        if (audit) self.maintenance_table_cursor = (ordinal + 1) % count;
        const audit_round_end = audit and self.maintenance_table_cursor == 0;
        var database = &foreground.db;
        var selected: ?*@import("tables.zig").Table = null;
        var selected_root: ?*h.Handle = null;
        defer if (selected_root) |root| {
            // Control-only early returns must not leave a newly opened index
            // owner retaining inputs through an otherwise idle poll interval.
            const merge_borrows = if (self.pending_text_merge) |job| job.root == root else false;
            if (!merge_borrows and self.maintenance_snapshot == root and root.db_live and root.maintenance_references == 0 and
                self.text_source_runtime_cache == null and self.maintenance_retained_logical == null)
            {
                @import("tables.zig").closeAll(root);
                root.db.closeImmutableSnapshot();
                root.db_live = false;
            }
        };
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
            // Selection and detached replay use one namespace runtime. Opening
            // it on the serving wrapper first duplicated all index/provider
            // startup merely to discover this turn's work.
            const namespace_root = if (self.options.profile == .native and foreground.embedded_tables.contains(name))
                foreground
            else if (self.options.profile == .native) blk: {
                selected_root = try self.refreshMaintenanceSnapshotNamed(foreground, .source, name);
                // A merge can co-own the selected runtime. A namespace switch
                // must retire that whole cache reference, never close the
                // table whose immutable merge inputs still borrow its owner.
                if (!selected_root.?.embedded_tables.contains(name)) {
                    if (self.pending_text_merge) |job| if (job.root == selected_root.?) {
                        self.clearTextSourceRuntimeCache();
                        self.maintenance_retained_logical = null;
                        self.maintenance_snapshot = null;
                        closeMaintenanceSnapshot(selected_root.?);
                        selected_root = try self.refreshMaintenanceSnapshotNamed(foreground, .source, name);
                    };
                }
                const selected_store = selected_root.?.owned_lite_backend.?.native_docstore.?;
                selected_store.writer_admission = null;
                selected_store.file.replay_consumer_admission = false;
                // The outer admission already owns this writer. Detached
                // callbacks are installed only after control work is captured.
                break :blk selected_root.?;
            } else foreground;
            database = (if (self.options.profile == .native and namespace_root != foreground) self.openMaintenanceNamespace(namespace_root, name) else @import("tables.zig").get(namespace_root, name)) catch return true;
            selected = namespace_root.embedded_tables.get(name).?;
        };
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.catalog_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.catalog_ns);
        const selection_root = selected_root orelse foreground;
        const selection_file = &selection_root.owned_lite_backend.?.native_docstore.?.file;
        var namespace_pending = self.options.open_mode == .writer and try hasPendingReplay(database);
        const entry_sequence = database.core.nextDerivedSequence();
        const readiness_namespace = if (selected) |table| table.namespace else "root";
        const entry_readiness = if (self.options.profile == .native) try namespaceReadiness(selection_root, readiness_namespace) else null;
        var authority_arena = std.heap.ArenaAllocator.init(alloc);
        defer authority_arena.deinit();
        const projection_pending = self.options.open_mode == .writer and try @import("resolver_authority.zig").hasProjectionDebt(selection_root, database, authority_arena.allocator());
        if (projection_pending) namespace_pending = true;
        const group_state = try self.groupSourceStateNamed(database, if (selected) |table| table.name else "default");
        // Conservative legacy turns do not dispatch an independent actor.
        // They must keep servicing producers through their complete turn.
        group_state.external_producers.store(self.options.profile == .native and selection_file.header.indexed_reclamation, .release);
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.debt_checks_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.debt_checks_ns);
        const allocator_stats = stats: {
            const foreground_store = selection_root.owned_lite_backend.?.native_docstore.?;
            foreground_store.mutex.lock();
            defer foreground_store.mutex.unlock();
            break :stats try selection_file.allocatorStats();
        };
        if (allocator_stats) |ledger| {
            group_state.replay_budget.updatePressure(ledger.pending_data_objects, ledger.reusable_pages, selection_file.allocatorWorkSnapshot().reuse_blocked_append_pages);
            group_state.producer_replay_budget.updatePressure(ledger.pending_data_objects, ledger.reusable_pages, selection_file.allocatorWorkSnapshot().reuse_blocked_append_pages);
            // Bound each service publication independently, increasing useful
            // work when foreground retirement is outpacing the baseline turn.
            const work_pages: usize = @intCast(@min(512, 128 + ledger.pending_data_objects / 8));
            self.publication_stats.retirement_work_pages = work_pages;
            self.configureRetirementBudget(selection_root.owned_lite_backend.?.native_docstore.?);
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
        if (!publish_ready_merge and self.options.open_mode == .writer and schedule.source_recovery_next and now >= schedule.next_source_recovery_ns) {
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
        var group_source_turn = false;
        if (!publish_ready_merge and self.options.open_mode == .writer and schedule.has_group_sources and schedule.group_source_next and
            now >= schedule.next_group_source_ns)
        {
            // Group queues can advance through cursor/lease observations
            // without appending a journal record. Their poll deadline is an
            // independent wake source, not evidence that providers must run.
            schedule.next_group_source_ns = now + 100 * std.time.ns_per_ms;
            if (self.options.profile == .native) {
                // A worker can submit several control commands. Capture its
                // immutable runtime here, then discover/fan in off admission
                // and admit commands individually through PreparedPublication.
                group_source_turn = true;
                schedule.group_source_next = false;
            } else if (try database.runGroupSourceMaintenanceSlice(group_state, null)) {
                schedule.group_source_next = false;
                self.runnable_sources.notify(io, .{ .table_id = database.core.identity_namespace.table_id, .incarnation = database.root_incarnation });
                schedule.database_pending = true;
                self.notifyMutation();
                return true;
            }
        }
        if (!group_source_turn) schedule.group_source_next = true;
        var ttl_more = false;
        if (!publish_ready_merge and now >= schedule.next_ttl_ns) {
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
        const covered_controls = if (self.options.profile == .native and entry_readiness != null)
            try coveredTextControls(selection_root, database, readiness_namespace, entry_readiness.?)
        else
            null;
        var observed_covered_source = false;
        const covered_source = !publish_ready_merge and !namespace_pending and !schedule.database_pending and
            !schedule.startup_replay_pending and now < schedule.next_database_audit_ns and
            covered_controls != null and schedule.covered_text_controls != null and
            std.mem.eql(u8, &covered_controls.?, &schedule.covered_text_controls.?);
        if (covered_source) {
            if (entry_sequence != schedule.serviced_sequence) {
                // Every durable consumer was checked at this same admitted
                // tip. Only source observation advances here; retained journal
                // bytes/credits remain owned by physical retirement.
                schedule.serviced_sequence = entry_sequence;
                schedule.readiness = entry_readiness;
                observed_covered_source = true;
                self.publication_stats.covered_source_observations +|= 1;
                if (!schedule.artifact_pending) schedule.next_artifact_ns = now + 100 * std.time.ns_per_ms;
                schedule.artifact_pending = true;
            }
            // Producer headroom must not wait for an optional housekeeping
            // deadline. This uses the very same window/tier policy as capture.
            if (try database.core.index_manager.nativeTextMaintenanceNeedsMerge()) schedule.next_artifact_ns = now;
            if (try selection_file.replayAdmission()) |debt| {
                if (debt.bytes >= @max(@as(u64, 1), @min(1024 * 1024, debt.limits.bytes / 4)) or
                    debt.records >= @max(@as(u64, 1), @min(64, debt.limits.records / 4))) schedule.next_artifact_ns = now;
            }
        }
        const ordinary_source_due = if (self.options.profile == .native)
            namespace_pending or schedule.database_pending or entry_sequence != schedule.serviced_sequence or
                database.core.nextDerivedSequence() != entry_sequence or now >= schedule.next_database_audit_ns
        else
            self.maintenance_pending;
        const run_source = !publish_ready_merge and (group_source_turn or ordinary_source_due);
        const run_database = publish_ready_merge or run_source or (schedule.artifact_pending and now >= schedule.next_artifact_ns);
        var serving_physical_serviced = false;
        var serving_retirement_more = false;
        var serving_index_more = false;
        if (run_database and !run_source and covered_source and selected_root == null and
            self.options.open_mode == .writer)
        {
            // Covered native text needs physical service, not a replacement
            // DB/provider runtime. Keep this bounded metadata-only slice under
            // the existing coordinator/path lease; merge construction still
            // uses independently retained immutable sources outside admission.
            self.publication_stats.serving_physical_turns +|= 1;
            schedule.next_artifact_ns = now + 100 * std.time.ns_per_ms;
            const previous_state = database.borrowed_group_source_state;
            database.borrowed_group_source_state = group_state;
            defer database.borrowed_group_source_state = previous_state;
            const debt = try selection_file.replayAdmission();
            const pressure = if (debt) |value|
                value.bytes >= @max(@as(u64, 1), @min(1024 * 1024, value.limits.bytes / 4)) or
                    value.records >= @max(@as(u64, 1), @min(64, value.limits.records / 4))
            else
                false;
            serving_retirement_more = try database.runBackgroundTurnWithCancellation(h.db_mod.types.CancellationToken.fromAtomic(&self.stopping), if (pressure) .retirement else .retirement_batched);
            serving_index_more = try database.core.index_manager.serviceNativeTextStorageSlice();
            serving_physical_serviced = true;
            schedule.artifact_pending = serving_retirement_more or serving_index_more;
            self.maintenance_storage_pending = true;
            if (self.pending_text_merge != null or !try database.core.index_manager.nativeTextMaintenanceNeedsMerge()) {
                self.publication_stats.maintenance_database_turns +|= 1;
                self.publication_stats.maintenance_artifact_only_turns +|= 1;
                return self.finishMaintenanceVisit(false, true, audit_round_end);
            }
            // Capture immutable merge inputs from the checkpoint *after* the
            // serving physical slice. Never let stale detached cleanup reject
            // a merge before construction even starts. Publication still
            // validates the owned task's exact inputs and concurrent deletes.

        }
        const captured_artifact_pending = schedule.artifact_pending;
        if (run_source) schedule.database_pending = true;
        if (run_database) {
            // Artifact continuations need a catalog capture, not source replay.
            self.publication_stats.maintenance_database_turns +|= 1;
            if (run_source) self.publication_stats.maintenance_source_turns +|= 1 else self.publication_stats.maintenance_artifact_only_turns +|= 1;
        } else self.publication_stats.maintenance_idle_namespace_turns +|= 1;
        // A storage-only visit has no reason to borrow the selected DB after
        // releasing admission. Foreground refresh may retire that DB while the
        // raw storage owner is detached. Queue physical work in its dedicated
        // lane instead; it owns no provider/index runtime or borrowed DB state.
        if (self.options.profile == .native and !run_database) {
            if (observed_covered_source) self.maintenance_storage_pending = true;
            return self.finishMaintenanceVisit(false, false, audit_round_end);
        }
        // Immutable text construction runs independently of the replay actor;
        // publication and native staging remain bounded maintenance slices.

        const turn_arena = try alloc.create(std.heap.ArenaAllocator);
        turn_arena.* = .init(alloc);
        var turn_arena_owned = true;
        defer if (turn_arena_owned) {
            turn_arena.deinit();
            alloc.destroy(turn_arena);
        };
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
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.schedule_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.schedule_ns);
        const root = selected_root orelse if (independent) try self.refreshMaintenanceSnapshotNamed(foreground, if (run_source) .source else if (run_database) .artifacts else .storage, if (selected) |table| table.name else null) else foreground;
        if (independent and run_database) {
            database = if (selected) |table| try @import("tables.zig").get(root, table.name) else &root.db;
            // Immutable inputs retain durable range leases throughout replay;
            // the separate primary snapshot owns its cross-process epoch.
            root.owned_lite_backend.?.native_docstore.?.artifact_reader_fence = false;
        }
        const publication_name = try turn_alloc.dupe(u8, if (selected) |table| table.name else "default");
        const index_prefix = try std.fmt.allocPrint(turn_alloc, "{s}/", .{database.core.index_manager.base_path});
        // Replay may lazily open reads against this captured primary tip.
        // Retain its data frontier through the bounded replay window; artifact
        // leases and catalog fingerprints do not protect these primary roots.
        var source_read = if (run_source and !group_source_turn) try database.core.store.runtime_store.beginRead() else null;
        defer if (source_read) |*read| read.abort();
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.snapshot_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.snapshot_ns);
        var catalog = if (run_database) try PublicationCatalog.captureForStoragePublication(turn_alloc, root, database, publication_name) else null;
        defer if (catalog) |*proof| proof.deinit();
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.publication_catalog_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.publication_catalog_ns);
        // Artifact/control turns may continue a previously validated source
        // runtime. They cannot promote an artifact-only runtime into one.
        const had_source_runtime = if (self.text_source_runtime_cache) |cache| cache.root == root else false;
        var source_runtime_cache = if (independent and root.db_live and (run_source or had_source_runtime) and shadow_scopes.items.len == 0) try TextSourceRuntimeCache.captureObserved(root, &self.publication_stats.text_runtime_cache) else null;
        defer if (source_runtime_cache) |cache| cache.destroy();
        var publication: PreparedPublication = .{ .owner = self, .name = publication_name, .identity = database.core.identity_namespace, .incarnation = database.root_incarnation, .catalog = if (catalog) |*proof| proof else null, .index_prefix = index_prefix, .shadow_scopes = shadow_scopes.items, .snapshot_root = root, .snapshot_database = database, .publication_overhead_ns = &group_state.publication_overhead_ns, .source_read = if (source_read) |*read| read else null, .relational_rows = if (database.enrichment_runtime) |runtime| runtime.config.relational_base_rows else false };
        const managed = try database.core.index_manager.managedIndexes(turn_alloc);
        const captured_file = &root.owned_lite_backend.?.native_docstore.?.file;
        if (run_source and captured_file.header.indexed_reclamation and captured_file.header.indexed_revision >= 12) {
            const scopes = try turn_alloc.alloc(PreparedPublication.IndexScope, managed.len);
            for (managed, scopes) |index, *scope| {
                const domain = try std.fmt.allocPrint(turn_alloc, "{s}/indexes/{s}", .{ database.core.index_manager.base_path, index.name });
                scope.* = .{ .name = index.name, .domain = domain, .identity = try captured_file.indexScopeIdentity(domain) };
            }
            publication.index_scopes = scopes;
        }
        const replay_progress = if (run_source) try ReplayProgress.capture(turn_alloc, database, managed) else null;
        if (serving_physical_serviced and !run_source and self.pending_text_merge == null) {
            // Task capture can read native deletion metadata. Capture it while
            // this checkpoint still owns writer admission, rather than racing
            // each foreground append before the worker has even started.
            // The new runtime is unpublished and has no admission callbacks
            // installed yet, so these bounded reads cannot re-enter the gate.
            std.debug.assert(!root.owned_lite_backend.?.native_docstore.?.index_reads_require_admission);
            if (try self.beginPendingTextMergeAssumeAdmitted(root, database, publication_name, &catalog.?, 0))
                self.publication_stats.admitted_merge_captures +|= 1;
        }
        // Keep the primary epoch until runBackgroundTurn returns and every
        // replay cursor has closed. Preparing the last text segment does not
        // end the journal cursor's borrowing lifetime.
        if (independent) retainMaintenanceSnapshot(root);
        defer if (independent) closeMaintenanceSnapshot(root);
        root.maintenance_references += 1;
        self.maintenance_activity.begin();
        var active_runtime_borrow = true;
        defer if (active_runtime_borrow) {
            std.debug.assert(gate_locked);
            root.maintenance_references -= 1;
            self.maintenance_activity.finish(io);
        };
        root.maintenance_owner = self;
        root.maintenance_publication = if (run_database) &publication else null;
        const store = root.owned_lite_backend.?.native_docstore.?;
        errdefer {
            root.maintenance_owner = null;
            root.maintenance_publication = null;
            store.writer_admission = null;
        }
        store.file.replay_consumer_admission = true;
        const split_index_work = independent and store.file.header.indexed_reclamation;
        if (split_index_work) {
            if (comptime h.builtin.is_test) if (self.test_text_merge_segment_limit) |limit| {
                if (database.text_merge_runtime == null) try database.initOptionalTextMergeRuntime(.{
                    .enabled = true,
                    .max_pending_segments = limit,
                    .max_pending_bytes = 0,
                    .manual_producer_assistance = true,
                    .defer_producer_to_owner = true,
                });
            };
            if (database.text_merge_runtime) |runtime| {
                runtime.config.defer_producer_to_owner = true;
                if (comptime h.builtin.is_test) if (self.test_text_merge_segment_limit) |limit| {
                    runtime.config.enabled = true;
                    runtime.config.max_pending_segments = limit;
                    runtime.config.max_pending_bytes = 0;
                };
            }
        }
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
        const publication_overhead_before = group_state.publication_overhead_ns.load(.monotonic);
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.setup_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.setup_ns);
        if (publish_ready_merge and split_index_work and !run_source) {
            // Immutable output and source-coordinate lookup were prepared off
            // admission. Reconstruct current source deletions on this fresh
            // runtime, then publish against the same admitted checkpoint.
            // Unrelated appends are preserved; only new source deletions request
            // another off-admission lookup slice.
            self.publication_stats.ready_merge_publication_turns +|= 1;
            const started = nowNs();
            var completed_merge = false;
            var merge_error: ?anyerror = null;
            std.debug.assert(root.maintenance_writer_references == 0 and root.maintenance_writer_lease == null);
            root.maintenance_writer_references = 1;
            {
                defer {
                    std.debug.assert(root.maintenance_writer_references == 1);
                    root.maintenance_writer_references = 0;
                }
                const result: anyerror!bool = result: {
                    self.takeAvailableAllocator(&store.file) catch |err| break :result err;
                    store.prepareWriterAllocatorAssumeAdmitted() catch |err| break :result err;
                    if (!store.file.free_pages_verified) break :result error.EnrichmentWaitCanceled;
                    break :result self.finishPendingTextMergeImpl(root, database, publication_name, true);
                };
                completed_merge = result catch |err| failed: {
                    merge_error = err;
                    // An unpublished alias candidate may own native cleanup.
                    // Dispose it while this same borrowed admission is live;
                    // the remaining private task can retire outside the gate.
                    if (self.pending_text_merge) |pending| if (pending.publication) |*prepared| {
                        if (prepared.prepared_segments.len != 0) {
                            prepared.deinit(alloc);
                            pending.publication = null;
                        }
                    };
                    break :failed false;
                };
            }
            const retain_refusal = if (merge_error) |err| switch (err) {
                error.EnrichmentWaitCanceled, error.FileBusy, error.WouldBlock, error.IndexApplyBusy => if (self.pending_text_merge) |pending|
                    pending.key_range != null and !needsOutcomeReconciliation(root)
                else
                    false,
                else => false,
            } else false;
            if (retain_refusal) self.maintenance_turn_deferred = true;
            if (merge_error) |err| {
                if (err == error.ResourceBudgetExceeded or err == error.MergeOutputTooLarge) {
                    if (self.pending_text_merge) |pending| self.adjustTextMergeCapImpl(pending, true, true) catch {};
                }
                self.noteMaintenanceFailure(err, publication.identity.table_id, .artifacts);
            }
            const lookup_pending = if (self.pending_text_merge) |pending| pending.lookup_needed else false;
            if (completed_merge or (merge_error != null and !retain_refusal)) retired_merge = self.takePendingTextMergeAssumeLocked();
            if (retired_merge) |finished| if (finished.applied and
                (namespace_pending or schedule.database_pending or schedule.startup_replay_pending))
            {
                // A capacity yield advanced neither coverage nor a fragment,
                // so it earned no source ticket. Publishing the merge makes
                // that source runnable again. Waiting for a catalog audit here
                // strands its retained file-wide credits behind hot siblings.
                schedule.database_pending = true;
                self.runnable_sources.notify(io, .{ .table_id = publication.identity.table_id, .incarnation = publication.incarnation });
                self.maintenance_pending = true;
            };
            schedule.artifact_pending = true;
            schedule.next_artifact_ns = std.Io.Clock.awake.now(io).nanoseconds + if (lookup_pending) @as(i96, 0) else 100 * std.time.ns_per_ms;
            database.borrowed_group_source_state = null;
            root.maintenance_publication = null;
            store.writer_admission = null;
            root.maintenance_owner = null;
            self.publication_stats.publication_hold_ns +|= @intCast(@max(0, nowNs() - started));
            self.publication_stats.maintenance_capture_ns +|= @import("antfly_platform").time.monotonicNs() -| turn_started;
            self.maintenance_storage_pending = true;
            clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.ready_merge_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.ready_merge_ns);
            return self.finishMaintenanceVisit(lookup_pending, true, audit_round_end);
        }
        clock.mark(&self.publication_stats.maintenance_hold_sites.capture_steps.finish_ns, &self.publication_stats.maintenance_hold_sites.capture_max_steps.finish_ns);
        if (split_index_work and run_source and !group_source_turn and !projection_pending and shadow_scopes.items.len == 0 and
            self.pending_producer == null and now >= schedule.next_producer_ns and try producerNeedsWork(database))
        {
            try self.beginProducerAssumeAdmitted(root, database, group_state, turn_arena, catalog.?, source_read, publication, replay_progress.?);
            // The job now owns the publication frame, primary epoch and active
            // runtime borrow. The ordinary turn retains none of those borrows.
            turn_arena_owned = false;
            catalog = null;
            source_read = null;
            active_runtime_borrow = false;
            self.maintenance_pending = true;
            schedule.database_pending = true;
            return self.finishMaintenanceVisit(true, false, audit_round_end);
        }
        lease.close();
        MaintenanceHoldSites.record(&self.publication_stats.maintenance_hold_sites.turn_capture, turn_held_from);
        lease_held = false;
        self.gate.mutex.unlock(io);
        gate_locked = false;

        if (comptime h.builtin.is_test) if (self.test_after_capture) |hook| hook.notify(hook.ptr, root);
        const detached_started = @import("antfly_platform").time.monotonicNs();
        const cancellation = h.db_mod.types.CancellationToken.fromAtomic(&self.stopping);
        if (run_database) database.graph_replacement_admission = .{ .ptr = root, .enter = enterReplacementWriter, .leave = leaveMaintenanceWriter };
        var maintenance_error: ?anyerror = null;
        var source_capacity_yield = false;
        var maintenance_error_phase: MaintenanceFailurePhase = .source;
        // Source preparation reads an independently retained immutable cut.
        // Verifying allocation on that old wrapper would require a quiet tip
        // before read-only work could start. Indexed publication instead rebases
        // and authenticates the current allocator under its writer admission.
        // Legacy files still need their detached graph proof.
        if (run_source and !store.file.header.indexed_reclamation) store.prepareAllocatorVerification(&self.verification_cancel) catch |err| {
            maintenance_error = err;
        };
        const proof_finished = @import("antfly_platform").time.monotonicNs();
        // A shadow owns disjoint unpublished index inputs. Give it a bounded
        // turn before replay, whose serving-index publication invalidates the
        // broad capture. Unrelated serving writes may rebase this turn, while
        // changes to any selected shadow generation still reject publication.
        const projected = if (run_source and !group_source_turn and maintenance_error == null) @import("resolver_authority.zig").pullProjection(root, database, turn_alloc) catch |err| blk: {
            if (err == error.TextMergeProducerDeferred) {
                source_capacity_yield = true;
                break :blk false;
            }
            maintenance_error = err;
            break :blk false;
        } else false;
        var retain_artifact_runtime = !run_database and root.db_live and self.maintenance_retained_logical != null;
        var group_source_changed = false;
        var pending = if (maintenance_error != null or source_capacity_yield) true else if (group_source_turn) group: {
            // Queries pin fresh primary cuts after admission. Command CAS and
            // group lease checks execute again inside their individual writer
            // admission; the source/ack cursor cannot authorize stale effects.
            const previous = store.primary_reads_require_admission;
            store.primary_reads_require_admission = true;
            defer store.primary_reads_require_admission = previous;
            if (comptime h.builtin.is_test) if (self.test_before_group_source) |hook| hook.notify(hook.ptr);
            group_source_changed = database.runGroupSourceMaintenanceSlice(group_state, .{ .ptr = &publication, .submit_fn = PreparedPublication.submitGroupSource, .submit_many_fn = PreparedPublication.submitGroupSourceControls }) catch |err| {
                maintenance_error = err;
                break :group true;
            };
            // An idle control poll must not manufacture provider replay debt.
            // Keep a pre-existing ordinary turn, or wake it for committed work.
            break :group ordinary_source_due or group_source_changed;
        } else if (run_source and !projected) database.runBackgroundTurnWithCancellation(cancellation, if (shadow_scopes.items.len != 0) .shadow else if (split_index_work) .replay else .complete) catch |err| blk: {
            // Capacity is scheduler control flow, not a stale source. Keep
            // its immutable runtime and service the merges that return credits.
            // Treating it as a source failure skips that service indefinitely.
            if (err == error.TextMergeProducerDeferred) {
                source_capacity_yield = true;
                break :blk true;
            }
            maintenance_error = err;
            break :blk true;
        } else projected;
        const replay_finished = @import("antfly_platform").time.monotonicNs();
        const replay_publication_overhead = group_state.publication_overhead_ns.load(.monotonic) -| publication_overhead_before;
        // Capture source eligibility before retirement/LSM housekeeping adds
        // its own continuation. An artifact-only turn must never re-run source
        // providers, replay, relational sweeps or status snapshot publication.
        const source_more = run_source and (pending or projected or publication.index_inputs_changed or maintenance_error != null);
        const source_completed = run_source and !group_source_turn and maintenance_error == null and !source_capacity_yield;
        // Another consumer may have advanced before a sibling capacity yield.
        // Its durable fragment/coverage is useful progress even though the
        // complete source round must remain pending.
        const source_advanced = if (run_source and !group_source_turn and maintenance_error == null) replay_progress.?.advanced(database) else false;
        if (run_source and qualificationMetricsEnabled()) std.log.info("antfly_bench_native_source_turn table_id={d} group={any} advanced={any} pending={any} capacity_yield={any} elapsed_ns={d} error={s}", .{ publication.identity.table_id, group_source_turn, source_advanced, source_more, source_capacity_yield, replay_finished -| detached_started, if (maintenance_error) |err| @errorName(err) else "none" });
        // The schedule map can grow while the owner gate is released.
        var artifact_more = !run_database and captured_artifact_pending;
        var artifact_delay_ns: u64 = 100 * std.time.ns_per_ms;
        if (run_database) database.graph_replacement_admission = null;
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
            // Control maintenance also uses primary point probes. Its source
            // epoch is gone, so rebase and pin their current checkpoint before
            // looking up repair summaries or other control records.
            store.primary_reads_require_admission = true;
            defer store.primary_reads_require_admission = false;
            // Revalidate the current catalog and consumer fences while holding
            // path admission through a bounded journal-retirement transaction.
            const retirement_more = if (serving_physical_serviced) serving_retirement_more else blk: {
                enterMaintenanceWriter(root, .index) catch |err| {
                    maintenance_error = err;
                    maintenance_error_phase = .retirement;
                    break :blk true;
                };
                defer leaveMaintenanceWriter(root);
                // Batch physical journal deletion independently of each source
                // wake. A small/tight journal bypasses the delay before its
                // durable admission budget can obstruct another producer.
                const debt = store.file.replayAdmission() catch |err| {
                    maintenance_error = err;
                    maintenance_error_phase = .retirement;
                    break :blk true;
                };
                const pressure = if (debt) |value|
                    value.bytes >= @max(@as(u64, 1), @min(1024 * 1024, value.limits.bytes / 4)) or
                        value.records >= @max(@as(u64, 1), @min(64, value.limits.records / 4))
                else
                    false;
                break :blk database.runBackgroundTurnWithCancellation(cancellation, if (pressure) .retirement else .retirement_batched) catch |err| {
                    maintenance_error = err;
                    maintenance_error_phase = .retirement;
                    break :blk true;
                };
            };
            artifact_more = retirement_more;
            pending = pending or retirement_more;
            // Capacity-restoring construction is independent of optional
            // control/LSM service. Capture it first: a graph reader fence or
            // another optional service refusal must not prevent the text
            // merge that makes the source and its journal credits runnable.
            if (maintenance_error == null and self.pending_text_merge == null) {
                const started = self.beginPendingTextMerge(root, database, publication_name, &catalog.?, 0) catch |err| failed: {
                    maintenance_error = err;
                    maintenance_error_phase = .artifacts;
                    break :failed false;
                };
                artifact_more = artifact_more or started;
            }
            if (self.pending_text_merge) |active| {
                if (std.mem.eql(u8, active.name, publication_name)) artifact_more = true;
            }
            const more = if (serving_physical_serviced) serving_index_more else database.runBackgroundTurnWithCancellation(cancellation, .index_storage) catch |err| blk: {
                if (maintenance_error == null) {
                    maintenance_error = err;
                    maintenance_error_phase = .artifacts;
                }
                break :blk true;
            };
            artifact_more = artifact_more or more;
            pending = pending or artifact_more;
            retain_artifact_runtime = more and maintenance_error == null;
            if (!retirement_more and !group_state.control_maintenance_pending) {
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
        // A retained immutable owner must not borrow a scheduling entry which
        // a later dropped-namespace audit may destroy before this DB closes.
        if (independent) database.borrowed_group_source_state = null;
        // No detached computation/publication can use the catalog now. Drop
        // its checkpoint pin before physical service, which otherwise retains
        // an obsolete reader fence throughout its own reuse/vacuum attempt.
        if (catalog) |*proof| proof.deinit();
        catalog = null;
        // Catching the captured target is not a lifetime boundary for parsed
        // immutable inputs. Later primary batches can reuse them after taking
        // a fresh primary epoch. Idle turns above evict any stale witnesses.
        const retain_source_runtime = source_runtime_cache != null and maintenance_error == null and !publication.index_inputs_changed;
        if (retain_source_runtime) {
            // This witness belongs to the runtime we just serviced. Physical
            // service may adopt a foreign index checkpoint without rebinding
            // that runtime; never certify its old inputs with the newer cut.
            source_runtime_cache.?.indexes = store.file.activeCheckpoint().logical.indexes;
        } else if (source_runtime_cache != null) {
            if (maintenance_error != null) self.publication_stats.text_runtime_cache.not_retained_failed +|= 1 else if (publication.index_inputs_changed) self.publication_stats.text_runtime_cache.not_retained_index_inputs +|= 1 else self.publication_stats.text_runtime_cache.not_retained_settled +|= 1;
        }
        const retained_logical: ?native.LogicalGenerations = if (retain_artifact_runtime or retain_source_runtime) store.file.activeCheckpoint().logical else null;
        const merge_owns_root = if (self.pending_text_merge) |active| active.root == root else false;
        if (independent and root.db_live and !retain_artifact_runtime and !retain_source_runtime and !merge_owns_root) {
            // Provider/query actors and their primary observations still end
            // here. Bounded immutable readers may bridge a later primary batch
            // even when this turn caught up. TTL and byte caps bound retention.
            const immutable_yield = if (maintenance_error) |err| switch (err) {
                error.EnrichmentWaitCanceled, error.EnrichmentSourceChanged, error.FileBusy, error.WouldBlock => true,
                else => false,
            } else false;
            if (maintenance_error == null or immutable_yield) {
                // A changed tip invalidates mutable publication authority, not
                // the bytes protected by existing immutable value leases. Keep
                // the bounded handoff across expected admission yields too;
                // each later open validates its exact inode/root identity.
                // Uncertain publication outcomes still clear this handoff.
                self.gate.mutex.lockUncancelable(io);
                self.preserveImmutableTextHandoff(root, database);
                if (immutable_yield and (self.immutable_sources.count != 0 or self.immutable_text_handoff != null))
                    self.publication_stats.text_runtime_cache.immutable_yield_handoffs +|= 1;
                self.gate.mutex.unlock(io);
            }
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
        if (!independent or !run_database) {
            // Native database work queues the independent storage lane below.
            // Running it here as well doubles allocator/orphan service per
            // replay window and delays the next useful source publication.
            // The separate lane releases all source pins and retains its
            // alternating scheduling guarantee during sustained ingestion.
            if (comptime h.builtin.is_test) if (self.test_before_storage_service) |hook| hook.notify(hook.ptr, root, retain_source_runtime);
            store.defer_background_vacuum = self.pending_text_merge != null;
            defer store.defer_background_vacuum = false;
            store.maintainBackgroundOnce(&self.verification_cancel) catch |err| {
                if (maintenance_error == null) {
                    maintenance_error = err;
                    maintenance_error_phase = .storage;
                }
                pending = true;
            };
        }

        self.gate.mutex.lockUncancelable(io);
        defer self.gate.mutex.unlock(io);
        self.publication_stats.replay_capacity_yields +|= @intFromBool(source_capacity_yield);
        self.publication_stats.maintenance_capture_ns +|= detached_started -| turn_started;
        self.publication_stats.maintenance_proof_ns +|= proof_finished -| detached_started;
        self.publication_stats.maintenance_replay_ns +|= replay_finished -| proof_finished;
        self.publication_stats.replay_publication_overhead_ns +|= replay_publication_overhead;
        self.publication_stats.maintenance_publication_overhead_ns +|= group_state.publication_overhead_ns.load(.monotonic) -| publication_overhead_before;
        self.publication_stats.producer_replay_budget = group_state.producer_replay_budget.producerSnapshot();
        self.publication_stats.maintenance_artifacts_ns +|= artifacts_finished -| replay_finished;
        self.publication_stats.maintenance_storage_ns +|= @import("antfly_platform").time.monotonicNs() -| artifacts_finished;
        self.maintenance_retained_logical = retained_logical;
        self.clearTextSourceRuntimeCache();
        const storage_yield = maintenance_error_phase == .storage and if (maintenance_error) |err| switch (err) {
            error.EnrichmentWaitCanceled, error.FileBusy, error.WouldBlock => true,
            else => false,
        } else false;
        // A physical-service yield does not invalidate successful source work.
        // Every later reuse still checks the pre-service index witness, inode,
        // catalog and controls against a fresh admitted checkpoint.
        if (retain_source_runtime and !needsOutcomeReconciliation(root) and (maintenance_error == null or storage_yield)) {
            self.text_source_runtime_cache = source_runtime_cache.?;
            source_runtime_cache = null;
            if (storage_yield) self.publication_stats.text_runtime_cache.retained_storage_yield +|= 1;
        } else if (retain_source_runtime) self.publication_stats.text_runtime_cache.not_retained_storage_error +|= 1;
        if (merge_owns_root and !retain_source_runtime and !retain_artifact_runtime) {
            std.debug.assert(self.maintenance_snapshot == root);
            self.maintenance_snapshot = null;
            self.maintenance_retained_logical = null;
            closeMaintenanceSnapshot(root);
        }
        if (maintenance_error) |err| {
            self.noteMaintenanceFailure(err, publication.identity.table_id, maintenance_error_phase);
        } else self.publication_stats.last_maintenance_error = null;
        if (source_more and (source_advanced or group_source_changed)) {
            self.runnable_sources.notify(io, .{ .table_id = publication.identity.table_id, .incarnation = publication.incarnation });
            self.publication_stats.maintenance_progress_requeues +|= 1;
        }
        if (run_database) {
            // Foreground table admission can grow the map while this turn is
            // detached. Reacquire its entry after taking the gate again.
            if (self.maintenance_schedules.getPtr(.{ .table_id = publication.identity.table_id, .incarnation = publication.incarnation })) |completed| {
                if (!publish_ready_merge) completed.database_pending = source_more;
                completed.artifact_pending = artifact_more or artifact_failed;
                completed.next_artifact_ns = std.Io.Clock.awake.now(io).nanoseconds + artifact_delay_ns;
                if (source_completed) {
                    completed.startup_replay_pending = false;
                    completed.serviced_sequence = entry_sequence;
                    completed.readiness = entry_readiness;
                    completed.covered_text_controls = if (!source_more) covered_controls else null;
                    completed.next_database_audit_ns = std.Io.Clock.awake.now(io).nanoseconds + database_audit_delay_ns;
                }
            }
        }
        store.writer_admission = null;
        root.maintenance_owner = null;
        root.maintenance_references -= 1;
        self.maintenance_activity.finish(io);
        active_runtime_borrow = false;
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
        return self.producer_jobs_active.load(.acquire) != 0 or self.runnable_sources.pending(h.handleLockIo()) or self.maintenance_storage_pending or self.maintenance_pending or self.maintenance_table_cursor != 0 or changed or ttl_pending;
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
        const foreground = self.current orelse return true;
        if (needsOutcomeReconciliation(foreground)) return error.OutcomeReconciliationRequired;
        if (self.maintenance_snapshot) |snapshot| if (needsOutcomeReconciliation(snapshot)) return error.OutcomeReconciliationRequired;
        const entry_header = foreground.owned_lite_backend.?.native_docstore.?.file.header;
        var tip = try native.NativeFile.openWithIo(alloc, io, self.path, .{ .read_only = true, .internal_reader = true });
        var tip_open = true;
        defer if (tip_open) tip.close();
        tip.allocator_cancel_token = &self.verification_cancel;
        self.reader_coordinator.observeCurrent((try tip.file.stat(tip.runtime())).inode, tip.headerFingerprint());
        try tip.inheritAllocatorVerification(&foreground.owned_lite_backend.?.native_docstore.?.file);
        if (self.maintenance_snapshot) |snapshot| {
            const previous = &snapshot.owned_lite_backend.?.native_docstore.?.file;
            try tip.inheritAllocatorVerification(previous);
            if (snapshot.maintenance_references == 0) {
                if (!try tip.takeVerifiedAllocator(previous)) _ = try tip.takeAllocatorRebaseBase(previous);
            }
        }
        try self.takeAvailableAllocator(&tip);
        // Certificate validation reuses the detached ledger/digest. It does not
        // construct DBs, open index leases, or run source/provider maintenance.
        // A genuinely unchecked tip still yields to the ordinary proof lane.
        if (!tip.free_pages_verified) _ = try tip.inheritAllocatorCertificate();
        if (!tip.free_pages_verified) return true;
        const root = try self.refreshMaintenanceSnapshotAt(foreground, .storage, &tip);
        tip.close();
        tip_open = false;
        const store = root.owned_lite_backend.?.native_docstore.?;
        const maintenance_entry_header = store.file.header;
        retainMaintenanceSnapshot(root);
        defer closeMaintenanceSnapshot(root);
        root.maintenance_references += 1;
        // Foreground refresh may retire its logical wrapper while physical
        // service is detached. Retain it until adoption has checked identity.
        foreground.query_borrows += 1;
        defer {
            std.debug.assert(gate_locked);
            foreground.query_borrows -= 1;
            self.collectRetired();
        }
        self.maintenance_activity.begin();
        root.maintenance_owner = self;
        root.maintenance_publication = null;
        store.writer_admission = .{ .ptr = root, .enter = enterMaintenanceWriter, .leave = leaveMaintenanceWriter };
        lease.close();
        MaintenanceHoldSites.record(&self.publication_stats.maintenance_hold_sites.storage_turn, turn_held_from);
        lease_held = false;
        self.gate.mutex.unlock(io);
        gate_locked = false;
        const service_started = @import("antfly_platform").time.monotonicNs();
        if (comptime h.builtin.is_test) if (self.test_before_storage_service) |hook| hook.notify(hook.ptr, root, self.text_source_runtime_cache != null);
        store.defer_background_vacuum = self.pending_text_merge != null;
        defer store.defer_background_vacuum = false;
        const result = store.maintainBackgroundOnce(&self.verification_cancel);
        self.gate.mutex.lockUncancelable(io);
        gate_locked = true;
        self.publication_stats.maintenance_storage_ns +|= @import("antfly_platform").time.monotonicNs() -| service_started;
        store.writer_admission = null;
        root.maintenance_owner = null;
        root.maintenance_references -= 1;
        self.maintenance_activity.finish(io);
        inheritReclamationTelemetry(self.current.?.owned_lite_backend.?.native_docstore.?, store);
        try self.adoptIdleMaintenanceCheckpoint(foreground, entry_header, maintenance_entry_header, store);
        result catch |err| {
            // Physical service does not certify or discard a retained source
            // witness. Reuse still validates its original logical identities,
            // including when admission adopted a foreign checkpoint then yielded.
            if (self.text_source_runtime_cache != null and switch (err) {
                error.EnrichmentWaitCanceled, error.FileBusy, error.WouldBlock => true,
                else => false,
            }) self.publication_stats.text_runtime_cache.retained_storage_yield +|= 1;
            return err;
        };
        return true;
    }

    /// Model a cold owner including its path-owned allocator authority.
    /// Test callers must own the gate and drain background operations first.
    pub fn resetAllocatorAuthorityForTest(self: *Owner) void {
        std.debug.assert(h.builtin.is_test);
        forgetAllocatorAuthorityForTest(self);
    }

    pub fn maintenanceTurnForTest(self: *Owner) !bool {
        std.debug.assert(h.builtin.is_test);
        return self.maintenanceTurn();
    }

    pub fn maintenanceContinuationPendingForTest(self: *Owner) bool {
        std.debug.assert(h.builtin.is_test);
        const io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(io);
        defer self.gate.mutex.unlock(io);
        if (self.producer_jobs_active.load(.acquire) != 0 or self.pending_text_merge != null or self.maintenance_storage_pending or self.maintenance_pending or
            self.maintenance_table_cursor != 0 or self.runnable_sources.pending(io)) return true;
        var schedules = self.maintenance_schedules.valueIterator();
        while (schedules.next()) |schedule| if (schedule.database_pending or schedule.artifact_pending) return true;
        return false;
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
        return if (self.runnable_sources.pending(h.handleLockIo()) or self.maintenance_storage_pending or self.maintenance_table_cursor != 0) 0 else if (pending or self.producer_jobs_active.load(.acquire) != 0 or self.pending_text_merge != null) 100 else 1000;
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
        var gate_refusal: ?anyerror = null;
        try self.verifyAllocatorWithCleanupAssumeLocked(lease, connection, &gate_refusal);
        // Reader teardown must return with the gate held, even after expiry.
        // Its mandatory drain cannot authorize fresh work after a timed-out
        // handoff. The completed proof remains cached for a later request.
        if (gate_refusal) |err| return err;
    }

    fn reacquireAllocatorProofGate(self: *Owner, connection: ?*Connection, refusal: *?anyerror) void {
        const io = h.handleLockIo();
        if (connection) |caller| if (caller.admission_deadline_ns) |deadline| {
            self.gate.mutex.lockUntil(io, deadline) catch |err| {
                // Runtime/proof references must be released under this gate.
                // Join that teardown, but retain the admission refusal.
                self.gate.mutex.lockUncancelable(io);
                if (refusal.* == null) refusal.* = err;
            };
            return;
        };
        self.gate.mutex.lockUncancelable(io);
    }

    fn verifyAllocatorWithCleanupAssumeLocked(self: *Owner, lease: *?native.PathWriterLock, connection: ?*Connection, gate_refusal: *?anyerror) !void {
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
            // the on-disk header and inode must still be the live ones.
            var probe = try native.NativeFile.openWithIo(alloc, io, self.path, .{ .read_only = true, .internal_reader = true });
            defer probe.close();
            if (std.meta.eql(file.header, probe.header) and
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
        try self.takeAvailableAllocator(file);
        if (file.free_pages_verified) return;
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
            self.reacquireAllocatorProofGate(connection, gate_refusal);
        }
        lease.*.?.close();
        lease.* = null;
        self.gate.mutex.unlock(io);
        if (comptime h.builtin.is_test) if (self.test_after_allocator_capture) |hook| hook.notify(hook.ptr, reader);
        const proof_started = std.Io.Clock.awake.now(io).nanoseconds;
        const verified = reader.completeAllocatorVerification();
        const proof_ns: u64 = @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - proof_started));
        // The proof is this call's own work, not waiting: it does not use up
        // the busy budget carried to later re-entries of the same call.
        if (connection) |caller| {
            if (caller.admission_active_sink) |sink| sink.* +|= proof_ns;
            // Credit it now, before reacquiring the lease below, so the walk
            // does not shorten this attempt's own wait either.
            if (caller.admission_deadline_ns) |*deadline| deadline.* +|= proof_ns;
        }
        // Credit execution before the timed gate wait. Crediting it after a
        // refusal could leave artificial budget for another admission attempt.
        self.reacquireAllocatorProofGate(connection, gate_refusal);
        self.publication_stats.allocator_proof_walks +|= 1;
        self.publication_stats.allocator_proof_ns +|= proof_ns;
        self.publication_stats.allocator_proof_max_ns = @max(self.publication_stats.allocator_proof_max_ns, proof_ns);
        if (verified) |_| {} else |_| {
            self.publication_stats.allocator_proof_failures +|= 1;
        }
        try verified;
        // If reacquiring the lease below yields busy, the next attempt reuses
        // this proof instead of repeating the walk.
        self.allocator_proof_receipt = try reader.allocatorProofReceipt();
        if (gate_refusal.*) |err| return err;
        // A foreground operation has not started its mutation yet. Preserve
        // its connection-local busy policy when another process wins the
        // writer lease during the detached proof. Background turns stay
        // nonblocking and yield to that writer instead of consuming its budget.
        lease.* = if (connection) |caller| try caller.acquirePathLeaseAssumeLocked() else self.withReadRegistrationBatch(try self.tryWriterLeaseAssumeLocked());
        var probe = try native.NativeFile.openWithIo(alloc, io, self.path, .{ .read_only = true, .internal_reader = true });
        defer probe.close();
        if (self.current) |current| {
            // A foreground publication may have failed while the walk ran.
            // Retrying the same immutable proof cannot reconcile that outcome.
            if (needsOutcomeReconciliation(current)) return error.OutcomeReconciliationRequired;
            const live = &current.owned_lite_backend.?.native_docstore.?.file;
            if (live.transaction_header != null or live.durable_header != null) return error.FileBusy;
            if (std.meta.eql(live.header, probe.header) and
                (try live.file.stat(live.runtime())).inode == (try probe.file.stat(probe.runtime())).inode)
                try live.inheritAllocatorVerification(reader);
        }
        self.allocator_proof_receipt = null;
    }

    fn deinitMaintenanceSchedules(self: *Owner) void {
        var schedules = self.maintenance_schedules.valueIterator();
        while (schedules.next()) |schedule| {
            if (schedule.group_sources) |state| {
                state.deinit();
                alloc.destroy(state);
            }
            if (schedule.name) |name| alloc.free(name);
            schedule.text_merge_caps.deinit(alloc);
        }
        self.maintenance_schedules.deinit(alloc);
        self.runnable_sources.deinit(alloc);
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
            if (lease == null) lease = try self.tryWriterLeaseAssumeLocked();
        }
    }

    fn qualificationMetricsEnabled() bool {
        if (comptime h.builtin.os.tag == .freestanding) return false;
        const raw = std.c.getenv("ANTFLY_BENCH_METRICS") orelse return false;
        const enabled = std.mem.span(raw);
        return enabled.len != 0 and !std.mem.eql(u8, enabled, "0") and !std.ascii.eqlIgnoreCase(enabled, "false");
    }

    fn logQualificationSummary(self: *Owner) void {
        if (!qualificationMetricsEnabled()) return;
        self.updatePathLeaseStatistics();
        const p = &self.publication_stats;
        const namespaces = self.reader_coordinator.epochNamespaceStats();
        std.log.info("antfly_bench_native_epoch_namespaces opens={d} validations={d}", .{ namespaces.opens, namespaces.validations });
        // Numeric/enum diagnostics only: no path, SQL, model or document data.
        // Preparation overlaps actor service; durations are not additive.
        std.log.info("antfly_bench_native_owner source_turns={d} capture_ns={d} replay_ns={d} artifacts_ns={d} storage_ns={d} runtime_refreshes={d} maintenance_errors={d} maintenance_timeouts={d} merge_started={d} merge_applied={d} merge_discarded={d} merge_prepare_ns={d} merge_staging_ns={d} merge_staged_bytes={d} merge_publication_ns={d} sql_overlay_hits={d} sql_overlay_scope_extensions={d} sql_overlay_rebuilds={d} sql_overlay_rebuild_entries={d} sql_overlay_evictions={d} sql_staged_statements={d} replay_waits={d} replay_refused_prepare_ns={d} last_failure={s}", .{ p.maintenance_source_turns, p.maintenance_capture_ns, p.maintenance_replay_ns, p.maintenance_artifacts_ns, p.maintenance_storage_ns, p.serving_runtime_refreshes, p.maintenance_errors, p.maintenance_queue_timeouts, p.text_merge_jobs_started, p.text_merge_jobs_applied, p.text_merge_jobs_discarded, p.text_merge_prepare_ns, p.text_merge_staging_ns, p.text_merge_staged_bytes, p.text_merge_publication_ns, p.sql_constraint_overlay_hits, p.sql_constraint_overlay_scope_extensions, p.sql_constraint_overlay_rebuilds, p.sql_constraint_overlay_rebuild_entries, p.sql_constraint_overlay_evictions, p.sql_constraint_staged_statements, p.replay_backpressure_waits, p.replay_refused_preparation_ns, p.last_maintenance_failure orelse "none" });
        std.log.info("antfly_bench_native_producers started={d} completed={d} service_ns={d}", .{ p.producer_jobs_started, p.producer_jobs_completed, p.producer_service_ns });
        std.log.info("antfly_bench_native_group_controls batches={d} commands={d} native_commits={d}", .{ p.group_source_control_batches, p.group_source_control_commands, p.group_source_control_commits });
        std.log.info("antfly_bench_native_service covered_observations={d} serving_physical_turns={d} admitted_merge_captures={d} ready_merge_publication_turns={d}", .{ p.covered_source_observations, p.serving_physical_turns, p.admitted_merge_captures, p.ready_merge_publication_turns });
        inline for (@typeInfo(MaintenanceHoldSites).@"struct".field_names) |field_name| {
            if (comptime !std.mem.eql(u8, field_name, "capture_steps") and !std.mem.eql(u8, field_name, "capture_max_steps")) {
                const site = @field(p.maintenance_hold_sites, field_name);
                std.log.info("antfly_bench_native_hold_" ++ field_name ++ " count={d} ns={d} max_ns={d}", .{ site.count, site.ns, site.max_ns });
            }
        }
        const steps = p.maintenance_hold_sites.capture_steps;
        std.log.info("antfly_bench_native_capture_steps refresh_ns={d} marker_sweep_ns={d} recover_ns={d} catalog_ns={d} debt_checks_ns={d} schedule_ns={d} snapshot_ns={d} publication_catalog_ns={d}", .{ steps.refresh_ns, steps.marker_sweep_ns, steps.recover_ns, steps.catalog_ns, steps.debt_checks_ns, steps.schedule_ns, steps.snapshot_ns, steps.publication_catalog_ns });
        inline for (@typeInfo(CaptureSteps).@"struct".field_names) |field_name| {
            std.log.info("antfly_bench_native_capture_step_{s} ns={d} max_ns={d}", .{ field_name, @field(steps, field_name), @field(p.maintenance_hold_sites.capture_max_steps, field_name) });
        }
        inline for (@typeInfo(TextRuntimeCacheDiagnostics).@"struct".field_names) |field_name| {
            const count = @field(p.text_runtime_cache, field_name);
            if (count != 0) std.log.info("antfly_bench_native_runtime_cache_{s} count={d}", .{ field_name, count });
        }
        if (self.current) |current| {
            const work = current.owned_lite_backend.?.native_docstore.?.file.allocatorWorkSnapshot();
            std.log.info("antfly_bench_native_allocator full_loads={d} full_load_ns={d} digest_rebuilds={d} digest_rebuild_ns={d} journal_rebases={d} journal_rebase_ns={d} cache_transfers={d} cache_copies={d}", .{ work.full_loads, work.full_load_ns, work.digest_rebuilds, work.digest_rebuild_ns, work.journal_rebases, work.journal_rebase_ns, work.cache_transfers, work.cache_copies });
            std.log.info("antfly_bench_native_artifact_sources opens={d} reuses={d}", .{ work.artifact_source_opens, work.artifact_source_reuses });
        }
        const f = p.maintenance_failures;
        std.log.info("antfly_bench_native_runtime maintenance_opens={d} namespace_opens={d} source_reuses={d}", .{ p.maintenance_runtime_opens, p.maintenance_namespace_opens, p.maintenance_source_runtime_reuses });
        std.log.info("antfly_bench_native_service refresh_ns={d} replay_publication_overhead_ns={d} index_admission_ns={d} metadata_admission_ns={d} publication_hold_ns={d}", .{ p.serving_runtime_refresh_ns, p.replay_publication_overhead_ns, p.replay_index_admission_ns, p.replay_metadata_admission_ns, p.publication_hold_ns });
        std.log.info("antfly_bench_native_owner_failures capture={d} source={d} retirement={d} artifacts={d} storage={d}", .{ f.capture, f.source, f.retirement, f.artifacts, f.storage });
        const leases = p.path_leases;
        std.log.info("antfly_bench_native_path_leases leases={d} admission_ns={d} admission_max_ns={d} hold_ns={d} hold_max_ns={d}", .{ leases.leases, leases.admission_ns, leases.admission_max_ns, leases.hold_ns, leases.hold_max_ns });
    }

    pub fn updatePathLeaseStatistics(self: *Owner) void {
        self.publication_stats.path_leases = self.gate.lease_stats.snapshot();
    }

    fn deinit(self: *Owner) void {
        if (self.worker) |worker| worker.join();
        self.producer_wake.set(h.handleLockIo());
        if (self.producer_worker) |worker| worker.join();
        const uncertain_producer = if (self.pending_producer) |pending| needsOutcomeReconciliation(pending.root) else false;
        self.collectCompletedProducer();
        self.immutable_sources.clear();
        self.clearImmutableTextHandoff();
        self.clearTextSourceRuntimeCache();
        const uncertain_merge = if (self.pending_text_merge) |pending| needsOutcomeReconciliation(pending.root) else false;
        self.discardPendingTextMerge();
        self.logQualificationSummary();
        if (!uncertain_merge and !uncertain_producer) self.certifyAllocatorOnClose() catch |err| {
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
    left.writer_allocator_cache = null;
    right.writer_allocator_cache = null;
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

    fn traceAdmissionFailure(_: *Connection, phase: enum { local_gate, path_lease, runtime_refresh, proof_turn, allocator_proof, proof_tip_changed }, err: anyerror, started_ns: u64) void {
        if ((err == error.FileBusy or err == error.WouldBlock) and Owner.qualificationMetricsEnabled())
            std.log.info("antfly_bench_admission_refusal phase={s} error={s} elapsed_ns={d}", .{ @tagName(phase), @errorName(err), @import("antfly_platform").time.monotonicNs() -| started_ns });
    }

    fn refreshOwnerForAdmission(self: *Connection) !*h.Handle {
        const started = @import("antfly_platform").time.monotonicNs();
        return self.owner.refresh() catch |err| {
            self.traceAdmissionFailure(.runtime_refresh, err, started);
            return err;
        };
    }

    pub fn acquire(self: *Connection) !native.PathWriterLock {
        return self.acquireWithMaintenance(false);
    }

    pub fn acquireForMaintenance(self: *Connection) !native.PathWriterLock {
        return self.acquireWithMaintenance(true);
    }

    fn acquireWithMaintenance(self: *Connection, manual_maintenance: bool) !native.PathWriterLock {
        const io = h.handleLockIo();
        const started = @import("antfly_platform").time.monotonicNs();
        if (self.admission_deadline_ns) |deadline|
            self.gate.mutex.lockForLeaseUntil(io, deadline) catch |err| {
                self.traceAdmissionFailure(.local_gate, err, started);
                return err;
            }
        else
            self.gate.mutex.lockForLease(io);
        errdefer self.gate.mutex.unlock(io);
        if (manual_maintenance) self.owner.maintenance_activity.wait(io, &self.gate.mutex);
        return self.acquirePathLeaseAssumeLocked();
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

    fn acquirePathLeaseAssumeLocked(self: *Connection) !native.PathWriterLock {
        const started = @import("antfly_platform").time.monotonicNs();
        return self.acquirePathLeaseWithinBudget() catch |err| {
            self.traceAdmissionFailure(.path_lease, err, started);
            return err;
        };
    }

    fn acquirePathLeaseWithinBudget(self: *Connection) !native.PathWriterLock {
        const io = h.handleLockIo();
        const deadline = self.admissionDeadline();
        // Writers take FIFO turns across processes. An expired budget still
        // permits an immediate acquisition when nobody is queued.
        if (h.liteOpenModeCanWrite(self.options.open_mode)) {
            const stats = &self.owner.publication_stats.foreground_lease;
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
            stats.recordAcquired(if (lease.queue_turn != null) .queued else .uncontended, @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started)));
            lease.admission_ns = @intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started));
            lease.startHold(stats);
            return self.owner.withReadRegistrationBatch(lease);
        }
        // Our own retained exclusive lease would block this shared lock.
        self.gate.mutex.dropRetained();
        var backoff: i96 = std.time.ns_per_ms;
        while (true) {
            // A slow detached proof may outlast the retry budget. Always try
            // the lease once: a stable verified tip must still be admitted if
            // no contention wait is needed. An expired budget forbids waiting,
            // not an uncontended acquisition.
            const lease = native.lockReaderPathWithIo(alloc, io, self.path);
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
        var root = try self.refreshOwnerForAdmission();
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
                lease.* = try self.acquirePathLeaseAssumeLocked();
                root = try self.refreshOwnerForAdmission();
            }
            if (try needsCapacityProof(root) or (intent == .mutation and try self.needsReuseAdmission(root))) {
                // Keep foreign unverified opens from publishing index leases
                // between the detached proof and reacquiring the writer lease.
                const proof_started = @import("antfly_platform").time.monotonicNs();
                if (!try self.owner.retainMaintenanceTurnAssumeLocked()) {
                    self.traceAdmissionFailure(.proof_turn, error.FileBusy, proof_started);
                    return error.FileBusy;
                }
                defer self.owner.releaseMaintenanceTurnAssumeLocked();
                self.owner.verifyAllocatorAssumeLocked(lease, self) catch |err| {
                    self.traceAdmissionFailure(.allocator_proof, err, proof_started);
                    return err;
                };
                if (lease.* == null) lease.* = try self.acquirePathLeaseAssumeLocked();
                root = try self.refreshOwnerForAdmission();
                // Another process may move the tip during the detached walk.
                // Do not repeat whole-file proofs while holding this
                // connection's admission queue. No transaction has started:
                // refuse safely and let a later request prove the new tip.
                if (try needsCapacityProof(root) or (intent == .mutation and try self.needsReuseAdmission(root))) {
                    self.traceAdmissionFailure(.proof_tip_changed, error.FileBusy, proof_started);
                    return error.FileBusy;
                }
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
        if (compatible(owner.options, options) and !owner.hasUncertainOutcome()) break owner;
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
        owner.options.writer_allocator_cache = &gate.writer_allocator_cache;
        if (initial) |root| {
            root.owned_lite_backend.?.native_docstore.?.file.writer_allocator_cache = &gate.writer_allocator_cache;
            root.owned_lite_backend.?.native_docstore.?.setArtifactPublicationGate(OwnedArtifactGate.view(gate));
        }
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
                owner.attachPathLeaseStats(&lease);
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

test "capi native readonly lease timing accounts only for the initial open" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    @import("io_lifetime.zig").acquire();
    defer @import("io_lifetime.zig").release();
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    var tmp = try api.TestDirectory.init("readonly-initial-lease-timing");
    defer tmp.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, tmp.path());
    const path = try std.fmt.allocPrint(a, "{s}/database.aflite", .{tmp.path()});
    defer a.free(path);
    {
        const seed = try open(path, .{ .profile = .native, .no_sync = true }, true);
        h.closeHandle(seed);
    }
    inline for (.{ h.db_mod.OpenMode.query_readonly, h.db_mod.OpenMode.status_only }) |mode| {
        const started = std.Io.Clock.awake.now(io).nanoseconds;
        const reader = try open(path, .{ .profile = .native, .open_mode = mode, .no_sync = true }, false);
        defer h.closeHandle(reader);
        const snapshot = reader.lite_connection.?.gate.lease_stats.snapshot();
        const elapsed = std.Io.Clock.awake.now(io).nanoseconds - started;
        try std.testing.expect(snapshot.leases > 0);
        // No machine-speed limit: every measured lease was inside this open,
        // even if the test process was descheduled or the filesystem was slow.
        try std.testing.expect(snapshot.hold_ns <= elapsed);
        try std.testing.expect(snapshot.hold_max_ns <= elapsed);
    }
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
            forgetAllocatorAuthorityForTest(owner);
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
            if (scenario == .uncertain) {
                // This existing owner stays fenced. A separate writable open
                // reconstructs durable truth without reusing uncertain state.
                const poisoned = owner.current.?;
                poisoned.owned_lite_backend.?.native_docstore.?.file.checkpoint_publication_uncertain = true;
                try std.testing.expectError(error.OutcomeReconciliationRequired, connection.refresh(&lease, .mutation));
                try std.testing.expect(owner.current == poisoned);
                try std.testing.expect(!owner.allocator_verification_active);
                continue;
            }
            if (scenario == .uncertain_during) {
                owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.capture };
                try std.testing.expectError(error.OutcomeReconciliationRequired, connection.refresh(&lease, .mutation));
                try std.testing.expectEqual(@as(usize, 1), probe.captures);
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
    forgetAllocatorAuthorityForTest(owner);
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
        forgetAllocatorAuthorityForTest(owner);
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
        if (scenario != .foreground) {
            // The proof completed before the busy yield. The next attempt
            // transfers it under the lease instead of walking again.
            probe.release();
            owner.test_after_allocator_capture = null;
            if (lease == null) lease = try owner.tryWriterLeaseAssumeLocked();
            const walks = owner.publication_stats.allocator_proof_walks;
            try owner.verifyAllocatorAssumeLocked(&lease, if (scenario == .background) null else connection);
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
            const certify_on_commit = store.file.allocator_certificate_on_commit;
            store.file.allocator_certificate_on_commit = false;
            defer store.file.allocator_certificate_on_commit = certify_on_commit;
            try store.file.putCatalogRecord("uncertified-proof-fixture", "primitive publication without a seal");
            forgetAllocatorAuthorityForTest(owner);
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
            forgetAllocatorAuthorityForTest(owner);
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
    forgetAllocatorAuthorityForTest(owner);
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
            forgetAllocatorAuthorityForTest(owner);
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
        forgetAllocatorAuthorityForTest(owner);
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
    forgetAllocatorAuthorityForTest(owner);
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

// Model a cold process, rather than clearing one stale wrapper hint while
// another quiescent runtime or the path cache still owns checked authority.
fn forgetAllocatorAuthorityForTest(owner: *Owner) void {
    std.debug.assert(h.builtin.is_test and !owner.allocator_verification_active);
    owner.gate.writer_allocator_cache.deinit();
    const roots = [_]?*h.Handle{ owner.current, owner.maintenance_snapshot, if (owner.pending_text_merge) |pending| pending.root else null };
    for (roots) |maybe_root| if (maybe_root) |root| {
        const file = &root.owned_lite_backend.?.native_docstore.?.file;
        file.free_pages_verified = false;
        file.allocator_rebase_header = null;
        file.allocator_ledger_rebased = false;
        file.allocator_verification_base = null;
    };
}

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
        forgetAllocatorAuthorityForTest(owner);
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

    /// Replay lifecycle probes need an outstanding journal entry even when
    /// the public batch path can publish prepared text with its primary write.
    fn writeReplaySource(self: *@This(), request: h.db_mod.types.BatchRequest) !void {
        var lease = try self.connection.acquire();
        defer self.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try self.connection.owner.refresh();
        try root.db.batch(request);
        Owner.notifySourceMutation(self.connection.owner, root.db.core.identity_namespace.table_id, root.db.root_incarnation);
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

const ConstraintOverlayProbe = struct {
    fn run(id: ?*anyopaque, session: ?u64, statement: []const u8, expected_error: ?[]const u8) !?i64 {
        const a = std.testing.allocator;
        const encoded = try std.json.Stringify.valueAlloc(a, .{ .statement = statement, .session_id = session }, .{});
        defer a.free(encoded);
        var out: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&out);
        const code = api.antfly_db_sql_json(id, .fromSlice(encoded), &out);
        if (expected_error == null) {
            if (code != .ok) std.debug.print("constraint overlay SQL fixture failed statement={s} response={s}\n", .{ statement, if (out.ptr) |ptr| ptr[0..out.len] else "" });
            try std.testing.expectEqual(h.capi.ErrorCode.ok, code);
        } else try std.testing.expect(code != .ok);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
        defer parsed.deinit();
        if (expected_error) |expected| {
            try std.testing.expectEqualStrings(expected, parsed.value.object.get("error").?.object.get("code").?.string);
            return null;
        }
        if (parsed.value.object.get("rows")) |rows| if (rows.array.items.len != 0) {
            const cell = rows.array.items[0].array.items[0];
            if (cell == .integer) return cell.integer;
            if (cell == .string or cell == .number_string) return try std.fmt.parseInt(i64, if (cell == .string) cell.string else cell.number_string, 10);
        };
        return null;
    }
};

test "capi native allocator proof uses checked path authority despite stale wrapper hints" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("warm-path-allocator-authority", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    const walks = owner.publication_stats.allocator_proof_walks;
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const file = &owner.current.?.owned_lite_backend.?.native_docstore.?.file;
        try std.testing.expect(owner.gate.writer_allocator_cache.holder.?.free_pages_verified);
        try std.testing.expect(owner.gate.writer_allocator_cache.holder.?.ledger != null);
        // This deliberately leaves the exact path-owned proof intact.
        file.free_pages_verified = false;
    }
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    try std.testing.expectEqual(walks, owner.publication_stats.allocator_proof_walks);
    try std.testing.expect(owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
    try fixture.expectWritten(.batch, true);
}

test "capi native SQL constraint overlay fences dependencies while ignoring unrelated primary writes" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("sql-constraint-overlay", 5000, .batch);
    defer fixture.deinit();
    const Probe = ConstraintOverlayProbe;
    _ = try Probe.run(fixture.id, null, "CREATE TABLE items (id BIGINT PRIMARY KEY, v BIGINT)", null);
    _ = try Probe.run(fixture.id, null, "SELECT COUNT(*) FROM items", null);
    var session_id: u64 = 0;
    const sessions = @import("sql_session.zig");
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(fixture.id, &session_id));
    defer _ = sessions.antfly_db_sql_session_close(fixture.id, session_id);
    _ = try Probe.run(fixture.id, session_id, "BEGIN", null);
    _ = try Probe.run(fixture.id, session_id, "INSERT INTO items (id,v) VALUES (1,10) ON CONFLICT (id) DO UPDATE SET v=EXCLUDED.v", null);
    const owner = fixture.connection.owner;
    const session = fixture.connection.sql_sessions.get(session_id).?;
    try std.testing.expectEqual(@as(usize, 1), session.test_overlay_rebuilds);
    _ = try Probe.run(fixture.id, session_id, "INSERT INTO items (id,v) VALUES (2,20) ON CONFLICT (id) DO UPDATE SET v=EXCLUDED.v", null);
    try std.testing.expectEqual(@as(usize, 1), session.test_overlay_rebuilds);
    try std.testing.expectEqual(@as(usize, 2), session.test_overlay_extensions);
    _ = try Probe.run(fixture.id, session_id, "SAVEPOINT kept", null);
    _ = try Probe.run(fixture.id, session_id, "UPDATE items SET v=11 WHERE id=1", null);
    _ = try Probe.run(fixture.id, session_id, "ROLLBACK TO SAVEPOINT kept", null);
    try std.testing.expect(session.constraint_overlay == null);
    _ = try Probe.run(fixture.id, session_id, "INSERT INTO items (id,v) VALUES (3,30)", null);
    try std.testing.expectEqual(@as(usize, 2), session.test_overlay_rebuilds);
    try std.testing.expectEqual(@as(?i64, 60), try Probe.run(fixture.id, session_id, "SELECT SUM(v) FROM items", null));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    _ = try Probe.run(fixture.id, session_id, "INSERT INTO items (id,v) VALUES (4,40)", null);
    try std.testing.expectEqual(@as(usize, 2), session.test_overlay_rebuilds);
    _ = try Probe.run(fixture.id, null, "INSERT INTO items (id,v) VALUES (99,0)", null);
    _ = try Probe.run(fixture.id, session_id, "UPDATE items SET v=20 WHERE id=2", null);
    try std.testing.expectEqual(@as(usize, 3), session.test_overlay_rebuilds);
    _ = try Probe.run(fixture.id, null, "CREATE TABLE unrelated (n BIGINT)", null);
    _ = try Probe.run(fixture.id, session_id, "INSERT INTO items (id,v) VALUES (5,50)", null);
    try std.testing.expectEqual(@as(usize, 4), session.test_overlay_rebuilds);
    _ = try Probe.run(fixture.id, session_id, "INSERT INTO items (id,v) VALUES (1,44) ON CONFLICT (id) DO UPDATE SET v=EXCLUDED.v", null);
    try std.testing.expectEqual(@as(?i64, 184), try Probe.run(fixture.id, session_id, "SELECT SUM(v) FROM items", null));
    _ = try Probe.run(fixture.id, session_id, "COMMIT", null);
    try std.testing.expect(session.constraint_overlay == null);
    _ = try Probe.run(fixture.id, session_id, "BEGIN", null);
    _ = try Probe.run(fixture.id, session_id, "UPDATE items SET v=77 WHERE id=1", null);
    _ = try Probe.run(fixture.id, null, "UPDATE items SET v=88 WHERE id=1", null);
    // A cached statement is never commit authority: a later conflicting write
    // must remain fenced even when COMMIT follows without another statement.
    _ = try Probe.run(fixture.id, session_id, "COMMIT", "40001");
    _ = try Probe.run(fixture.id, session_id, "ROLLBACK", null);
    try std.testing.expectEqual(@as(?i64, 88), try Probe.run(fixture.id, null, "SELECT v FROM items WHERE id=1", null));
    try std.testing.expect((try owner.current.?.owned_lite_backend.?.native_docstore.?.checkWithCancel(null)).valid);
}

test "capi native SQL constraint overlay extends independent scopes after validating prior effects" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const Probe = ConstraintOverlayProbe;
    const Change = enum { unrelated, relevant, ddl, conflict };
    for ([_]Change{ .unrelated, .relevant, .ddl, .conflict }) |change| {
        var fixture = try AdmissionFollowupFixture.init("sql-overlay-scope-extension", 5000, .batch);
        defer fixture.deinit();
        for ([_][]const u8{ "items", "other", "unrelated" }) |name| {
            const create = try std.fmt.allocPrint(std.testing.allocator, "CREATE TABLE {s} (id BIGINT PRIMARY KEY, v BIGINT)", .{name});
            defer std.testing.allocator.free(create);
            _ = try Probe.run(fixture.id, null, create, null);
            const seed = try std.fmt.allocPrint(std.testing.allocator, "INSERT INTO {s} (id,v) VALUES (0,0)", .{name});
            defer std.testing.allocator.free(seed);
            _ = try Probe.run(fixture.id, null, seed, null);
        }
        var id: u64 = 0;
        const sessions = @import("sql_session.zig");
        try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(fixture.id, &id));
        defer _ = sessions.antfly_db_sql_session_close(fixture.id, id);
        _ = try Probe.run(fixture.id, id, "BEGIN", null);
        const session = fixture.connection.sql_sessions.get(id).?;
        for (1..17) |n| {
            var buffer: [128]u8 = undefined;
            _ = try Probe.run(fixture.id, id, try std.fmt.bufPrint(&buffer, "INSERT INTO items (id,v) VALUES ({d},{d})", .{ n, n }), null);
        }
        try std.testing.expectEqual(@as(usize, 1), session.test_overlay_rebuilds);
        const stats = &fixture.connection.owner.publication_stats;
        const entries_before = stats.sql_constraint_overlay_rebuild_entries;
        const extensions_before = stats.sql_constraint_overlay_scope_extensions;
        _ = try Probe.run(fixture.id, null, switch (change) {
            .unrelated => "INSERT INTO unrelated (id,v) VALUES (99,0)",
            .relevant => "INSERT INTO items (id,v) VALUES (99,0)",
            .ddl => "CREATE TABLE new_dependency (id BIGINT PRIMARY KEY)",
            .conflict => "INSERT INTO items (id,v) VALUES (1,99)",
        }, null);
        if (change == .conflict) {
            // Speculative effects do not grant commit authority. COMMIT must
            // reject the duplicate logical primary key, preserve the peer's
            // row, and publish none of the staged writes.
            _ = try Probe.run(fixture.id, id, "INSERT INTO other (id,v) VALUES (1,1)", null);
            try std.testing.expectEqual(@as(usize, 2), session.test_overlay_rebuilds);
            try std.testing.expectEqual(entries_before + 16, stats.sql_constraint_overlay_rebuild_entries);
            _ = try Probe.run(fixture.id, id, "COMMIT", "23505");
            _ = try Probe.run(fixture.id, id, "ROLLBACK", null);
            try std.testing.expectEqual(@as(?i64, 99), try Probe.run(fixture.id, null, "SELECT v FROM items WHERE id=1", null));
            try std.testing.expectEqual(@as(?i64, 1), try Probe.run(fixture.id, null, "SELECT COUNT(*) FROM other", null));
        } else {
            _ = try Probe.run(fixture.id, id, "INSERT INTO other (id,v) VALUES (1,1)", null);
            const can_extend = change == .unrelated;
            try std.testing.expectEqual(@as(usize, if (can_extend) 1 else 2), session.test_overlay_rebuilds);
            try std.testing.expectEqual(entries_before + @as(u64, if (can_extend) 0 else 16), stats.sql_constraint_overlay_rebuild_entries);
            try std.testing.expectEqual(extensions_before + @as(u64, if (can_extend) 1 else 0), stats.sql_constraint_overlay_scope_extensions);
            _ = try Probe.run(fixture.id, id, "INSERT INTO items (id,v) VALUES (17,17)", null);
            try std.testing.expectEqual(@as(usize, if (can_extend) 1 else 2), session.test_overlay_rebuilds);
            _ = try Probe.run(fixture.id, id, "COMMIT", null);
            try std.testing.expectEqual(@as(?i64, 2), try Probe.run(fixture.id, null, "SELECT COUNT(*) FROM other", null));
        }
        try std.testing.expectEqual(@as(usize, 0), session.budget.live);
        try std.testing.expect((try fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.checkWithCancel(null)).valid);
    }
}

test "capi native SQL constraint overlay fences inbound and outbound foreign key dependencies" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("sql-overlay-dependencies", 5000, .batch);
    defer fixture.deinit();
    const Probe = ConstraintOverlayProbe;
    _ = try Probe.run(fixture.id, null, "CREATE TABLE parents (id BIGINT PRIMARY KEY)", null);
    _ = try Probe.run(fixture.id, null, "CREATE TABLE children (id BIGINT PRIMARY KEY, parent_id BIGINT REFERENCES parents(id))", null);
    _ = try Probe.run(fixture.id, null, "INSERT INTO parents (id) VALUES (1)", null);
    var id: u64 = 0;
    const sessions = @import("sql_session.zig");
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(fixture.id, &id));
    defer _ = sessions.antfly_db_sql_session_close(fixture.id, id);
    _ = try Probe.run(fixture.id, id, "BEGIN", null);
    _ = try Probe.run(fixture.id, id, "INSERT INTO children (id,parent_id) VALUES (1,1)", null);
    const session = fixture.connection.sql_sessions.get(id).?;
    const before = session.test_overlay_rebuilds;
    _ = try Probe.run(fixture.id, null, "INSERT INTO parents (id) VALUES (2)", null);
    // Switching to an FK-connected target cannot hide a relevant peer change.
    _ = try Probe.run(fixture.id, id, "INSERT INTO parents (id) VALUES (3)", null);
    try std.testing.expectEqual(before + 1, session.test_overlay_rebuilds);
    _ = try Probe.run(fixture.id, id, "INSERT INTO children (id,parent_id) VALUES (2,2)", null);
    try std.testing.expectEqual(before + 1, session.test_overlay_rebuilds);
    _ = try Probe.run(fixture.id, id, "COMMIT", null);
    try std.testing.expectEqual(@as(?i64, 2), try Probe.run(fixture.id, null, "SELECT COUNT(*) FROM children", null));
}

test "capi native SQL constraint overlay evicts optional allocation failures without losing staged writes" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("sql-constraint-overlay-budget", 5000, .batch);
    defer fixture.deinit();
    const Probe = ConstraintOverlayProbe;
    _ = try Probe.run(fixture.id, null, "CREATE TABLE items (id BIGINT PRIMARY KEY, payload TEXT)", null);
    var id: u64 = 0;
    const sessions = @import("sql_session.zig");
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(fixture.id, &id));
    defer _ = sessions.antfly_db_sql_session_close(fixture.id, id);
    _ = try Probe.run(fixture.id, id, "BEGIN", null);
    _ = try Probe.run(fixture.id, id, "INSERT INTO items (id,payload) VALUES (1,'small')", null);
    const session = fixture.connection.sql_sessions.get(id).?;
    try std.testing.expect(session.constraint_overlay != null);
    // Fail only optional cache growth, after authoritative statement planning
    // and raw postimage ownership have succeeded. Existing arena frees still
    // refund the original transaction budget through this allocator.
    var failures = std.testing.FailingAllocator.init(session.budget.allocator(), .{ .fail_index = 0, .resize_fail_index = 0 });
    session.constraint_overlay.?.value.arena.child_allocator = failures.allocator();
    const payload: [65536]u8 = @splat('x');
    const statement = try std.fmt.allocPrint(std.testing.allocator, "INSERT INTO items (id,payload) VALUES (2,'{s}')", .{payload});
    defer std.testing.allocator.free(statement);
    _ = try Probe.run(fixture.id, id, statement, null);
    try std.testing.expect(session.constraint_overlay == null);
    _ = try Probe.run(fixture.id, id, "INSERT INTO items (id,payload) VALUES (3,'after eviction')", null);
    try std.testing.expectEqual(@as(?i64, 3), try Probe.run(fixture.id, id, "SELECT COUNT(*) FROM items", null));
    _ = try Probe.run(fixture.id, id, "COMMIT", null);
    try std.testing.expectEqual(@as(usize, 0), session.budget.live);
    try std.testing.expectEqual(@as(?i64, 3), try Probe.run(fixture.id, null, "SELECT COUNT(*) FROM items", null));
}

test "capi native SQL constraint overlay preserves foreign key cascades duplicate errors and savepoints" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("sql-constraint-overlay-fk", 5000, .batch);
    defer fixture.deinit();
    const Probe = ConstraintOverlayProbe;
    _ = try Probe.run(fixture.id, null, "CREATE TABLE parents (id BIGINT PRIMARY KEY)", null);
    _ = try Probe.run(fixture.id, null, "CREATE TABLE children (id BIGINT PRIMARY KEY, parent_id BIGINT REFERENCES parents(id) ON DELETE CASCADE)", null);
    var session: u64 = 0;
    const sessions = @import("sql_session.zig");
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(fixture.id, &session));
    defer _ = sessions.antfly_db_sql_session_close(fixture.id, session);
    _ = try Probe.run(fixture.id, session, "BEGIN", null);
    _ = try Probe.run(fixture.id, session, "INSERT INTO parents (id) VALUES (1)", null);
    _ = try Probe.run(fixture.id, session, "INSERT INTO children (id,parent_id) VALUES (1,1)", null);
    _ = try Probe.run(fixture.id, session, "SAVEPOINT valid", null);
    _ = try Probe.run(fixture.id, session, "INSERT INTO children (id,parent_id) VALUES (1,1)", "23505");
    _ = try Probe.run(fixture.id, session, "ROLLBACK TO SAVEPOINT valid", null);
    _ = try Probe.run(fixture.id, session, "DELETE FROM parents WHERE id=1", null);
    try std.testing.expectEqual(@as(?i64, 0), try Probe.run(fixture.id, session, "SELECT COUNT(*) FROM children", null));
    _ = try Probe.run(fixture.id, session, "ROLLBACK TO SAVEPOINT valid", null);
    try std.testing.expectEqual(@as(?i64, 1), try Probe.run(fixture.id, session, "SELECT COUNT(*) FROM children", null));
    _ = try Probe.run(fixture.id, session, "COMMIT", null);
    try std.testing.expectEqual(@as(?i64, 1), try Probe.run(fixture.id, null, "SELECT COUNT(*) FROM children", null));
}

fn seedPausedPrivateMerge(fixture: *AdmissionFollowupFixture) !*PendingTextMerge {
    return seedPausedPrivateMergeInTable(fixture, null);
}

fn seedPausedPrivateMergeInTable(fixture: *AdmissionFollowupFixture, table_name: ?[]const u8) !*PendingTextMerge {
    return seedPausedPrivateMergeWithDeletes(fixture, table_name, false);
}

fn seedPausedPrivateMergeWithDeletes(fixture: *AdmissionFollowupFixture, table_name: ?[]const u8, delete_inputs: bool) !*PendingTextMerge {
    return seedPausedPrivateMergeFixture(fixture, table_name, delete_inputs, false);
}

fn seedPausedPrivateMergeFixture(fixture: *AdmissionFollowupFixture, table_name: ?[]const u8, delete_inputs: bool, primary_canary: bool) !*PendingTextMerge {
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    if (table_name == null) try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    const owner = fixture.connection.owner;
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try owner.refresh();
        const database = if (table_name) |name| blk: {
            try @import("tables.zig").create(root, name, "{}", false);
            const child = try @import("tables.zig").get(root, name);
            try child.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
            break :blk child;
        } else &root.db;
        if (primary_canary) try database.batch(.{ .writes = &.{.{ .key = "initial-source", .value = "{\"text\":\"initial source\"}" }}, .sync_level = .write });
        const entry = database.core.index_manager.textIndexEntry("text").?;
        for (0..12) |i| {
            var id: [32]u8 = undefined;
            var writer = segment_mod.SegmentWriter.init(std.testing.allocator);
            defer writer.deinit();
            try writer.addStoredDoc(try std.fmt.bufPrint(&id, "private-{d}", .{i}), "{}");
            const bytes = try writer.build();
            defer std.testing.allocator.free(bytes);
            try entry.persistent.indexSegment(bytes);
        }
        if (delete_inputs) for (0..12) |i| {
            var id: [32]u8 = undefined;
            try std.testing.expect(try entry.persistent.deleteById(try std.fmt.bufPrint(&id, "private-{d}", .{i})));
        };
        entry.compaction_pending.store(true, .release);
    }
    test_private_merge_entered.store(false, .release);
    test_pause_private_merge.store(true, .release);
    errdefer test_pause_private_merge.store(false, .release);
    for (0..64) |_| {
        owner.maintenance_pending = true;
        _ = try owner.maintenanceTurn();
        if (owner.pending_text_merge != null) break;
    }
    const pending = owner.pending_text_merge orelse return error.PrivateMergeWasNotScheduled;
    for (0..1000) |_| {
        if (test_private_merge_entered.load(.acquire)) break;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(test_private_merge_entered.load(.acquire));
    try std.testing.expect(pending.owns_root);
    try std.testing.expect(pending.root.maintenance_runtime_references.load(.acquire) >= 1);
    if (owner.maintenance_snapshot == pending.root)
        try std.testing.expect(pending.root.maintenance_runtime_references.load(.acquire) >= 2);
    try std.testing.expect(!pending.done.load(.acquire));
    return pending;
}

test "capi native private text construction owns scratch across inline catalog relocation" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("private-merge-owned-scratch", 5000, .batch);
    defer fixture.deinit();
    defer test_pause_private_merge.store(false, .release);
    const pending = try seedPausedPrivateMergeWithDeletes(&fixture, null, false);
    const manager = pending.database.core.index_manager;
    {
        try pending.database.lockApplyForPortableRuntime();
        defer pending.database.core.unlockApply();
        manager.catalog_mutex.lockExclusive();
        defer manager.catalog_mutex.unlockExclusive();
        // Mirror the backing-array relocation caused by an unrelated index
        // creation. The worker is paused after capture, so its old persistent
        // pointer must never be consulted when construction resumes.
        const old = manager.text_indexes.allocatedSlice();
        const replacement = try manager.alloc.alloc(@TypeOf(old[0]), old.len + 1);
        @memcpy(replacement[0..manager.text_indexes.items.len], manager.text_indexes.items);
        manager.text_indexes.items = replacement[0..manager.text_indexes.items.len];
        manager.text_indexes.capacity = replacement.len;
        manager.alloc.free(old);
    }
    test_pause_private_merge.store(false, .release);
    for (0..1000) |_| {
        if (pending.done.load(.acquire)) break;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(pending.done.load(.acquire));
    if (pending.failure) |err| return err;
    try std.testing.expect(pending.result != null);
    try std.testing.expect(pending.result.?.run != null);
    fixture.connection.owner.discardPendingTextMerge();
    try std.testing.expect(fixture.connection.owner.pending_text_merge == null);
}

test "capi native asynchronous text merge coowns reusable runtimes through replay and cache eviction" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("shared-text-merge-runtime", 5000, .batch);
    defer fixture.deinit();
    defer test_pause_private_merge.store(false, .release);
    const pending = try seedPausedPrivateMergeFixture(&fixture, "shared-merge", false, true);
    const owner = fixture.connection.owner;
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const foreground = try owner.refresh();
        const table = try @import("tables.zig").get(foreground, "shared-merge");
        owner.clearTextSourceRuntimeCache();
        if (owner.maintenance_snapshot != pending.root) {
            if (owner.maintenance_snapshot) |old| Owner.closeMaintenanceSnapshot(old);
            Owner.retainMaintenanceSnapshot(pending.root);
            owner.maintenance_snapshot = pending.root;
        }
        const shared_store = pending.root.owned_lite_backend.?.native_docstore.?;
        // The admitted foreground may have published fresh immutable-reader
        // aliases. Those do not change the captured merge's input identity.
        try shared_store.refreshIndexPublicationCheckpoint(foreground.owned_lite_backend.?.native_docstore.?.file.header);
        owner.text_source_runtime_cache = (try TextSourceRuntimeCache.capture(pending.root)) orelse return error.TestUnexpectedResult;
        owner.maintenance_retained_logical = shared_store.file.activeCheckpoint().logical;
        const opens = owner.test_runtime_snapshot_opens;
        try table.batch(.{ .writes = &.{.{ .key = "continued-source", .value = "{\"text\":\"continued source\"}" }}, .sync_level = .write });
        const reused = try owner.refreshMaintenanceSnapshot(foreground, .source);
        try std.testing.expectEqual(@intFromPtr(pending.root), @intFromPtr(reused));
        try std.testing.expectEqual(opens, owner.test_runtime_snapshot_opens);
        try std.testing.expect(!pending.done.load(.acquire));
        // A changed immutable input really does require a fresh runtime. The
        // merge's independent strong reference keeps its old owner alive.
        var writer = segment_mod.SegmentWriter.init(std.testing.allocator);
        defer writer.deinit();
        try writer.addStoredDoc("new-immutable-input", "{}");
        const bytes = try writer.build();
        defer std.testing.allocator.free(bytes);
        try table.core.index_manager.textIndexEntry("text").?.persistent.indexSegment(bytes);
        const replacement = try owner.refreshMaintenanceSnapshot(foreground, .source);
        try std.testing.expect(replacement != pending.root);
        try std.testing.expectEqual(@as(usize, 1), pending.root.maintenance_runtime_references.load(.acquire));
        try std.testing.expect(!pending.done.load(.acquire));
    }
    // Joins the canceled immutable worker before releasing its last owner.
    owner.discardPendingTextMerge();
    try std.testing.expect(owner.pending_text_merge == null);
    try fixture.expectWritten(.batch, true);
}

test "capi native asynchronous text merge staging cannot certify foreign index publications" {
    inline for (.{ false, true }) |foreign| {
        if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
        var fixture = try AdmissionFollowupFixture.init("merge-stage-foreign-input", 5000, .batch);
        defer fixture.deinit();
        defer test_pause_private_merge.store(false, .release);
        const pending = try seedPausedPrivateMergeFixture(&fixture, "staged-merge", false, true);
        const owner = fixture.connection.owner;
        {
            var lease = try fixture.connection.acquire();
            defer {
                lease.close();
                fixture.connection.gate.mutex.unlock(h.handleLockIo());
            }
            const foreground = try owner.refresh();
            const table = try @import("tables.zig").get(foreground, "staged-merge");
            owner.clearTextSourceRuntimeCache();
            if (owner.maintenance_snapshot != pending.root) {
                if (owner.maintenance_snapshot) |old| Owner.closeMaintenanceSnapshot(old);
                Owner.retainMaintenanceSnapshot(pending.root);
                owner.maintenance_snapshot = pending.root;
            }
            const store = pending.root.owned_lite_backend.?.native_docstore.?;
            try store.refreshIndexPublicationCheckpoint(foreground.owned_lite_backend.?.native_docstore.?.file.header);
            owner.text_source_runtime_cache = (try TextSourceRuntimeCache.capture(pending.root)) orelse return error.TestUnexpectedResult;
            owner.maintenance_retained_logical = store.file.activeCheckpoint().logical;
            // Advance the actual LSM journal behind the detached runtime. Its next
            // edit must come from a fresh runtime even if private staging rebases
            // the old NativeFile to the new physical checkpoint first.
            for (0..@as(usize, if (foreign) 2 else 0)) |i| {
                var writer = segment_mod.SegmentWriter.init(std.testing.allocator);
                defer writer.deinit();
                var id: [32]u8 = undefined;
                try writer.addStoredDoc(try std.fmt.bufPrint(&id, "foreign-{d}", .{i}), "{}");
                const bytes = try writer.build();
                defer std.testing.allocator.free(bytes);
                try table.core.index_manager.textIndexEntry("text").?.persistent.indexSegment(bytes);
            }
            try std.testing.expectEqual(!foreign, try owner.text_source_runtime_cache.?.matches(&foreground.owned_lite_backend.?.native_docstore.?.file));
        }
        test_pause_private_merge.store(false, .release);
        for (0..1000) |_| {
            if (pending.done.load(.acquire)) break;
            try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expect(pending.done.load(.acquire));
        try owner.stagePendingTextMerge(pending);
        const cache = owner.text_source_runtime_cache orelse return error.TestUnexpectedResult;
        const staged_file = &pending.root.owned_lite_backend.?.native_docstore.?.file;
        try std.testing.expectEqual(!foreign, try cache.matches(staged_file));
        try std.testing.expectEqual(!foreign, std.meta.eql(owner.maintenance_retained_logical.?, staged_file.activeCheckpoint().logical));
        {
            var lease = try fixture.connection.acquire();
            defer {
                lease.close();
                fixture.connection.gate.mutex.unlock(h.handleLockIo());
            }
            const foreground = try owner.refresh();
            const replacement = try owner.refreshMaintenanceSnapshot(foreground, .source);
            try std.testing.expectEqual(foreign, replacement != pending.root);
            const table = try @import("tables.zig").get(replacement, "staged-merge");
            // Publish from the replacement and cold-open its journal. This rejects
            // a stale sequence even when physical integrity remains valid.
            var writer = segment_mod.SegmentWriter.init(std.testing.allocator);
            defer writer.deinit();
            try writer.addStoredDoc("after-stage", "{}");
            const bytes = try writer.build();
            defer std.testing.allocator.free(bytes);
            const index = &table.core.index_manager.textIndexEntry("text").?.persistent;
            try index.indexSegment(bytes);
            const reopened = try owner.refresh();
            const reopened_table = try @import("tables.zig").get(reopened, "staged-merge");
            try std.testing.expectEqual(index.snapshot().segments.len, reopened_table.core.index_manager.textIndexEntry("text").?.persistent.snapshot().segments.len);
            try std.testing.expect((try reopened.owned_lite_backend.?.native_docstore.?.file.check()).valid);
        }
        owner.discardPendingTextMerge();
    }
}

test "capi native asynchronous text merge removes inputs already fully deleted at capture" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("private-merge-all-deleted", 5000, .batch);
    defer fixture.deinit();
    defer test_pause_private_merge.store(false, .release);
    const pending = try seedPausedPrivateMergeWithDeletes(&fixture, "empty-merge", true);
    const owner = fixture.connection.owner;
    const ids = try std.testing.allocator.alloc(u64, pending.task.source.len);
    defer std.testing.allocator.free(ids);
    for (pending.task.source, pending.task.merge_indices, ids) |source, index, *id| {
        id.* = source.id;
        try std.testing.expectEqual(@as(usize, pending.task.snapshot.segments[index].reader.doc_count), source.deleted.?.cardinality());
    }
    const applied_before = owner.publication_stats.text_merge_jobs_applied;
    const staged_before = owner.publication_stats.text_merge_staged_bytes;
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try owner.refresh();
        const database = try @import("tables.zig").get(root, "empty-merge");
        const entry = database.core.index_manager.textIndexEntry("text").?;
        var writer = segment_mod.SegmentWriter.init(std.testing.allocator);
        defer writer.deinit();
        try writer.addStoredDoc("newer-survivor", "{}");
        const bytes = try writer.build();
        defer std.testing.allocator.free(bytes);
        try entry.persistent.indexSegment(bytes);
    }
    test_pause_private_merge.store(false, .release);
    for (0..256) |_| {
        _ = try owner.maintenanceTurn();
        if (owner.publication_stats.text_merge_jobs_applied > applied_before) break;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(owner.publication_stats.text_merge_jobs_applied > applied_before);
    try std.testing.expectEqual(staged_before, owner.publication_stats.text_merge_staged_bytes);
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const root = try owner.refresh();
    const database = try @import("tables.zig").get(root, "empty-merge");
    const snapshot = database.core.index_manager.textIndexEntry("text").?.persistent.snapshot();
    try std.testing.expectEqual(@as(u32, 1), snapshot.liveDocCount());
    for (snapshot.segments) |segment| for (ids) |id| try std.testing.expect(segment.id != id);
    try std.testing.expect((try root.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}

test "capi native asynchronous text merge preserves newer segments and deletion races while replay advances" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("private-merge-replay-and-deletes", 5000, .batch);
    defer fixture.deinit();
    defer test_pause_private_merge.store(false, .release);
    const pending = try seedPausedPrivateMerge(&fixture);
    const owner = fixture.connection.owner;
    defer owner.test_text_merge_segment_limit = null;
    var selected_id: ?[]u8 = null;
    for (pending.task.merge_indices) |index| {
        selected_id = try pending.task.snapshot.segments[index].reader.storedIdAlloc(std.testing.allocator, 0);
        if (selected_id != null) break;
    }
    const deleted_id = selected_id orelse return error.StoredMergeFixtureMissing;
    defer std.testing.allocator.free(deleted_id);
    const sources_before = owner.publication_stats.maintenance_source_turns;
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "new-source-during-merge", .value = "{\"text\":\"new primary source\"}" }}, .sync_level = .write });
    for (0..32) |_| {
        _ = try owner.maintenanceTurn();
        if (owner.publication_stats.maintenance_source_turns > sources_before) break;
    }
    try std.testing.expect(owner.publication_stats.maintenance_source_turns > sources_before);
    try std.testing.expect(owner.pending_text_merge == pending);
    {
        // A Connection lease starts an epoch-sharing batch. Inspect outside
        // that batch so the probe itself cannot create the observed frontier.
        const io = h.handleLockIo();
        owner.gate.mutex.lockUncancelable(io);
        defer owner.gate.mutex.unlock(io);
        var lease = try native.lockWriterPathWithIo(alloc, io, fixture.path);
        defer lease.close();
        var peer = try native.NativeFile.openWithIo(alloc, io, fixture.path, .{ .externally_locked = true, .no_sync = true });
        defer peer.close();
        peer.reader_coordinator = owner.reader_coordinator;
        try peer.inheritAllocatorVerification(&owner.current.?.owned_lite_backend.?.native_docstore.?.file);
        if (!peer.free_pages_verified) _ = try peer.inheritAllocatorCertificate();
        peer.allocator_certificate_on_commit = true;
        const epochs = try @import("../storage/lite/reader_epochs.zig").scan(alloc, io, fixture.path, peer.header.reader_epoch_namespace, (try peer.file.stat(io)).inode);
        try std.testing.expect(epochs.data == null);
        const reused_before = (try peer.allocatorStats()).?.reused_pages;
        const physical_value: [8192]u8 = @splat('v');
        for (0..64) |_| try peer.putDocument("private-merge-physical-reuse", &physical_value);
        try std.testing.expect((try peer.allocatorStats()).?.reused_pages > reused_before);
    }
    var expected_live: u32 = 0;
    var appended_id: u64 = 0;
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const current = try owner.refresh();
        const entry = current.db.core.index_manager.textIndexEntry("text").?;
        expected_live = entry.persistent.snapshot().liveDocCount();
        try std.testing.expect(try entry.persistent.deleteById(deleted_id));
        var writer = segment_mod.SegmentWriter.init(std.testing.allocator);
        defer writer.deinit();
        try writer.addStoredDoc("appended-during-merge", "{}");
        const bytes = try writer.build();
        defer std.testing.allocator.free(bytes);
        try entry.persistent.indexSegment(bytes);
        const snapshot = entry.persistent.snapshot();
        appended_id = snapshot.segments[snapshot.segments.len - 1].id;
        try std.testing.expectEqual(expected_live, snapshot.liveDocCount());
        owner.test_text_merge_segment_limit = snapshot.segments.len;
    }
    api.antfly_buffer_free_zero(&output);
    try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "blocked-source-during-merge", .value = "{\"text\":\"capacity must return before this can replay\"}" }}, .sync_level = .write });
    // At the current segment limit this new record cannot publish. A ready
    // merge must take an artifact-only turn before replay retries its capacity.
    for (0..8) |_| _ = try owner.maintenanceTurn();
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const current = try owner.refresh();
        try std.testing.expectEqual(owner.test_text_merge_segment_limit.?, current.db.core.index_manager.textIndexEntry("text").?.persistent.snapshot().segments.len);
    }
    test_pause_private_merge.store(false, .release);
    for (0..1000) |_| {
        _ = try owner.maintenanceTurn();
        if (owner.pending_text_merge == null) break;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(owner.pending_text_merge == null);
    for (0..32) |_| _ = try owner.maintenanceTurn();
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const current = try owner.refresh();
    const snapshot = current.db.core.index_manager.textIndexEntry("text").?.persistent.snapshot();
    try std.testing.expectEqual(expected_live + 1, snapshot.liveDocCount());
    var appended_present = false;
    for (snapshot.segments) |segment| if (segment.id == appended_id) {
        appended_present = true;
    };
    try std.testing.expect(appended_present);
    try std.testing.expect(owner.publication_stats.text_merge_jobs_applied != 0);
    try std.testing.expect(owner.publication_stats.text_merge_prepare_ns != 0);
    try std.testing.expect(owner.publication_stats.text_merge_staged_bytes != 0);
    try std.testing.expect((try current.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}

test "capi native asynchronous text merge retires a ready result when its namespace is dropped" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("private-merge-drop-ready-namespace", 5000, .batch);
    defer fixture.deinit();
    defer test_pause_private_merge.store(false, .release);
    const pending = try seedPausedPrivateMergeInTable(&fixture, "merge_scope");
    const owner = fixture.connection.owner;
    try std.testing.expectEqualStrings("merge_scope", pending.name);
    test_pause_private_merge.store(false, .release);
    for (0..1000) |_| {
        _ = try owner.maintenanceTurn();
        if (owner.pending_text_merge == pending and pending.ready) break;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(owner.pending_text_merge == pending and pending.ready);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("tables.zig").antfly_db_drop_table(fixture.id, .fromSlice("merge_scope")));
    var rejected = false;
    for (0..8) |_| {
        _ = owner.maintenanceTurn() catch |err| failed: {
            if (err != error.EnrichmentSourceChanged) return err;
            rejected = true;
            break :failed true;
        };
        if (rejected) break;
    }
    try std.testing.expect(rejected);
    try std.testing.expect(owner.pending_text_merge == null);
    try std.testing.expect(!owner.publication_stats.text_merge_in_flight);
    try fixture.expectWritten(.batch, true);
}

test "capi native asynchronous text merge rejects DDL and drains canceled construction on close" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const Scenario = enum { close, create, delete };
    for ([_]Scenario{ .close, .create, .delete }) |scenario| {
        var fixture = try AdmissionFollowupFixture.init("private-merge-lifecycle", 5000, .batch);
        var closed = false;
        defer if (!closed) fixture.deinit();
        defer test_pause_private_merge.store(false, .release);
        _ = try seedPausedPrivateMerge(&fixture);
        if (scenario == .close) {
            const started = @import("antfly_platform").time.monotonicNs();
            fixture.deinit();
            closed = true;
            try std.testing.expect(@import("antfly_platform").time.monotonicNs() -| started < 5 * std.time.ns_per_s);
        } else {
            if (scenario == .create) {
                try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"unrelated\",\"type\":\"full_text\"}")));
            } else {
                var deleted = false;
                try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_delete_index(fixture.id, .fromSlice("text"), &deleted));
                try std.testing.expect(deleted);
            }
            test_pause_private_merge.store(false, .release);
            var rejected = false;
            for (0..1000) |_| {
                _ = fixture.connection.owner.maintenanceTurn() catch |err| failed: {
                    if (err != error.EnrichmentSourceChanged) return err;
                    rejected = true;
                    break :failed true;
                };
                if (rejected) break;
                try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
            }
            try std.testing.expect(rejected);
            try std.testing.expect(fixture.connection.owner.pending_text_merge == null);
            try fixture.expectWritten(.batch, true);
        }
    }
}

test "capi native asynchronous text merge preserves resource retry hints across fresh runtimes and yields producer admission" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("private-merge-budget-retry", 5000, .batch);
    defer fixture.deinit();
    defer test_pause_private_merge.store(false, .release);
    defer test_private_merge_budget_failures.store(0, .release);
    const pending = try seedPausedPrivateMerge(&fixture);
    const attempted = pending.task.source.len;
    const table_id = pending.database.core.identity_namespace.table_id;
    const incarnation = pending.database.root_incarnation;
    // The actor must yield immediately; waiting or assisting here would block
    // the very lane which stages the independent construction's result.
    try pending.database.initOptionalTextMergeRuntime(.{ .enabled = true, .max_pending_segments = 1, .max_pending_bytes = 0, .manual_producer_assistance = true, .defer_producer_to_owner = true });
    const started = @import("antfly_platform").time.monotonicNs();
    try std.testing.expectError(error.TextMergeProducerDeferred, pending.database.text_merge_runtime.?.acquireProducerPermit("text", 1, 0));
    try std.testing.expect(@import("antfly_platform").time.monotonicNs() -| started < std.time.ns_per_s);
    test_private_merge_budget_failures.store(1, .release);
    test_pause_private_merge.store(false, .release);
    const owner = fixture.connection.owner;
    var rejected = false;
    for (0..1000) |_| {
        _ = owner.maintenanceTurn() catch |err| failed: {
            if (err != error.ResourceBudgetExceeded) return err;
            rejected = true;
            break :failed true;
        };
        if (rejected) break;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(rejected);
    try std.testing.expect(owner.pending_text_merge == null);
    const schedule = owner.maintenance_schedules.get(.{ .table_id = table_id, .incarnation = incarnation }) orelse return error.MissingMergeRetrySchedule;
    try std.testing.expect(schedule.artifact_pending);
    try std.testing.expectEqual(@as(i96, 0), schedule.next_artifact_ns);
    try std.testing.expectEqual(@as(i64, 100), owner.maintenanceWaitMillisForTest(true));
    test_pause_private_merge.store(true, .release);
    for (0..64) |_| {
        owner.maintenance_pending = true;
        _ = try owner.maintenanceTurn();
        if (owner.pending_text_merge != null) break;
    }
    const retry = owner.pending_text_merge orelse return error.PrivateMergeRetryWasNotScheduled;
    try std.testing.expect(retry.task.source.len <= @max(2, attempted / 2));
}

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
    // A queue participant waits without a blocking kernel task. Deliberately
    // use a legacy flock-only holder to exercise the bounded executor fallback.
    const lock_path = try std.fmt.allocPrint(std.testing.allocator, "{s}.lock", .{fixture.path});
    defer std.testing.allocator.free(lock_path);
    const held = try std.Io.Dir.cwd().openFile(io, lock_path, .{ .mode = .read_write, .lock = .exclusive, .lock_nonblocking = true });
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
    held.close(io);
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
    const io = h.handleLockIo();
    const lock_path = try std.fmt.allocPrint(std.testing.allocator, "{s}.lock", .{fixture.path});
    defer std.testing.allocator.free(lock_path);
    const held = try std.Io.Dir.cwd().openFile(io, lock_path, .{ .mode = .read_write, .lock = .exclusive, .lock_nonblocking = true });
    const turn = owner.maintenanceTurn();
    held.close(io);
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
    // Quiescent SQL returns its mutable ledger to the path cache. The
    // serving wrapper keeps checked authority without retaining that loan.
    const cache = &owner.gate.writer_allocator_cache;
    const ledger = cache.holder.?.ledger.?;
    const file = &root.owned_lite_backend.?.native_docstore.?.file;
    const initial_work = file.allocatorWorkSnapshot();
    for (0..8) |i| {
        const before = ledger.digestWork();
        var request: [160]u8 = undefined;
        const bytes = try std.fmt.bufPrint(&request, "{{\"statement\":\"INSERT INTO numbers (n, _id) VALUES (1, 'warm-{d}')\"}}", .{i});
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(fixture.id, .fromSlice(bytes), &output));
        api.antfly_buffer_free_zero(&output);
        try std.testing.expect(owner.current == root);
        try std.testing.expect(cache.holder.?.ledger == ledger);
        const work = file.allocatorWorkSnapshot();
        try std.testing.expectEqual(initial_work.full_loads, work.full_loads);
        try std.testing.expectEqual(initial_work.digest_rebuilds, work.digest_rebuilds);
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
    try publication.validateCapturedStorageCurrent();
    try std.testing.expect(!root.embedded_tables.contains("numbers"));
    try std.testing.expectEqualDeep(header, root.owned_lite_backend.?.native_docstore.?.file.header);
    const id = root.embedded_table_records.getPtr("numbers").?;
    const original_id = id.*;
    id.* += 1;
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateCapturedStorageCurrent());
    id.* = original_id;
    publication.incarnation +%= 1;
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateCapturedStorageCurrent());
    publication.incarnation -%= 1;
    var txn = try database.core.store.beginWriteTxn();
    errdefer txn.abort();
    try txn.put(keys.public_schema_json_key, "{\"changed\":true}");
    try txn.commit();
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateCapturedStorageCurrent());
    try std.testing.expect(!root.embedded_tables.contains("numbers"));
}

test "capi native maintenance nested admission preserves index and outcome fences" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("nested-publication-fences", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    const root = blk: {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        break :blk try owner.refreshMaintenanceSnapshot(try owner.refresh(), .source);
    };
    var publication_overhead: std.atomic.Value(u64) = .init(0);
    var publication: PreparedPublication = .{
        .owner = owner,
        .name = "default",
        .identity = root.db.core.identity_namespace,
        .incarnation = root.db.root_incarnation,
        .catalog = null,
        .index_prefix = "unused/",
        .snapshot_root = root,
        .snapshot_database = &root.db,
        .publication_overhead_ns = &publication_overhead,
        .source_read = null,
        .relational_rows = false,
    };
    root.maintenance_owner = owner;
    root.maintenance_publication = &publication;
    defer {
        root.maintenance_owner = null;
        root.maintenance_publication = null;
    }
    const hold_before = owner.publication_stats.publication_hold_ns;
    const site_before = owner.publication_stats.maintenance_hold_sites.writer_primary.count;
    try Owner.enterMaintenanceWriter(root, .primary);
    var held = true;
    defer if (held) Owner.leaveMaintenanceWriter(root);
    publication.index_inputs_changed = true;
    for ([_]PublicationScope{ .index, .index_read }) |scope|
        try std.testing.expectError(error.EnrichmentWaitCanceled, Owner.enterMaintenanceWriter(root, scope));
    try std.testing.expectEqual(@as(usize, 1), root.maintenance_writer_references);
    // A completed primary publication may still record its replay watermark.
    const before_nested_overhead = publication_overhead.load(.monotonic);
    try Owner.enterMaintenanceWriter(root, .primary);
    try std.testing.expectEqual(@as(usize, 2), root.maintenance_writer_references);
    Owner.leaveMaintenanceWriter(root);
    try std.testing.expectEqual(hold_before, owner.publication_stats.publication_hold_ns);
    try std.testing.expectEqual(before_nested_overhead, publication_overhead.load(.monotonic));
    const file = &root.owned_lite_backend.?.native_docstore.?.file;
    file.checkpoint_publication_uncertain = true;
    defer file.checkpoint_publication_uncertain = false;
    for ([_]PublicationScope{ .primary, .index, .index_read, .allocator_service, .reclamation }) |scope|
        try std.testing.expectError(error.OutcomeReconciliationRequired, Owner.enterMaintenanceWriter(root, scope));
    try std.testing.expectEqual(@as(usize, 1), root.maintenance_writer_references);
    // Deterministic elapsed time verifies that only the outermost release
    // charges both the aggregate controller feedback and per-site receipt.
    root.maintenance_publication_started_ns = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds - 5 * std.time.ns_per_ms;
    const feedback_before_release = publication_overhead.load(.monotonic);
    Owner.leaveMaintenanceWriter(root);
    held = false;
    const hold_delta = owner.publication_stats.publication_hold_ns - hold_before;
    try std.testing.expect(hold_delta >= 5 * std.time.ns_per_ms);
    try std.testing.expectEqual(hold_delta, publication_overhead.load(.monotonic) - feedback_before_release);
    try std.testing.expectEqual(site_before + 1, owner.publication_stats.maintenance_hold_sites.writer_primary.count);
    try std.testing.expectEqual(@as(?i96, null), root.maintenance_publication_started_ns);
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
        // SQL records initial admission and re-entry after owned preparation;
        // eligible artifact staging/assembly can add more bounded entries.
        // Regardless of those publication stages, the stable proof runs once.
        const admissions = stats.mutation_admission_count - admissions_before;
        switch (operation) {
            .read => try std.testing.expectEqual(@as(u64, 0), admissions),
            .batch => try std.testing.expectEqual(@as(u64, 1), admissions),
            .sql => try std.testing.expect(admissions >= 2),
        }
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

test "capi native allocator proof initial admission honors the local wait budget" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const io = h.handleLockIo();
    var fixture = try AdmissionFollowupFixture.init("queued-stable-proof", 50, .batch);
    defer fixture.deinit();
    const Writer = struct {
        fixture: *AdmissionFollowupFixture,
        result: h.capi.ErrorCode = .busy,
        done: std.Io.Event = .unset,
        fn run(self: *@This()) void {
            self.result = self.fixture.call(.batch);
            self.done.set(h.handleLockIo());
        }
    };
    for ([_]*QueuedMutex{ &fixture.connection.mutex, &fixture.connection.gate.mutex }) |mutex| {
        var writer: Writer = .{ .fixture = &fixture };
        mutex.lockUncancelable(io);
        const thread = std.Thread.spawn(.{}, Writer.run, .{&writer}) catch |err| {
            mutex.unlock(io);
            return err;
        };
        // Expiry must finish while the preceding call still owns its mutex.
        // Both the per-connection lane and the shared path lane consume the
        // public call's budget, without unlocking somebody else's admission.
        const finished_while_held = if (writer.done.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } })) |_| true else |_| false;
        mutex.unlock(io);
        thread.join();
        try std.testing.expect(finished_while_held);
        try std.testing.expectEqual(h.capi.ErrorCode.busy, writer.result);
        try fixture.expectWritten(.batch, false);
    }
}

test "capi native allocator proof suspended call preserves its unspent admission budget" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const io = h.handleLockIo();
    var fixture = try AdmissionFollowupFixture.init("resumed-budget", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var guard = api.enterHandle(fixture.id, .read).?;
    defer guard.leave();
    try std.testing.expect(guard.entry_error == null);
    // Model the remaining balance after earlier admission waits. Independent
    // computation may outlast this balance without consuming another wait.
    const balance: i96 = 50 * std.time.ns_per_ms;
    guard.admission_budget_ns = balance;
    guard.suspendQuery();
    try io.sleep(.fromMilliseconds(100), .awake);
    var held: ?native.PathWriterLock = try native.lockWriterPathWithIo(alloc, io, fixture.path);
    defer if (held) |*lease| lease.close();
    const Probe = struct {
        connection: *Connection,
        held: *?native.PathWriterLock,
        remaining: ?i96 = null,
        fn busy(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.remaining = self.connection.admission_deadline_ns.? - std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds;
            self.held.*.?.close();
            self.held.* = null;
        }
    };
    var probe: Probe = .{ .connection = fixture.connection, .held = &held };
    fixture.connection.test_path_lease_busy = .{ .ptr = &probe, .notify = Probe.busy };
    defer fixture.connection.test_path_lease_busy = null;
    try std.testing.expect(guard.resumeQuery(fixture.id));
    try std.testing.expect(guard.entry_error == null);
    try std.testing.expect(probe.remaining != null and probe.remaining.? > 0 and probe.remaining.? <= balance);
    try std.testing.expect(guard.admission_budget_ns != null and guard.admission_budget_ns.? <= balance);
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
    // Foreground publication may now satisfy visibility despite the producer
    // permit. Inject the same error at the explicit durable boundary so this
    // contract always tests committed classification, not scheduler timing.
    const mutation_commit = @import("../storage/db/mutation_commit.zig");
    mutation_commit.test_fail_after_primary_commit = error.TextMergeBackpressureTimeout;
    defer mutation_commit.test_fail_after_primary_commit = null;
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
    _ = try file.reclaimPagesForMaintenanceWithCancel(8, null);
    try std.testing.expect((try file.check()).valid);
    Owner.leaveMaintenanceWriter(detached);
    held = false;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.read));
    try fixture.expectWritten(.batch, true);
}

test "capi native logical generations maintenance snapshot replacement carries the warm allocator ledger" {
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
            return f.connection.owner.refreshMaintenanceSnapshot(f.connection.owner.current.?, .storage);
        }
    }.run;
    const first = try refreshSnapshot(&fixture);
    first.maintenance_owner = owner;
    // Warm and verify the first snapshot's ledger under allocator service.
    try Owner.enterMaintenanceWriter(first, .allocator_service);
    _ = try first.owned_lite_backend.?.native_docstore.?.file.reclaimPagesForMaintenanceWithCancel(8, null);
    Owner.leaveMaintenanceWriter(first);
    first.maintenance_owner = null;
    const first_file = &first.owned_lite_backend.?.native_docstore.?.file;
    // An idle runtime returns its mutable ledger to the path cache. Measure
    // that shared owner, not a runtime-local cache that should now be empty.
    const cache = &owner.gate.writer_allocator_cache;
    try std.testing.expect(cache.borrower == null);
    const warm_ledger = cache.holder.?.ledger.?;
    try std.testing.expect(cache.holder.?.free_pages_verified);
    const before = first_file.allocatorWorkSnapshot();

    // A foreground write moves the tip and borrows the same allocator. The
    // replacement's bounded storage turn must not reload or copy the ledger.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const second = try refreshSnapshot(&fixture);
    try std.testing.expect(second != first);
    const file = &second.owned_lite_backend.?.native_docstore.?.file;
    second.maintenance_owner = owner;
    defer second.maintenance_owner = null;
    try Owner.enterMaintenanceWriter(second, .allocator_service);
    var held = true;
    defer if (held) Owner.leaveMaintenanceWriter(second);
    _ = try file.reclaimPagesForMaintenanceWithCancel(8, null);
    const after = file.allocatorWorkSnapshot();
    try std.testing.expectEqual(before.full_loads, after.full_loads);
    try std.testing.expectEqual(before.cache_copies, after.cache_copies);
    try std.testing.expect(after.cache_transfers > before.cache_transfers);
    try std.testing.expect(cache.borrower == null);
    try std.testing.expectEqual(warm_ledger, cache.holder.?.ledger.?);
    try std.testing.expect(cache.holder.?.free_pages_verified);
    // Integrity inspection deliberately uses independent allocator metadata;
    // its proof/load cost is separate from the service measurements above.
    try std.testing.expect((try file.check()).valid);
    Owner.leaveMaintenanceWriter(second);
    held = false;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.read));
    try fixture.expectWritten(.batch, true);
}

test "capi native allocator proof issue1431 replay queues exactly one separate physical service" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("separate-physical-service", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    const Probe = struct {
        calls: usize = 0,
        fn notify(ptr: *anyopaque, _: *h.Handle, _: bool) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
    };
    var probe: Probe = .{};
    owner.test_before_storage_service = .{ .ptr = &probe, .notify = Probe.notify };
    defer owner.test_before_storage_service = null;
    var saw_source = false;
    for (0..16) |_| {
        const before_source = owner.publication_stats.maintenance_source_turns;
        const before_service = probe.calls;
        owner.maintenance_pending = true;
        _ = try owner.maintenanceTurn();
        if (owner.publication_stats.maintenance_source_turns == before_source) continue;
        saw_source = true;
        try std.testing.expectEqual(before_service, probe.calls);
        try std.testing.expect(owner.maintenance_storage_pending);
        _ = try owner.maintenanceTurn();
        try std.testing.expectEqual(before_service + 1, probe.calls);
        break;
    }
    try std.testing.expect(saw_source);
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
    owner.test_before_storage_service = .{ .ptr = &probe, .notify = struct {
        fn notify(ptr: *anyopaque, root: *h.Handle, _: bool) void {
            Probe.notify(ptr, root);
        }
    }.notify };
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

test "capi native artifact yields preserve exact immutable leases across runtime teardown" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    for ([_]anyerror{ error.EnrichmentWaitCanceled, error.EnrichmentSourceChanged }) |yield_error| {
        var fixture = try AdmissionFollowupFixture.init("artifact-yield-handoff", 5000, .batch);
        defer fixture.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
        const owner = fixture.connection.owner;
        {
            var lease = try fixture.connection.acquire();
            defer {
                lease.close();
                fixture.connection.gate.mutex.unlock(h.handleLockIo());
            }
            const live = try owner.refresh();
            try live.owned_lite_backend.?.native_docstore.?.file.putIndexCatalogRecord("/yield-input", "protected immutable bytes");
        }
        const Source = @import("../segment_source.zig").Source;
        const Probe = struct {
            yield_error: anyerror,
            source: ?Source = null,
            calls: usize = 0,
            failure: ?anyerror = null,
            fn capture(ptr: *anyopaque, root: *h.Handle) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (root.maintenance_publication == null or self.source != null) return;
                self.source = root.owned_lite_backend.?.native_docstore.?.openArtifactSource("/yield-input") catch |err| {
                    self.failure = err;
                    return;
                };
                @import("../storage/db/catalog/index_manager.zig").test_before_text_merge_step = .{ .manager = root.db.core.index_manager, .ptr = self, .notify = merge };
            }
            fn merge(ptr: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.calls += 1;
                return self.yield_error;
            }
        };
        var probe: Probe = .{ .yield_error = yield_error };
        defer if (probe.source) |*source| source.close();
        defer @import("../storage/db/catalog/index_manager.zig").test_before_text_merge_step = null;
        owner.test_after_capture = .{ .ptr = &probe, .notify = Probe.capture };
        defer owner.test_after_capture = null;
        for (0..8) |_| {
            _ = try owner.maintenanceTurnForTest();
            if (probe.calls != 0 or probe.failure != null) break;
        }
        if (probe.failure) |err| return err;
        try std.testing.expect(probe.calls != 0);
        try std.testing.expect(owner.maintenance_snapshot != null and !owner.maintenance_snapshot.?.db_live);
        try std.testing.expect(owner.immutable_sources.count != 0);
        try std.testing.expectEqual(@as(u64, 1), owner.publication_stats.text_runtime_cache.immutable_yield_handoffs);
        const original_ptr = probe.source.?.ranges.ptr;
        probe.source.?.close();
        probe.source = null;
        owner.test_after_capture = null;
        @import("../storage/db/catalog/index_manager.zig").test_before_text_merge_step = null;
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const live = try owner.refresh();
        const replacement = try owner.refreshMaintenanceSnapshot(live, .source);
        var reused = try replacement.owned_lite_backend.?.native_docstore.?.openArtifactSource("/yield-input");
        defer reused.close();
        try std.testing.expectEqual(original_ptr, reused.ranges.ptr);
        var bytes: [25]u8 = undefined;
        try reused.readInto(0, &bytes);
        try std.testing.expectEqualStrings("protected immutable bytes", &bytes);
        // An uncertain commit must invalidate even immutable handoffs.
        replacement.sql_decision_uncertain = true;
        owner.preserveImmutableTextHandoff(replacement, &replacement.db);
        replacement.sql_decision_uncertain = false;
        try std.testing.expectEqual(@as(usize, 0), owner.immutable_sources.count);
        try std.testing.expect(owner.immutable_text_handoff == null);
    }
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
        peer_sequence: u64 = 0,
        failure: ?anyerror = null,
        fn capture(ptr: *anyopaque, root: *h.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.root = root;
            @import("../storage/db/catalog/index_manager.zig").test_before_text_merge_step = .{ .manager = root.db.core.index_manager, .ptr = self, .notify = merge };
        }
        fn merge(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.check() catch |err| {
                self.failure = err;
                return err;
            };
        }
        fn check(self: *@This()) !void {
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
                self.peer_sequence = peer.activeCheckpoint().commit_sequence;
                try std.testing.expect((try peer.check()).valid);
            }
            // Control reads after the source epoch ends must open on the
            // freshly admitted primary checkpoint, not an unretained old cut.
            // Read before the explicit catalog admission below: that admission
            // must not accidentally hide a missing fence on primary reads.
            {
                var primary = try store.beginRead();
                defer primary.abort();
                if (primary.checkpoint.commit_sequence < self.peer_sequence) std.debug.print("artifact primary checkpoint observed={d} required_at_least={d}\n", .{ primary.checkpoint.commit_sequence, self.peer_sequence });
                try std.testing.expect(primary.checkpoint.commit_sequence >= self.peer_sequence);
                const current = try primary.get("artifact-phase-peer");
                try std.testing.expectEqual(@as(usize, 8192), current.len);
                try std.testing.expectEqual(@as(u64, 127), std.mem.readInt(u64, current[0..8], .little));
            }
            // Index-catalog access uses the same fresh publication fence.
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
    if (probe.failure) |err| return err;
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

test "capi native SQL waits for replay capacity before autocommit or session COMMIT" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const Probe = ConstraintOverlayProbe;
    const sessions = @import("sql_session.zig");
    const Capacity = struct {
        fn limit(fixture: *AdmissionFollowupFixture, full: bool) !void {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const root = try fixture.connection.owner.refresh();
            const file = &root.owned_lite_backend.?.native_docstore.?.file;
            var state = (try file.replayAdmission()).?;
            try std.testing.expect(state.records != 0);
            state.limits.records = if (full) state.records else 4096;
            try file.putDocument(native.replay_admission.key, &state.encode());
        }
    };
    for ([_]bool{ false, true }) |session_commit| {
        var fixture = try AdmissionFollowupFixture.init("sql-replay-capacity", 2000, .sql);
        defer fixture.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"seedtext\",\"type\":\"full_text\"}")));
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.sql));
        var session: u64 = 0;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(fixture.id, &session));
        defer _ = sessions.antfly_db_sql_session_close(fixture.id, session);
        if (session_commit) {
            _ = try Probe.run(fixture.id, session, "BEGIN", null);
            _ = try Probe.run(fixture.id, session, "UPDATE numbers SET n=n+1 WHERE _id='accepted'", null);
        }
        try Capacity.limit(&fixture, true);
        const polls = api.test_replay_capacity_polls.load(.monotonic);
        try std.testing.expectEqual(@as(?i64, 7), try Probe.run(fixture.id, null, "SELECT n FROM numbers WHERE _id='accepted'", null));
        // Empty transaction control and reads must never wait behind producers.
        if (!session_commit) {
            _ = try Probe.run(fixture.id, session, "BEGIN", null);
            _ = try Probe.run(fixture.id, session, "COMMIT", null);
        }
        try std.testing.expectEqual(polls, api.test_replay_capacity_polls.load(.monotonic));
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
                const deadline = std.Io.Clock.awake.now(io).nanoseconds + 3 * std.time.ns_per_s;
                while (api.test_replay_capacity_polls.load(.monotonic) == self.polls) {
                    if (std.Io.Clock.awake.now(io).nanoseconds >= deadline) return error.CapacityWaitDidNotStart;
                    try io.sleep(.fromMilliseconds(1), .awake);
                }
                // This must acquire the same connection and writer gates that
                // the waiting SQL call released, including for a live session.
                try Capacity.limit(self.fixture, false);
            }
        };
        var consumer = Consumer{ .fixture = &fixture, .polls = polls };
        const worker = try std.Thread.spawn(.{}, Consumer.run, .{&consumer});
        var joined = false;
        defer if (!joined) worker.join();
        const result = Probe.run(fixture.id, if (session_commit) session else null, if (session_commit) "COMMIT" else "UPDATE numbers SET n=n+1 WHERE _id='accepted'", null);
        worker.join();
        joined = true;
        try std.testing.expectEqual(@as(?anyerror, null), consumer.failure);
        _ = try result;
        try std.testing.expect(api.test_replay_capacity_polls.load(.monotonic) > polls);
        try std.testing.expectEqual(@as(?i64, 8), try Probe.run(fixture.id, null, "SELECT n FROM numbers WHERE _id='accepted'", null));
        if (session_commit) {
            _ = try Probe.run(fixture.id, session, "BEGIN", null);
            try Capacity.limit(&fixture, true);
            const before_stage = api.test_replay_capacity_polls.load(.monotonic);
            _ = try Probe.run(fixture.id, session, "UPDATE numbers SET n=n+1 WHERE _id='accepted'", null);
            try std.testing.expectEqual(before_stage, api.test_replay_capacity_polls.load(.monotonic));
            const timeout_ms = fixture.connection.options.busy_timeout_ms;
            fixture.connection.options.busy_timeout_ms = 20;
            defer fixture.connection.options.busy_timeout_ms = timeout_ms;
            const request = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = "COMMIT", .session_id = session }, .{});
            defer std.testing.allocator.free(request);
            var out: h.capi.Buffer = .{};
            defer api.antfly_buffer_free(&out);
            try std.testing.expectEqual(h.capi.ErrorCode.busy, api.antfly_db_sql_json(fixture.id, .fromSlice(request), &out));
            var receipt = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, out.ptr.?[0..out.len], .{});
            defer receipt.deinit();
            try std.testing.expectEqualStrings("not_applied", receipt.value.object.get("commit_outcome").?.string);
            try std.testing.expectEqualStrings("55P03", receipt.value.object.get("error").?.object.get("code").?.string);
            try std.testing.expect(fixture.connection.sql_sessions.get(session).?.active);
            try std.testing.expect(!fixture.connection.sql_sessions.get(session).?.failed);
            try std.testing.expectEqual(@as(?i64, 8), try Probe.run(fixture.id, null, "SELECT n FROM numbers WHERE _id='accepted'", null));
            try Capacity.limit(&fixture, false);
            fixture.connection.options.busy_timeout_ms = timeout_ms;
            _ = try Probe.run(fixture.id, session, "COMMIT", null);
            try std.testing.expectEqual(@as(?i64, 9), try Probe.run(fixture.id, null, "SELECT n FROM numbers WHERE _id='accepted'", null));
        }
    }
}

test "capi native SQL COMMIT waits for its exact footprint after opportunity admission" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const Probe = ConstraintOverlayProbe;
    const sessions = @import("sql_session.zig");
    for ([_]bool{ false, true }) |session_commit| for ([_]bool{ false, true }) |replace_table| {
        var fixture = try AdmissionFollowupFixture.init("sql-exact-replay-capacity", 5000, .sql);
        defer fixture.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.sql));
        var session: u64 = 0;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(fixture.id, &session));
        defer _ = sessions.antfly_db_sql_session_close(fixture.id, session);
        if (session_commit) {
            _ = try Probe.run(fixture.id, session, "BEGIN", null);
            _ = try Probe.run(fixture.id, session, "UPDATE numbers SET n=n+1 WHERE _id='accepted'", null);
        }
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const file = &fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
            var state = (try file.replayAdmission()).?;
            try std.testing.expect(state.records != 0);
            state.limits.bytes = state.bytes + 1;
            try file.putDocument(native.replay_admission.key, &state.encode());
            // This is exactly the boundary the original importer reached: some
            // capacity exists, but the staged transaction cannot fit in it.
            try file.requireReplayCoordinatorCapacity();
        }
        const Consumer = struct {
            fixture: *AdmissionFollowupFixture,
            polls: u64,
            failure: ?anyerror = null,
            connection_retained: bool = false,
            session_commit: bool,
            replace_table: bool,
            fn run(self: *@This()) void {
                self.releaseCapacity() catch |err| {
                    self.failure = err;
                };
            }
            fn releaseCapacity(self: *@This()) !void {
                const io = h.handleLockIo();
                const deadline = std.Io.Clock.awake.now(io).nanoseconds + 6 * std.time.ns_per_s;
                while (api.test_replay_capacity_polls.load(.monotonic) == self.polls) {
                    if (std.Io.Clock.awake.now(io).nanoseconds >= deadline) return error.CapacityWaitDidNotStart;
                    try io.sleep(.fromMilliseconds(1), .awake);
                }
                // A same-connection session close or statement must not enter.
                const unlocked = self.fixture.connection.mutex.tryLock();
                if (unlocked) self.fixture.connection.mutex.unlock(io);
                if (unlocked == self.session_commit) return error.IncorrectSessionLaneOwnership;
                self.connection_retained = !unlocked;
                // The maintenance owner does not need that connection lane. It
                // must be able to acquire writer admission and return credits.
                var lease = try self.fixture.connection.acquire();
                defer self.fixture.connection.gate.mutex.unlock(io);
                defer lease.close();
                const root = try self.fixture.connection.owner.refresh();
                const database = try @import("tables.zig").get(root, "numbers");
                const raw = (try database.get(std.testing.allocator, "accepted")).?;
                defer std.testing.allocator.free(raw);
                var row = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, raw, .{});
                defer row.deinit();
                try std.testing.expectEqual(@as(i64, 7), row.value.object.get("n").?.integer);
                if (self.replace_table) {
                    var output: h.capi.Buffer = .{};
                    defer api.antfly_buffer_free_zero(&output);
                    try api.executeEmbeddedSql(root, "default", "{\"statement\":\"DROP TABLE numbers\"}", &output);
                    api.antfly_buffer_free_zero(&output);
                    try api.executeEmbeddedSql(root, "default", "{\"statement\":\"CREATE TABLE numbers (n BIGINT)\"}", &output);
                }
                const file = &root.owned_lite_backend.?.native_docstore.?.file;
                var state = (try file.replayAdmission()).?;
                state.limits.bytes = 16 * 1024 * 1024;
                try file.putDocument(native.replay_admission.key, &state.encode());
            }
        };
        const polls = api.test_replay_capacity_polls.load(.monotonic);
        var consumer = Consumer{ .fixture = &fixture, .polls = polls, .session_commit = session_commit, .replace_table = replace_table };
        const worker = try std.Thread.spawn(.{}, Consumer.run, .{&consumer});
        var joined = false;
        defer if (!joined) worker.join();
        const result = Probe.run(fixture.id, if (session_commit) session else null, if (session_commit) "COMMIT" else "UPDATE numbers SET n=n+1 WHERE _id='accepted'", if (replace_table) "40001" else null);
        worker.join();
        joined = true;
        try std.testing.expectEqual(@as(?anyerror, null), consumer.failure);
        try std.testing.expectEqual(session_commit, consumer.connection_retained);
        _ = try result;
        try std.testing.expect(api.test_replay_capacity_polls.load(.monotonic) > polls);
        if (replace_table) {
            try std.testing.expectEqual(@as(?i64, null), try Probe.run(fixture.id, null, "SELECT n FROM numbers WHERE _id='accepted'", null));
            if (session_commit) _ = try Probe.run(fixture.id, session, "ROLLBACK", null);
            continue;
        }
        try std.testing.expectEqual(@as(?i64, 8), try Probe.run(fixture.id, null, "SELECT n FROM numbers WHERE _id='accepted'", null));
        // A second exact-footprint refusal with no consumer must exhaust the
        // same bounded wait budget without applying or reevaluating the row.
        if (session_commit) {
            _ = try Probe.run(fixture.id, session, "BEGIN", null);
            _ = try Probe.run(fixture.id, session, "UPDATE numbers SET n=n+1 WHERE _id='accepted'", null);
        }
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const file = &fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
            var state = (try file.replayAdmission()).?;
            state.limits.bytes = state.bytes + 1;
            try file.putDocument(native.replay_admission.key, &state.encode());
            try file.requireReplayCoordinatorCapacity();
        }
        fixture.connection.options.busy_timeout_ms = 20;
        _ = try Probe.run(fixture.id, if (session_commit) session else null, if (session_commit) "COMMIT" else "UPDATE numbers SET n=n+1 WHERE _id='accepted'", "55P03");
        fixture.connection.options.busy_timeout_ms = 5000;
        try std.testing.expectEqual(@as(?i64, 8), try Probe.run(fixture.id, null, "SELECT n FROM numbers WHERE _id='accepted'", null));
        if (session_commit) {
            try std.testing.expect(fixture.connection.sql_sessions.get(session).?.active);
            try std.testing.expect(!fixture.connection.sql_sessions.get(session).?.failed);
        }
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const file = &fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
            var state = (try file.replayAdmission()).?;
            state.limits.bytes = 16 * 1024 * 1024;
            try file.putDocument(native.replay_admission.key, &state.encode());
        }
        // Retry the same COMMIT, with no restaging or SQL reevaluation. The
        // staged increment must become visible once, after credits return.
        _ = try Probe.run(fixture.id, if (session_commit) session else null, if (session_commit) "COMMIT" else "UPDATE numbers SET n=n+1 WHERE _id='accepted'", null);
        try std.testing.expectEqual(@as(?i64, 9), try Probe.run(fixture.id, null, "SELECT n FROM numbers WHERE _id='accepted'", null));
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const file = &fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
            var state = (try file.replayAdmission()).?;
            state.limits.bytes = state.bytes + 1;
            try file.putDocument(native.replay_admission.key, &state.encode());
        }
        // Cancellation may clean up under the owner gate, but must not enter
        // mutation admission or start another allocator proof after sleep.
        if (session_commit) {
            _ = try Probe.run(fixture.id, session, "BEGIN", null);
            _ = try Probe.run(fixture.id, session, "UPDATE numbers SET n=n+1 WHERE _id='accepted'", null);
        }
        const Cancellation = struct {
            owner: *Owner,
            calls: usize = 0,
            admissions: u64 = 0,
            fn sleep(ptr: *anyopaque, _: std.Io.Duration) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.calls += 1;
                self.owner.gate.mutex.lockUncancelable(h.handleLockIo());
                defer self.owner.gate.mutex.unlock(h.handleLockIo());
                self.admissions = self.owner.publication_stats.mutation_admission_count;
                return error.Canceled;
            }
        };
        var cancellation: Cancellation = .{ .owner = fixture.connection.owner };
        api.test_replay_capacity_sleep = .{ .ptr = &cancellation, .sleep = Cancellation.sleep };
        defer api.test_replay_capacity_sleep = null;
        const canceled = Probe.run(fixture.id, if (session_commit) session else null, if (session_commit) "COMMIT" else "UPDATE numbers SET n=n+1 WHERE _id='accepted'", "57014");
        api.test_replay_capacity_sleep = null;
        _ = try canceled;
        try std.testing.expectEqual(@as(usize, 1), cancellation.calls);
        try std.testing.expectEqual(cancellation.admissions, fixture.connection.owner.publication_stats.mutation_admission_count);
        try std.testing.expectEqual(@as(?i64, 9), try Probe.run(fixture.id, null, "SELECT n FROM numbers WHERE _id='accepted'", null));
        if (session_commit) _ = try Probe.run(fixture.id, session, "ROLLBACK", null);
        // Both timeout and cancellation return the session serialization lane.
        _ = try Probe.run(fixture.id, session, "BEGIN", null);
        _ = try Probe.run(fixture.id, session, "ROLLBACK", null);
    };
}

test "capi native SQL capacity wait lets the replay owner return actual credits" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const Probe = ConstraintOverlayProbe;
    const sessions = @import("sql_session.zig");
    for ([_]bool{ false, true }) |session_commit| {
        var fixture = try AdmissionFollowupFixture.init("sql-replay-actual-credits", 10000, .sql);
        defer fixture.deinit();
        const peer = try open(fixture.path, .{ .profile = .native, .no_sync = true, .busy_timeout_ms = 10000 }, false);
        const peer_id = try api.publishHandle(peer);
        defer api.antfly_db_close(peer_id);
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const root = try fixture.connection.owner.refresh();
            const database = try @import("tables.zig").get(root, "numbers");
            if (database.core.index_manager.text_indexes.items.len == 0)
                try database.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
        }
        @import("sql_native_commit.zig").test_force_async_text = true;
        const seed = fixture.call(.sql);
        @import("sql_native_commit.zig").test_force_async_text = false;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, seed);
        var session: u64 = 0;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(fixture.id, &session));
        defer _ = sessions.antfly_db_sql_session_close(fixture.id, session);
        if (session_commit) {
            _ = try Probe.run(fixture.id, session, "BEGIN", null);
            _ = try Probe.run(fixture.id, session, "UPDATE numbers SET n=n+1 WHERE _id='accepted'", null);
        }
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const root = try fixture.connection.owner.refresh();
            const database = try @import("tables.zig").get(root, "numbers");
            try std.testing.expect(try Owner.hasPendingReplay(database));
            const file = &root.owned_lite_backend.?.native_docstore.?.file;
            var state = (try file.replayAdmission()).?;
            try std.testing.expect(state.records != 0);
            state.limits.bytes = state.bytes + 1;
            try file.putDocument(native.replay_admission.key, &state.encode());
            try file.requireReplayCoordinatorCapacity();
        }
        const Consumer = struct {
            id: ?*anyopaque,
            polls: u64,
            result: h.capi.ErrorCode = .invalid_argument,
            timed_out: bool = false,
            fn run(self: *@This()) void {
                const io = h.handleLockIo();
                const deadline = std.Io.Clock.awake.now(io).nanoseconds + 12 * std.time.ns_per_s;
                while (api.test_replay_capacity_polls.load(.monotonic) == self.polls) {
                    if (std.Io.Clock.awake.now(io).nanoseconds >= deadline) {
                        self.timed_out = true;
                        return;
                    }
                    io.sleep(.fromMilliseconds(1), .awake) catch return;
                }
                // Use the real replay/retirement service through another
                // connection. Never manufacture credits or raise the quota.
                self.result = api.antfly_db_run_until_idle(self.id);
            }
        };
        const polls = api.test_replay_capacity_polls.load(.monotonic);
        var consumer: Consumer = .{ .id = peer_id, .polls = polls };
        const worker = try std.Thread.spawn(.{}, Consumer.run, .{&consumer});
        var joined = false;
        defer if (!joined) worker.join();
        const committed = Probe.run(fixture.id, if (session_commit) session else null, if (session_commit) "COMMIT" else "UPDATE numbers SET n=n+1 WHERE _id='accepted'", null);
        worker.join();
        joined = true;
        try std.testing.expect(!consumer.timed_out);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, consumer.result);
        _ = try committed;
        try std.testing.expect(api.test_replay_capacity_polls.load(.monotonic) > polls);
        try std.testing.expectEqual(@as(?i64, 8), try Probe.run(fixture.id, null, "SELECT n FROM numbers WHERE _id='accepted'", null));
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        try std.testing.expect((try fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file.check()).valid);
    }
}

test "capi native replay admission preserves oversized record input ownership" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var fixture = try AdmissionFollowupFixture.init("replay-oversized-inputs", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"search\",\"type\":\"full_text\"}")));
    // A record exceeds the initial 64-family budget. Each fragment keeps
    // its own source/callback borrows alive, then durably checkpoints progress
    // before another turn can release and reconstruct the source snapshot.
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
    const cached = owner.gate.writer_allocator_cache.holder.?.ledger.?;
    const loads = first_file.allocatorWorkSnapshot().full_loads;
    try std.testing.expect(cached.digest_cache != null);
    _ = try owner.refresh();
    const foreground_file = &owner.current.?.owned_lite_backend.?.native_docstore.?.file;
    foreground_file.incremental_checkpoints = true;
    try foreground_file.beginTransaction();
    try foreground_file.putDocument("cache-transfer-doc", "two");
    try foreground_file.commitTransaction();
    const replacement = try owner.refreshMaintenanceSnapshot(owner.current.?, .storage);
    const adopted = &replacement.owned_lite_backend.?.native_docstore.?.file;
    try std.testing.expectEqual(cached, owner.gate.writer_allocator_cache.holder.?.ledger.?);
    try std.testing.expect(adopted.free_pages_verified);
    try std.testing.expectEqual(loads, adopted.allocatorWorkSnapshot().full_loads);
    try adopted.putCatalogRecord("cache-transfer-after", "three");
    try std.testing.expectEqual(loads, adopted.allocatorWorkSnapshot().full_loads);
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
    try std.testing.expectEqual(@as(u64, 0), owner.publication_stats.maintenance_database_turns - turns);
    // The audit can also visit the already-serviced root; every unrelated
    // indexed namespace must remain idle without opening its runtime.
    try std.testing.expect(owner.publication_stats.maintenance_idle_namespace_turns - idle_turns >= 8);
    try std.testing.expectEqual(@as(u64, 8), owner.publication_stats.maintenance_readiness_skips - readiness_skips);
    // Already loaded foreground namespaces may remain cached; maintenance
    // must not construct a fresh detached runtime for any idle namespace.
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

test "capi native covered text source observes writes without reopening source and fences changed controls" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("covered-text-source", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    const key: Owner.MaintenanceKey = .{ .table_id = owner.current.?.db.core.identity_namespace.table_id, .incarnation = owner.current.?.db.root_incarnation };
    var warmed = false;
    for (0..64) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.maintenance_schedules.getPtr(key)) |schedule| {
            if (schedule.covered_text_controls != null and !schedule.database_pending and !schedule.startup_replay_pending and !owner.maintenance_storage_pending) {
                warmed = true;
                break;
            }
        }
    }
    if (!warmed) return error.CoveredSourceWarmupMissing;
    // Keep this test deterministic: service clocks, rather than host speed,
    // decide whether optional physical work is due during observation.
    const far_future = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + std.time.ns_per_hour;
    owner.maintenance_schedules.getPtr(key).?.next_database_audit_ns = far_future;
    owner.maintenance_schedules.getPtr(key).?.artifact_pending = true;
    owner.maintenance_schedules.getPtr(key).?.next_artifact_ns = far_future;
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice("{\"inserts\":{\"covered\":{\"text\":\"atomic native publication\"}},\"sync_level\":\"full_index\"}"), &output));
    const target = owner.current.?.db.core.nextDerivedSequence();
    const before = owner.publication_stats.maintenance_source_turns;
    for (0..8) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.maintenance_schedules.getPtr(key).?.serviced_sequence == target) break;
    }
    try std.testing.expectEqual(target, owner.maintenance_schedules.getPtr(key).?.serviced_sequence);
    try std.testing.expectEqual(before, owner.publication_stats.maintenance_source_turns);
    // Observing semantic completion must not refund physically retained bytes.
    const store = owner.current.?.owned_lite_backend.?.native_docstore.?;
    if ((try store.file.replayAdmission()).?.records == 0) return error.CoveredObservationReleasedPhysicalCredits;
    const opened = owner.test_runtime_snapshot_opens;
    const artifacts = owner.publication_stats.maintenance_artifact_only_turns;
    owner.maintenance_schedules.getPtr(key).?.next_artifact_ns = 0;
    owner.maintenance_schedules.getPtr(key).?.group_sources.?.replay_retirement.batch.first_pending_ns = 0;
    for (0..8) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.publication_stats.maintenance_artifact_only_turns != artifacts) break;
    }
    if (owner.publication_stats.maintenance_artifact_only_turns <= artifacts) return error.NativePhysicalSliceNotServiced;
    try std.testing.expectEqual(opened, owner.test_runtime_snapshot_opens);
    try std.testing.expectEqual(before, owner.publication_stats.maintenance_source_turns);
    // A real lagging consumer still needs detached source service even when
    // every schema/control fingerprint is unchanged.
    try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "replay-only", .value = "{\"text\":\"lagging consumer must run\"}" }}, .sync_level = .write });
    const replay_target = owner.current.?.db.core.nextDerivedSequence();
    for (0..32) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.publication_stats.maintenance_source_turns > before) break;
    }
    if (owner.publication_stats.maintenance_source_turns <= before) return error.LaggingConsumerNotServiced;
    {
        var lease = try fixture.connection.acquire();
        defer lease.close();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        const root = try owner.refresh();
        try std.testing.expect(try root.db.core.loadAppliedSequence(alloc, "text") >= replay_target);
        const readiness = try Owner.namespaceReadiness(root, "root");
        const prior = (try Owner.coveredTextControls(root, &root.db, "root", readiness)).?;
        const control_keys = try root.db.core.index_manager.retainedTextRuntimeControlKeysAlloc(std.testing.allocator);
        defer {
            for (control_keys) |control| std.testing.allocator.free(control);
            std.testing.allocator.free(control_keys);
        }
        // The admission key is a data-plane control, outside the fixed schema
        // directory. Its presence must disqualify an otherwise covered source.
        {
            var write = try root.owned_lite_backend.?.native_docstore.?.beginWrite();
            errdefer write.abort();
            try write.put(control_keys[control_keys.len - 1], "pending");
            try write.commit();
        }
        try std.testing.expect((try Owner.coveredTextControls(root, &root.db, "root", try Owner.namespaceReadiness(root, "root"))) == null);
        _ = prior;
    }
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
    const observations = owner.publication_stats.covered_source_observations;
    for (0..16) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.maintenance_schedules.getPtr(key).?.serviced_sequence == target) break;
    }
    // An unchanged consumerless layout certifies the new tip without a
    // detached source turn; physical retirement still has to make progress.
    try std.testing.expectEqual(before, owner.publication_stats.maintenance_database_turns);
    try std.testing.expect(owner.publication_stats.covered_source_observations > observations);
    try std.testing.expectEqual(target, owner.maintenance_schedules.getPtr(key).?.serviced_sequence);
    const source_turns = owner.publication_stats.maintenance_source_turns;
    // Source progress schedules a bounded physical-retirement continuation;
    // it need not delete each journal record in the source publication turn.
    // Advance that continuation's clock without enabling the distant audit.
    {
        owner.gate.mutex.lockUncancelable(h.handleLockIo());
        defer owner.gate.mutex.unlock(h.handleLockIo());
        const state = try owner.groupSourceStateNamed(&owner.current.?.db, "default");
        state.replay_retirement.batch.first_pending_ns = 0;
        const pending = owner.maintenance_schedules.getPtr(key).?;
        try std.testing.expect(pending.artifact_pending);
        pending.next_artifact_ns = 0;
    }
    for (0..16) |_| {
        _ = try owner.maintenanceTurnForTest();
        const state = owner.maintenance_schedules.getPtr(key).?.group_sources.?;
        if (state.replay_retirement.sweep_bound >= target) break;
    }
    try std.testing.expectEqual(source_turns, owner.publication_stats.maintenance_source_turns);
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
    if (debt.records != 0) {
        const state = try owner.groupSourceStateNamed(&owner.current.?.db, "default");
        std.debug.print("unindexed retirement target={d} state={any} errors={d} failure={s}\n", .{ target, state.replay_retirement, owner.publication_stats.maintenance_errors, owner.publication_stats.last_maintenance_failure orelse "none" });
    }
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
    const opened = owner.test_runtime_snapshot_opens;
    _ = try owner.maintenanceTurnForTest();
    try std.testing.expect(!probe.entered);
    try std.testing.expectEqual(opened, owner.test_runtime_snapshot_opens);
    try std.testing.expect(!probe.has_source_epoch);
    try std.testing.expectEqual(sources, owner.publication_stats.maintenance_source_turns);
    try std.testing.expectEqual(artifacts + 1, owner.publication_stats.maintenance_artifact_only_turns);
    try std.testing.expect(!owner.maintenance_schedules.getPtr(key).?.database_pending);
}

test "capi native maintenance namespace working set reuses and evicts bounded inputs" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("namespace-runtime-working-set", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const foreground = try owner.refresh();
    for (0..TextSourceRuntimeCache.max_namespaces + 1) |i| {
        var buffer: [32]u8 = undefined;
        const name = try std.fmt.bufPrint(&buffer, "working-{d}", .{i});
        try @import("tables.zig").create(foreground, name, "{}", false);
        const database = try @import("tables.zig").get(foreground, name);
        try database.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    }
    const root = try owner.refreshMaintenanceSnapshot(foreground, .source);
    const opens = owner.publication_stats.maintenance_namespace_opens;
    var first: ?*h.db_mod.DB = null;
    for (0..2) |pass| {
        for (0..TextSourceRuntimeCache.max_namespaces) |i| {
            var buffer: [32]u8 = undefined;
            const name = try std.fmt.bufPrint(&buffer, "working-{d}", .{i});
            const database = try owner.openMaintenanceNamespace(root, name);
            if (i == 0) {
                if (pass == 0) first = database else try std.testing.expectEqual(first.?, database);
            }
        }
    }
    try std.testing.expectEqual(opens + TextSourceRuntimeCache.max_namespaces, owner.publication_stats.maintenance_namespace_opens);
    try std.testing.expectEqual(TextSourceRuntimeCache.max_namespaces, root.embedded_tables.count());
    // Reading the working set never alters the complete durable name catalog.
    try std.testing.expectEqual(TextSourceRuntimeCache.max_namespaces + 1, root.embedded_table_records.count());
    owner.text_source_runtime_cache = (try TextSourceRuntimeCache.capture(root)) orelse return error.TestUnexpectedResult;
    // Opening immutable readers publishes lease metadata. Production admission
    // refreshes the serving checkpoint before each capture; do the same here
    // instead of rebinding to the foreground's now unretained old page roots.
    try std.testing.expectEqual(root, try owner.refreshMaintenanceSnapshot(try owner.refresh(), .source));

    // Protect the first resident with a recent selection; the second is now
    // the oldest. Eviction drops only that DB and its artifact readers.
    _ = try owner.openMaintenanceNamespace(root, "working-0");
    var overflow_name: [32]u8 = undefined;
    _ = try owner.openMaintenanceNamespace(root, try std.fmt.bufPrint(&overflow_name, "working-{d}", .{TextSourceRuntimeCache.max_namespaces}));
    try std.testing.expectEqual(first.?, &root.embedded_tables.get("working-0").?.db);
    try std.testing.expect(!root.embedded_tables.contains("working-1"));
    try std.testing.expectEqual(TextSourceRuntimeCache.max_namespaces, root.embedded_tables.count());
    try std.testing.expectEqual(TextSourceRuntimeCache.max_namespaces + 1, root.embedded_table_records.count());
    try std.testing.expect(owner.text_source_runtime_cache == null);
    // Recapture covers every resident's catalog and mutable control witnesses.
    owner.text_source_runtime_cache = (try TextSourceRuntimeCache.capture(root)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(root, try owner.refreshMaintenanceSnapshot(try owner.refresh(), .source));
}

test "capi native maintenance keeps settled text inputs warm across artifact turns" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("settled-text-runtime-reuse", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    const owner = fixture.connection.owner;
    const key: Owner.MaintenanceKey = .{ .table_id = owner.current.?.db.core.identity_namespace.table_id, .incarnation = owner.current.?.db.root_incarnation };
    var settled = false;
    for (0..64) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.maintenance_schedules.getPtr(key)) |schedule| {
            if (!schedule.database_pending and schedule.next_database_audit_ns != 0 and !owner.maintenance_storage_pending) {
                settled = true;
                break;
            }
        }
    }
    try std.testing.expect(settled);
    const cache = owner.text_source_runtime_cache orelse return error.TestUnexpectedResult;
    const root = cache.root;
    const opens = owner.publication_stats.maintenance_runtime_opens;
    const schedule = owner.maintenance_schedules.getPtr(key).?;
    schedule.next_database_audit_ns = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + std.time.ns_per_hour;
    schedule.artifact_pending = true;
    schedule.next_artifact_ns = 0;
    const source_turns = owner.publication_stats.maintenance_source_turns;
    _ = try owner.maintenanceTurnForTest();
    try std.testing.expectEqual(source_turns, owner.publication_stats.maintenance_source_turns);
    try std.testing.expectEqual(root, (owner.text_source_runtime_cache orelse return error.TestUnexpectedResult).root);
    try std.testing.expectEqual(opens, owner.publication_stats.maintenance_runtime_opens);

    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "next", .value = "{\"text\":\"warm café 東京\"}" }}, .sync_level = .write });
    const reuses = owner.publication_stats.maintenance_source_runtime_reuses;
    for (0..16) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.publication_stats.maintenance_source_runtime_reuses != reuses) break;
    }
    try std.testing.expect(owner.publication_stats.maintenance_source_runtime_reuses > reuses);
    try std.testing.expectEqual(opens, owner.publication_stats.maintenance_runtime_opens);

    // A settled reader must not retain an old generation's artifact leases
    // until the next source write. Even a storage/idle turn evicts it.
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const live = try owner.refresh();
        const current_cache = owner.text_source_runtime_cache orelse return error.TestUnexpectedResult;
        try std.testing.expect(try current_cache.matches(&live.owned_lite_backend.?.native_docstore.?.file));
        try live.owned_lite_backend.?.native_docstore.?.file.putIndexCatalogRecord("/outside/settled-runtime", "changed");
        try std.testing.expect(!try current_cache.matches(&live.owned_lite_backend.?.native_docstore.?.file));
    }
    _ = try owner.maintenanceTurnForTest();
    try std.testing.expect(owner.text_source_runtime_cache == null);
}

test "capi native maintenance retires uncertain cached readers without clearing the owner fence" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    inline for (.{ true, false }) |has_witness| {
        inline for (.{ "sql", "checkpoint", "secret" }) |kind| {
            var fixture = try AdmissionFollowupFixture.init("uncertain-cache-" ++ kind ++ (if (has_witness) "-witness" else "-without-witness"), 5000, .batch);
            defer fixture.deinit();
            try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
            try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
            const owner = fixture.connection.owner;
            const key: Owner.MaintenanceKey = .{ .table_id = owner.current.?.db.core.identity_namespace.table_id, .incarnation = owner.current.?.db.root_incarnation };
            for (0..64) |_| {
                _ = try owner.maintenanceTurnForTest();
                if (owner.maintenance_schedules.getPtr(key)) |schedule| {
                    if (!schedule.database_pending and schedule.next_database_audit_ns != 0 and !owner.maintenance_storage_pending) break;
                }
            }
            const source = (owner.text_source_runtime_cache orelse return error.TestUnexpectedResult).root;
            if (!has_witness) owner.clearTextSourceRuntimeCache();
            try std.testing.expect(!Owner.needsOutcomeReconciliation(owner.current.?));
            const peer_turn = (try owner.acquireMaintenanceTurnGuard()) orelse return error.TestUnexpectedResult;
            defer peer_turn.close(h.handleLockIo());
            if (comptime std.mem.eql(u8, kind, "sql")) source.sql_decision_uncertain = true else if (comptime std.mem.eql(u8, kind, "checkpoint")) source.owned_lite_backend.?.native_docstore.?.file.checkpoint_publication_uncertain = true else source.owned_lite_backend.?.native_docstore.?.secret_store_uncertain = true;
            defer {
                // These flags model uncertainty without changing the fixture's
                // durable state. Clear the propagated fence only for teardown.
                owner.current.?.sql_decision_uncertain = false;
                owner.current.?.owned_lite_backend.?.native_docstore.?.secret_store_uncertain = false;
                owner.current.?.owned_lite_backend.?.native_docstore.?.file.checkpoint_publication_uncertain = false;
            }
            owner.maintenance_storage_pending = true;
            try std.testing.expectError(error.OutcomeReconciliationRequired, owner.maintenanceTurnForTest());
            try std.testing.expect(owner.text_source_runtime_cache == null);
            try std.testing.expect(owner.maintenance_snapshot == null);
            try std.testing.expect(Owner.needsOutcomeReconciliation(owner.current.?));
            try std.testing.expectEqual(@as(usize, 0), owner.maintenance_turn_references);
            owner.maintenance_storage_pending = true;
            try std.testing.expect(owner.maintenanceWaitMillisForTest(true) > 0);
            const refused = blk: {
                owner.gate.mutex.lockUncancelable(h.handleLockIo());
                defer owner.gate.mutex.unlock(h.handleLockIo());
                var lease = try native.lockWriterPathWithIo(alloc, h.handleLockIo(), fixture.path);
                defer lease.close();
                break :blk owner.refresh();
            };
            try std.testing.expectError(error.OutcomeReconciliationRequired, refused);
        }
    }
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

test "capi native maintenance named selection opens only its detached table runtime" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("single-maintenance-table-runtime", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const root = owner.current.?;
        try @import("tables.zig").create(root, "active", "{}", false);
        const table = try @import("tables.zig").get(root, "active");
        try table.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
        @import("tables.zig").closeAll(root);
    }
    const Probe = struct {
        owner: *Owner,
        entered: bool = false,
        serving_open: bool = false,
        detached_open: bool = false,
        fn captured(ptr: *anyopaque, root: *h.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!root.embedded_tables.contains("active")) return;
            self.entered = true;
            self.serving_open = self.owner.current.?.embedded_tables.contains("active");
            self.detached_open = root != self.owner.current.? and root.embedded_tables.contains("active");
        }
    };
    var probe: Probe = .{ .owner = owner };
    owner.test_after_capture = .{ .ptr = &probe, .notify = Probe.captured };
    defer owner.test_after_capture = null;
    for (0..16) |_| {
        owner.maintenance_table_cursor = 1;
        owner.maintenance_storage_pending = false;
        _ = try owner.maintenanceTurnForTest();
        if (probe.entered) break;
    }
    try std.testing.expect(probe.entered and probe.detached_open);
    try std.testing.expect(!probe.serving_open);
}

test "capi native foreground namespace refresh retains predecessor inputs only through admission" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const io = h.handleLockIo();
    const tables = @import("tables.zig");
    var fixture = try AdmissionFollowupFixture.init("foreground-namespace-input-handoff", 5000, .batch);
    defer fixture.deinit();
    const owner = fixture.connection.owner;
    const Peer = struct {
        fn advance(f: *AdmissionFollowupFixture) !void {
            var peer = try native.NativeFile.openWithIo(std.testing.allocator, h.handleLockIo(), f.path, .{ .externally_locked = true, .no_sync = true, .reader_coordinator = f.connection.owner.reader_coordinator });
            defer peer.close();
            try peer.inheritAllocatorVerification(&f.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file);
            var cut: [8]u8 = undefined;
            std.mem.writeInt(u64, &cut, peer.activeCheckpoint().commit_sequence, .little);
            // A real foreign metadata publication requires reconstruction, but
            // leaves every named-table artifact's immutable value unchanged.
            try peer.putCatalogRecord("test/foreground-admission-cut", &cut);
        }
    };
    var source_ptr: *anyopaque = undefined;
    var segment_id: u64 = undefined;
    var old_address: usize = undefined;
    var opens_before: u64 = undefined;
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(io);
        }
        const root = try owner.refresh();
        try tables.create(root, "active", "{}", false);
        const table = try tables.get(root, "active");
        try table.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
        try table.batch(.{ .writes = &.{.{ .key = "one", .value = "{\"text\":\"first canary\"}" }}, .sync_level = .full_text });
        try table.batch(.{ .writes = &.{.{ .key = "two", .value = "{\"text\":\"second canary\"}" }}, .sync_level = .full_text });
        {
            const inventory = table.core.index_manager.textIndex("text").?.acquireSnapshot();
            defer inventory.release();
            try std.testing.expectEqual(@as(usize, 2), inventory.segments.len);
        }
        old_address = @intFromPtr(root);
        const before_unbridged = root.owned_lite_backend.?.native_docstore.?.file.allocator_work.artifact_source_opens;
        try Peer.advance(&fixture);
        // No extra snapshot or read pin keeps the predecessor alive here.
        // Establish the old lifecycle's negative control using this candidate,
        // without building or running a performance-reference binary.
        const unbridged = try owner.refresh();
        try std.testing.expect(@intFromPtr(unbridged) != old_address);
        const cold_table = try tables.get(unbridged, "active");
        // Source opens need not map one-to-one to logical segments. Require
        // observable cold-open work, then require none through C API admission.
        try std.testing.expect(unbridged.owned_lite_backend.?.native_docstore.?.file.allocator_work.artifact_source_opens > before_unbridged);
        {
            const inventory = cold_table.core.index_manager.textIndex("text").?.acquireSnapshot();
            defer inventory.release();
            const source = inventory.segments[0].reader.immutableSource();
            try std.testing.expect(source == .ranges and source.ranges.retention_artifact_only);
            source_ptr = source.ranges.ptr;
            segment_id = inventory.segments[0].id;
        }
        old_address = @intFromPtr(unbridged);
        try Peer.advance(&fixture);
        opens_before = unbridged.owned_lite_backend.?.native_docstore.?.file.allocator_work.artifact_source_opens;
    }
    var child: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(fixture.id, .fromSlice("active"), &child));
    defer api.antfly_db_close(child);
    try std.testing.expect(@intFromPtr(owner.current.?) != old_address);
    try std.testing.expectEqual(@as(usize, 0), owner.current.?.refresh_references);
    try std.testing.expectEqual(@as(usize, 0), owner.retired.items.len);
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(io);
        }
        const table = try tables.get(owner.current.?, "active");
        const inventory = table.core.index_manager.textIndex("text").?.acquireSnapshot();
        defer inventory.release();
        const unchanged = for (inventory.segments) |*entry| {
            if (entry.id == segment_id) break entry.reader.immutableSource();
        } else return error.TestUnexpectedResult;
        try std.testing.expectEqual(source_ptr, unchanged.ranges.ptr);
        try std.testing.expectEqual(opens_before, owner.current.?.owned_lite_backend.?.native_docstore.?.file.allocator_work.artifact_source_opens);
        try std.testing.expect((try owner.current.?.owned_lite_backend.?.native_docstore.?.file.check()).valid);
        try Peer.advance(&fixture);
    }
    var missing: ?*anyopaque = null;
    try std.testing.expect(tables.antfly_db_open_table(fixture.id, .fromSlice("missing"), &missing) != .ok);
    try std.testing.expect(missing == null);
    try std.testing.expectEqual(@as(usize, 0), owner.current.?.refresh_references);
    try std.testing.expectEqual(@as(usize, 0), owner.retired.items.len);
    h.capi.clearLastError();
}

test "capi native decoded artifact lease retains physical inputs and accounting after runtime close" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const source_module = @import("../segment_source.zig");
    var held: ?source_module.Decoded = null;
    defer if (held) |lease| lease.release();
    var backing: source_module.Source = undefined;
    var original: [8]u8 = undefined;
    var calls: usize = 0;
    const Loader = struct {
        fn decode(raw: *anyopaque, a: std.mem.Allocator) !*const anyopaque {
            const count: *usize = @ptrCast(@alignCast(raw));
            count.* += 1;
            const value = try a.create(u64);
            value.* = 0x12345678;
            return value;
        }
        fn oversized(_: *anyopaque, a: std.mem.Allocator) !*const anyopaque {
            const value = try a.create(u64);
            _ = try a.alloc(u8, 2 * 1024 * 1024);
            value.* = 0;
            return value;
        }
    };
    {
        var fixture = try AdmissionFollowupFixture.init("native-decoded-source-owner", 5000, .batch);
        defer fixture.deinit();
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const root = try fixture.connection.owner.refresh();
        try root.db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
        try root.db.batch(.{ .writes = &.{.{ .key = "one", .value = "{\"text\":\"owned immutable source\"}" }}, .sync_level = .full_text });
        const inventory = root.db.core.index_manager.textIndex("text").?.acquireSnapshot();
        defer inventory.release();
        backing = inventory.segments[0].reader.immutableSource();
        try backing.readInto(0, &original);
        {
            // An unrelated handoff use must not keep a failed optional
            // cache's metadata admission charged against the normal fallback.
            try std.testing.expect(backing.acquireUse());
            defer backing.releaseUse();
            const manager = backing.resourceManager().?;
            const before = manager.snapshot().memory.used_bytes;
            try std.testing.expect((try backing.acquireDecoded(.{ .offset = 0, .length = 8, .interpretation = @splat(92), .context = &calls, .decode = Loader.oversized })) == null);
            try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
        }
        const request: source_module.DecodeRequest = .{ .offset = 0, .length = 8, .interpretation = @splat(91), .context = &calls, .decode = Loader.decode };
        held = (try backing.acquireDecoded(request)) orelse return error.TestUnexpectedResult;
        const reused = (try backing.acquireDecoded(request)) orelse return error.TestUnexpectedResult;
        defer reused.release();
        try std.testing.expectEqual(held.?.value, reused.value);
        try std.testing.expectEqual(@as(usize, 1), calls);
    }
    // Every DB, runtime and ordinary Source reference has closed. The decode
    // lease alone keeps the exact native input and its resource owner alive.
    try std.testing.expectEqual(@as(u64, 0x12345678), @as(*const u64, @ptrCast(@alignCast(held.?.value))).*);
    var after: [8]u8 = undefined;
    try backing.readInto(0, &after);
    try std.testing.expectEqualSlices(u8, &original, &after);
    held.?.release();
    held = null;
}

test "capi native named runtime replacement hands off immutable artifact sources before closing" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("named-runtime-artifact-handoff", 5000, .batch);
    defer fixture.deinit();
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const owner = fixture.connection.owner;
    const foreground = try owner.refresh();
    const tables = @import("tables.zig");
    try tables.create(foreground, "active", "{}", false);
    const table = try tables.get(foreground, "active");
    try table.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    try table.batch(.{ .writes = &.{.{ .key = "one", .value = "{\"text\":\"immutable handoff canary\"}" }}, .sync_level = .full_text });
    // No serving runtime may keep a pooled source alive for this test. The
    // only possible donor on the second open is the previous detached table.
    tables.closeAll(foreground);
    const first = try owner.refreshMaintenanceSnapshotNamed(foreground, .source, "active");
    try std.testing.expect(first.embedded_tables.contains("active"));
    try std.testing.expect(first.owned_lite_backend.?.native_docstore.?.file.allocator_work.artifact_source_opens != 0);
    const live = try owner.refresh();
    try live.owned_lite_backend.?.native_docstore.?.file.putDocument("handoff-primary-tip", "fresh");
    const selected = try tables.get(first, "active");
    const first_inventory = selected.core.index_manager.text_indexes.items[0].persistent.acquireSnapshot();
    const original_source = first_inventory.segments[0].reader.immutableSource();
    first_inventory.release();
    try std.testing.expect(original_source == .ranges);
    var rejected: ImmutableTextHandoff.Pins = .{ .allocator = alloc };
    defer rejected.deinit();
    try std.testing.expect(!try rejected.append(selected.core.index_manager.text_indexes.items[0].persistent.acquireSnapshot(), 0, 0));
    owner.preserveImmutableTextHandoff(first, selected);
    try std.testing.expect(owner.immutable_sources.count != 0);
    try std.testing.expect(owner.immutable_sources.bytes <= 128 * 1024 * 1024);
    try std.testing.expect(owner.immutable_sources.range_bytes <= first.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().page_count *| first.owned_lite_backend.?.native_docstore.?.file.header.page_size);
    try std.testing.expect(owner.immutable_text_handoff != null);
    try std.testing.expect(owner.immutable_text_handoff.?.pins.bytes <= ImmutableTextHandoff.max_bytes);
    try std.testing.expect(owner.immutable_text_handoff.?.pins.source_bytes <= owner.immutable_text_handoff.?.generation_bytes);
    try std.testing.expect(original_source.ranges.retention_artifact_only);
    // Exercise the actual unsupported-runtime boundary: every mutable actor
    // and DB is closed before construction of its replacement starts.
    tables.closeAll(first);
    first.db.closeImmutableSnapshot();
    first.db_live = false;
    // No mutable-runtime witness is installed. This is a real replacement,
    // even though every immutable input remains identical.
    try std.testing.expect(owner.text_source_runtime_cache == null);
    const second = try owner.refreshMaintenanceSnapshotNamed(live, .source, "active");
    try std.testing.expect(second != first);
    const file = &second.owned_lite_backend.?.native_docstore.?.file;
    const replacement_table = try tables.get(second, "active");
    const replacement_inventory = replacement_table.core.index_manager.text_indexes.items[0].persistent.acquireSnapshot();
    const replacement_source = replacement_inventory.segments[0].reader.immutableSource();
    replacement_inventory.release();
    try std.testing.expect(replacement_source == .ranges);
    // Compare the backing range itself. LSM/control files may legitimately
    // open fresh sources, and allocator-work counters move between wrappers.
    try std.testing.expectEqual(original_source.ranges.ptr, replacement_source.ranges.ptr);
    try std.testing.expect(owner.immutable_text_handoff == null);
    // File-owned sources survive the single-namespace reader handoff.
    try std.testing.expect(owner.immutable_sources.count != 0);
    {
        var row = (try (try tables.get(second, "active")).lookup(std.testing.allocator, "one", .{})) orelse return error.TestUnexpectedResult;
        defer row.deinit(std.testing.allocator);
    }
    const fresh = (try file.getDocumentAlloc(std.testing.allocator, "handoff-primary-tip")) orelse return error.TestUnexpectedResult;
    defer std.testing.allocator.free(fresh);
    try std.testing.expectEqualStrings("fresh", fresh);
    try std.testing.expect((try file.check()).valid);
    owner.preserveImmutableTextHandoff(second, try tables.get(second, "active"));
    try std.testing.expect(owner.immutable_text_handoff != null);
    // The reused range belongs to the first registry, but the new decoder's
    // allocations belong to the second. Both must survive full backend close.
    owner.maintenance_snapshot = null;
    Owner.closeMaintenanceSnapshot(second);
    try std.testing.expect(owner.immutable_text_handoff.?.resource_owner.owned_resources != null);
    owner.expireImmutableTextHandoff(std.math.maxInt(u64));
    try std.testing.expect(owner.immutable_text_handoff == null);
    try std.testing.expectEqual(@as(usize, 0), owner.immutable_sources.count);
}

test "capi native maintenance relational replay diagnoses bounded runtime reuse" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    // Preserve real SQL validation and journal publication while explicitly
    // creating replay debt. Successful foreground text publication is tested
    // separately and must not silently turn this into an idle-maintenance test.
    @import("sql_native_commit.zig").test_force_async_text = true;
    defer @import("sql_native_commit.zig").test_force_async_text = false;
    const a = std.testing.allocator;
    const io = h.handleLockIo();
    inline for (.{ 1, 2 }) |namespace_count| {
        var fixture = try AdmissionFollowupFixture.init(if (namespace_count == 1) "relational-replay-one" else "relational-replay-two", 5000, .batch);
        defer fixture.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
        const owner = fixture.connection.owner;
        for (0..namespace_count) |n| {
            const request = try std.fmt.allocPrint(a, "{{\"statement\":\"CREATE TABLE replay_{d} (id BIGINT PRIMARY KEY, body TEXT, item JSONB)\"}}", .{n});
            defer a.free(request);
            var out: h.capi.Buffer = .{};
            defer api.antfly_buffer_free(&out);
            try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(fixture.id, .fromSlice(request), &out));
            var lease = try fixture.connection.acquire();
            defer {
                lease.close();
                fixture.connection.gate.mutex.unlock(io);
            }
            var name_buf: [32]u8 = undefined;
            const name = try std.fmt.bufPrint(&name_buf, "replay_{d}", .{n});
            const root = try owner.refresh();
            const table = try @import("tables.zig").get(root, name);
            if (table.core.index_manager.text_indexes.items.len == 0)
                try table.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
            _ = try owner.groupSourceStateNamed(table, name);
        }
        const StorageProbe = struct {
            connection: *Connection,
            foreign: bool,
            failure: ?anyerror = null,
            original: ?@import("../storage/lite/docstore.zig").WriterAdmission = null,
            injected: bool = false,
            adopted: bool = false,
            verify_cache: bool = false,
            checked: bool = false,
            expected_indexes: [16]u8 = undefined,
            refusals: usize = 0,

            fn notify(ptr: *anyopaque, root: *h.Handle, reusable: bool) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const store = root.owned_lite_backend.?.native_docstore.?;
                self.original = store.writer_admission;
                if (self.foreign and reusable and !self.injected) {
                    self.expected_indexes = store.file.activeCheckpoint().logical.indexes;
                    self.publishForeignIndex() catch |err| {
                        self.failure = err;
                        return;
                    };
                    self.injected = true;
                    self.verify_cache = true;
                }
                store.writer_admission = .{ .ptr = self, .enter = refuse, .leave = leave };
            }
            fn publishForeignIndex(self: *@This()) !void {
                var lease = try self.connection.acquire();
                defer {
                    lease.close();
                    self.connection.gate.mutex.unlock(h.handleLockIo());
                }
                const live = try self.connection.owner.refresh();
                try live.owned_lite_backend.?.native_docstore.?.file.putIndexCatalogRecord("/outside/replay-cache-probe", "foreign checkpoint");
            }
            fn refuse(ptr: *anyopaque, scope: PublicationScope) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.refusals += 1;
                if (self.verify_cache and !self.adopted) {
                    // Exercise allocator-service adoption, then yield before
                    // physical work. Its new header cannot certify old inputs.
                    const original = self.original orelse return error.TestUnexpectedResult;
                    try original.enter(original.ptr, scope);
                    original.leave(original.ptr);
                    self.adopted = true;
                }
                return error.EnrichmentWaitCanceled;
            }
            fn leave(_: *anyopaque) void {
                unreachable; // Every injected admission refuses before work.
            }
            fn check(self: *@This()) !void {
                if (self.failure) |err| return err;
                if (!self.verify_cache) return;
                try std.testing.expect(self.adopted);
                var lease = try self.connection.acquire();
                defer {
                    lease.close();
                    self.connection.gate.mutex.unlock(h.handleLockIo());
                }
                const cache = self.connection.owner.text_source_runtime_cache orelse return error.TestUnexpectedResult;
                try std.testing.expectEqualSlices(u8, &self.expected_indexes, &cache.indexes);
                const live = &self.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file;
                try std.testing.expect(!std.mem.eql(u8, &self.expected_indexes, &live.activeCheckpoint().logical.indexes));
                try std.testing.expect(!try cache.matches(live));
                self.verify_cache = false;
                self.checked = true;
            }
        };
        var storage_probe: StorageProbe = .{ .connection = fixture.connection, .foreign = namespace_count == 2 };
        owner.test_before_storage_service = .{ .ptr = &storage_probe, .notify = StorageProbe.notify };
        defer owner.test_before_storage_service = null;
        owner.publication_stats.text_runtime_cache = .{};
        const opens_before = owner.publication_stats.maintenance_runtime_opens;
        const reuses_before = owner.publication_stats.maintenance_source_runtime_reuses;
        for (0..8 * namespace_count) |batch| {
            {
                var arena: std.heap.ArenaAllocator = .init(a);
                defer arena.deinit();
                const scratch = arena.allocator();
                const rows = try scratch.alloc([]const u8, 256);
                for (rows, 0..) |*row, ordinal| {
                    const id = batch * 256 + ordinal;
                    row.* = try std.fmt.allocPrint(scratch, "('row-{d}',{d},'benchmark canary café 東京')", .{ id, id });
                }
                const statement = try std.fmt.allocPrint(scratch, "INSERT INTO replay_{d} (_id,id,body) VALUES {s}", .{ batch % namespace_count, try std.mem.join(scratch, ",", rows) });
                _ = try ConstraintOverlayProbe.run(fixture.id, null, statement, null);
            }
            _ = owner.maintenanceTurnForTest() catch |err| switch (err) {
                error.EnrichmentWaitCanceled, error.EnrichmentSourceChanged, error.FileBusy, error.WouldBlock => true,
                else => return err,
            };
            try storage_probe.check();
        }
        owner.test_before_storage_service = null;
        try std.testing.expect(storage_probe.refusals > 0);
        if (namespace_count == 2) try std.testing.expect(storage_probe.checked);
        const diagnostics = owner.publication_stats.text_runtime_cache;
        std.debug.print("LITE_RELATIONAL_RUNTIME namespaces={d} opens={d} reuses={d} diagnostics={f}\n", .{ namespace_count, owner.publication_stats.maintenance_runtime_opens - opens_before, owner.publication_stats.maintenance_source_runtime_reuses - reuses_before, std.json.fmt(diagnostics, .{}) });
        try std.testing.expect(diagnostics.capture_attempts > 0);
        try std.testing.expect(diagnostics.retained_storage_yield > 0);
        try std.testing.expect(owner.publication_stats.maintenance_source_runtime_reuses > reuses_before);
        try std.testing.expectEqual(@as(u64, 0), diagnostics.capture_namespace_limit);
        // Verify the diagnostic uses the real replay path. Drain only after
        // the measured operation counts, then check every acknowledged row.
        var drained = false;
        for (0..512) |_| {
            _ = owner.maintenanceTurnForTest() catch |err| switch (err) {
                error.EnrichmentWaitCanceled, error.EnrichmentSourceChanged, error.FileBusy, error.WouldBlock => true,
                else => return err,
            };
            var lease = try fixture.connection.acquire();
            defer {
                lease.close();
                fixture.connection.gate.mutex.unlock(io);
            }
            const root = try owner.refresh();
            var pending = false;
            for (0..namespace_count) |n| {
                var name_buf: [32]u8 = undefined;
                const table = try @import("tables.zig").get(root, try std.fmt.bufPrint(&name_buf, "replay_{d}", .{n}));
                pending = try Owner.hasPendingReplay(table) or pending;
            }
            if (!pending) {
                drained = true;
                break;
            }
        }
        try std.testing.expect(drained);
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(io);
        }
        const root = try owner.refresh();
        for (0..8 * namespace_count) |batch| {
            var name_buf: [32]u8 = undefined;
            const table = try @import("tables.zig").get(root, try std.fmt.bufPrint(&name_buf, "replay_{d}", .{batch % namespace_count}));
            for (0..256) |row| {
                var key_buf: [32]u8 = undefined;
                var found = (try table.lookup(a, try std.fmt.bufPrint(&key_buf, "row-{d}", .{batch * 256 + row}), .{})) orelse return error.TestUnexpectedResult;
                defer found.deinit(a);
            }
        }
        try std.testing.expect((try root.owned_lite_backend.?.native_docstore.?.file.check()).valid);
    }
}

test "capi native maintenance text source reuse refreshes relational rows and fences table controls" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("relational-text-source-runtime", 5000, .sql);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const foreground = owner.current.?;
    const table = try @import("tables.zig").get(foreground, "numbers");
    try table.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    try table.batch(.{ .writes = &.{.{ .key = "first", .value = "{\"n\":1}" }}, .sync_level = .write });
    const source = try owner.refreshMaintenanceSnapshot(foreground, .source);
    const source_table = try @import("tables.zig").get(source, "numbers");
    try std.testing.expect(source_table.core.index_manager.relational_base_rows);
    owner.text_source_runtime_cache = (try TextSourceRuntimeCache.capture(source)) orelse return error.TestUnexpectedResult;
    const opens = owner.test_runtime_snapshot_opens;
    const old_head = source_table.core.store.lastReplaySequence(0);
    const file = &foreground.owned_lite_backend.?.native_docstore.?.file;
    try foreground.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(source.owned_lite_backend.?.native_docstore.?.file.header);
    try table.batch(.{ .writes = &.{.{ .key = "second", .value = "{\"n\":2}" }}, .sync_level = .write });
    try std.testing.expectEqual(source, try owner.refreshMaintenanceSnapshot(foreground, .source));
    try std.testing.expectEqual(opens, owner.test_runtime_snapshot_opens);
    try std.testing.expect(source_table.core.store.lastReplaySequence(0) > old_head);
    var found = (try source_table.lookup(std.testing.allocator, "second", .{})) orelse return error.TestUnexpectedResult;
    defer found.deinit(std.testing.allocator);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, found.json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed.value.object.get("n").?.integer);
    var changed_catalog = table.core.table_catalog;
    changed_catalog.transaction_admission_bytes /= 2;
    var change = try table.core.store.beginWriteTxn();
    errdefer change.abort();
    try change.put(@import("../storage/db/table_catalog.zig").key, &changed_catalog.encode());
    try change.commit();
    try std.testing.expect(!try owner.text_source_runtime_cache.?.matches(file));
}

const ProducerServiceBlockForTest = struct {
    entered: std.atomic.Value(bool) = .init(false),
    release: std.atomic.Value(bool) = .init(false),
    stopped: std.atomic.Value(bool) = .init(false),

    fn run(ptr: *anyopaque, job: *PendingProducer) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.entered.store(true, .release);
        while (!self.release.load(.acquire) and !job.owner.stopping.load(.acquire))
            h.handleLockIo().sleep(.fromMilliseconds(1), .awake) catch return;
        if (job.owner.stopping.load(.acquire)) self.stopped.store(true, .release);
    }

    fn start(self: *@This(), fixture: *AdmissionFollowupFixture) !void {
        const owner = fixture.connection.owner;
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            try owner.current.?.db.addEnrichment(.{ .name = "copy", .kind = .asset, .field = "text", .content_type = "text/plain", .producer_json = "{\"type\":\"copy\"}" });
        }
        owner.test_before_producer_service = .{ .ptr = self, .run = run };
        try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "producer-source", .value = "{\"text\":\"owned producer source\"}" }}, .sync_level = .write });
        const io = h.handleLockIo();
        const deadline = std.Io.Clock.awake.now(io).nanoseconds + 5 * std.time.ns_per_s;
        while (!self.entered.load(.acquire)) {
            try std.testing.expect(std.Io.Clock.awake.now(io).nanoseconds < deadline);
            _ = try owner.maintenanceTurnForTest();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
    }
};

test "capi native independent producer discovers durable backfill without a resident resolver" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("independent-producer-backfill", 5000, .batch);
    defer fixture.deinit();
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const database = &fixture.connection.owner.current.?.db;
    try database.addResolver(.{ .name = "r", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "entity/{{_key}}", .promote = false });
    // Exercise the cold-driver fallback without destroying the owned runtime
    // or altering its append-context lifetimes.
    const runtime = database.resolution_runtime;
    database.resolution_runtime = null;
    defer database.resolution_runtime = runtime;
    try std.testing.expect(!try Owner.producerNeedsWork(database));
    const key = @import("../storage/db/catalog/resolver_catalog.zig").reresolve_resume_key;
    {
        var write = try database.core.store.beginWriteTxn();
        errdefer write.abort();
        try write.put(key, "last-checked-source");
        try write.commit();
    }
    try std.testing.expect(try Owner.producerNeedsWork(database));
}

test "capi native independent producer shutdown drains its owned publication frame" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("independent-producer-shutdown", 5000, .batch);
    var closed = false;
    defer if (!closed) fixture.deinit() else {
        std.testing.allocator.free(fixture.path);
        fixture.tmp.cleanup();
    };
    var block: ProducerServiceBlockForTest = .{};
    defer block.release.store(true, .release);
    try block.start(&fixture);
    try std.testing.expect(fixture.connection.owner.pending_producer != null);
    api.antfly_db_close(fixture.id);
    closed = true;
    try std.testing.expect(block.stopped.load(.acquire));
    const reopened = try open(fixture.path, .{ .profile = .native, .open_mode = .query_readonly, .no_sync = true }, false);
    const id = try api.publishHandle(reopened);
    defer api.antfly_db_close(id);
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_lookup_json(id, .fromSlice("producer-source"), &out));
}

test "capi native independent producer uncertainty fences the owner after job retirement" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("independent-producer-uncertainty", 5000, .batch);
    defer fixture.deinit();
    const owner = fixture.connection.owner;
    var block: ProducerServiceBlockForTest = .{};
    defer block.release.store(true, .release);
    try block.start(&fixture);
    const job = owner.pending_producer.?;
    owner.gate.mutex.lockUncancelable(h.handleLockIo());
    job.root.owned_lite_backend.?.native_docstore.?.file.checkpoint_publication_uncertain = true;
    owner.gate.mutex.unlock(h.handleLockIo());
    defer {
        owner.gate.mutex.lockUncancelable(h.handleLockIo());
        owner.current.?.owned_lite_backend.?.native_docstore.?.file.checkpoint_publication_uncertain = false;
        owner.gate.mutex.unlock(h.handleLockIo());
    }
    try std.testing.expectError(error.OutcomeReconciliationRequired, owner.maintenanceTurnForTest());
    block.release.store(true, .release);
    const io = h.handleLockIo();
    const deadline = std.Io.Clock.awake.now(io).nanoseconds + 5 * std.time.ns_per_s;
    while (true) {
        owner.gate.mutex.lockUncancelable(io);
        const pending = owner.pending_producer != null;
        owner.gate.mutex.unlock(io);
        if (!pending) break;
        try std.testing.expect(std.Io.Clock.awake.now(io).nanoseconds < deadline);
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    owner.collectCompletedProducer();
    try std.testing.expect(owner.pending_producer == null);
    try std.testing.expectError(error.OutcomeReconciliationRequired, owner.maintenanceTurnForTest());
    // This new operation is refused by the retained reconciliation fence;
    // only the original ambiguous publication has an unknown commit outcome.
    try std.testing.expectEqual(h.capi.ErrorCode.stalled, fixture.call(.batch));
    owner.test_before_producer_service = null;
}

test "capi native independent producer wait keeps real text replay and credits runnable" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("independent-producer-credits", 5000, .batch);
    defer fixture.deinit();
    const owner = fixture.connection.owner;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"raw\",\"type\":\"full_text\"}")));
    var names: [128][32]u8 = undefined;
    var writes: [128]h.db_mod.types.BatchWrite = undefined;
    for (&writes, 0..) |*write, i| write.* = .{
        .key = try std.fmt.bufPrint(&names[i], "old-{d}", .{i}),
        .value = "{\"text\":\"old independent text credit canary\"}",
    };
    // These old records have only text consumers. A later expensive producer
    // must not prevent that text consumer from returning their retained bytes.
    for (0..8) |batch|
        try fixture.writeReplaySource(.{ .writes = writes[batch * 16 ..][0..16], .sync_level = .write });
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        try owner.current.?.db.addEnrichment(.{ .name = "copy", .kind = .asset, .field = "text", .content_type = "text/plain", .producer_json = "{\"type\":\"copy\"}" });
    }
    try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "new-producer-input", .value = "{\"text\":\"new provider input\"}" }}, .sync_level = .write });
    const Block = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        fn run(ptr: *anyopaque, job: *PendingProducer) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire) and !job.owner.stopping.load(.acquire))
                h.handleLockIo().sleep(.fromMilliseconds(1), .awake) catch return;
        }
    };
    var block: Block = .{};
    defer block.release.store(true, .release);
    owner.test_before_producer_service = .{ .ptr = &block, .run = Block.run };
    const io = h.handleLockIo();
    const deadline = std.Io.Clock.awake.now(io).nanoseconds + 5 * std.time.ns_per_s;
    while (!block.entered.load(.acquire)) {
        try std.testing.expect(std.Io.Clock.awake.now(io).nanoseconds < deadline);
        _ = try owner.maintenanceTurnForTest();
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(owner.pending_producer != null);
    var before_bytes: u64 = 0;
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(io);
        defer lease.close();
        const root = try owner.refresh();
        const file = &root.owned_lite_backend.?.native_docstore.?.file;
        var state = (try file.replayAdmission()).?;
        before_bytes = state.bytes;
        state.limits.bytes = state.bytes + 1;
        try file.putDocument(native.replay_admission.key, &state.encode());
    }
    const Request = struct {
        fixture: *AdmissionFollowupFixture,
        done: std.atomic.Value(bool) = .init(false),
        result: h.capi.ErrorCode = .internal,
        fn run(self: *@This()) void {
            self.result = self.fixture.call(.batch);
            self.done.store(true, .release);
        }
    };
    const polls = api.test_replay_capacity_polls.load(.monotonic);
    var request: Request = .{ .fixture = &fixture };
    const caller = try std.Thread.spawn(.{}, Request.run, .{&request});
    var joined = false;
    defer if (!joined) caller.join();
    while (api.test_replay_capacity_polls.load(.monotonic) == polls and !request.done.load(.acquire)) {
        try std.testing.expect(std.Io.Clock.awake.now(io).nanoseconds < deadline);
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(api.test_replay_capacity_polls.load(.monotonic) > polls);
    // Keep the provider blocked throughout real consumer service. No test
    // changes watermarks, deletes journal records, or raises the quota. The
    // writer's exact footprint, not the first small refund, determines success.
    var reclaimed = false;
    while (!request.done.load(.acquire) and std.Io.Clock.awake.now(io).nanoseconds < deadline) {
        _ = owner.maintenanceTurnForTest() catch |err| switch (err) {
            error.EnrichmentWaitCanceled => true,
            else => return err,
        };
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(io);
            defer lease.close();
            const root = try owner.refresh();
            const state = try root.owned_lite_backend.?.native_docstore.?.file.replayAdmission();
            reclaimed = state.?.bytes < before_bytes;
        }
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    caller.join();
    joined = true;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, request.result);
    try std.testing.expect(reclaimed);
    try std.testing.expect(!owner.pending_producer.?.done.load(.acquire));
    try std.testing.expect(block.entered.load(.acquire) and !block.release.load(.acquire));
    // Structural mutation may retire the producer's old catalog, but cannot
    // relocate an inline index array still borrowed by its separate actor.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"other\",\"type\":\"full_text\"}")));
    var deleted = false;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_delete_index(fixture.id, .fromSlice("other"), &deleted));
    try std.testing.expect(deleted);
    try std.testing.expect(!owner.pending_producer.?.done.load(.acquire));
    block.release.store(true, .release);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(fixture.id));
    owner.test_before_producer_service = null;
    try std.testing.expect(owner.pending_producer == null);
    try std.testing.expect(owner.publication_stats.producer_jobs_completed != 0);
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(io);
    defer lease.close();
    try std.testing.expect((try owner.current.?.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}

test "capi native cooperative replay separates raw text from generated artifact dependencies" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const background = @import("../storage/background_runtime.zig");
    const QueuedOutput = struct {
        fn commit(ptr: *anyopaque, batch: GeneratedBatch, promotions: Promotions, deletes: []const []const u8, guards: SourceGuards, fence: ?WriteFence) anyerror!@import("../storage/db/enrichment/enrichment_runtime.zig").GeneratedRecordCommit {
            const db: *h.db_mod.DB = @ptrCast(@alignCast(ptr));
            return db.enqueuePreparedGeneratedBatch(batch, promotions, deletes, guards, fence);
        }
    };
    for ([_]bool{ false, true }) |workers| {
        var host = try background.BackendRuntimeHandle.init(a, .{
            .backend = .manual,
            .borrowed_io = .{ .general = std.testing.io },
        });
        defer host.deinit();
        var tmp = try api.TestDirectory.init("independent-text-replay");
        defer tmp.cleanup();
        var db = try h.db_mod.DB.open(a, tmp.path(), .{
            .backend_runtime = host.ptr(),
            .executor = .{ .backend = .manual },
            .primary_backend = .{ .mem = .{} },
            .physical_root_mode = .external_backend,
            .start_index_workers = workers,
            .start_optional_runtimes = true,
            .start_optional_runtime_workers = false,
            .enrichment = .{ .enable_without_producers = true },
        });
        defer db.close();
        try db.addEnrichment(.{ .name = "copy", .kind = .asset, .field = "text", .content_type = "text/plain", .producer_json = "{\"type\":\"copy\"}" });
        try db.addIndex(.{ .name = "raw", .kind = .full_text, .config_json = "{}" });
        try db.addIndex(.{ .name = "generated", .kind = .full_text, .config_json = "{\"sources\":[{\"artifact\":\"copy\"}]}" });
        // Native maintenance queues generated output through its publication
        // port. Match that contract here: the generic in-memory writer applies
        // generated indexes eagerly and deliberately has no producer-cut fence.
        db.enrichment_runtime.?.write_ctx = &db;
        db.enrichment_runtime.?.write_fn = QueuedOutput.commit;
        try std.testing.expect(!try db.core.indexRequiresEnrichmentReplay("raw"));
        try std.testing.expect(try db.core.indexRequiresEnrichmentReplay("generated"));
        var names: [32][32]u8 = undefined;
        var writes: [32]h.db_mod.types.BatchWrite = undefined;
        for (&writes, 0..) |*write, i| {
            write.* = .{
                .key = try std.fmt.bufPrint(&names[i], "source-{d}", .{i}),
                .value = "{\"text\":\"silver submarine\"}",
            };
            try db.batch(.{ .writes = writes[i .. i + 1], .sync_level = .write });
        }
        const target = db.core.nextDerivedSequence();
        _ = try db.runBackgroundTurnWithCancellation(.none, .replay);
        const produced = db.enrichment_runtime.?.stats().applied_sequence;
        // Raw primary text and committed artifact effects can advance while
        // the producer still has requests outstanding. An artifact consumer's
        // watermark does not claim those future requests are complete.
        try std.testing.expect(produced < target);
        try std.testing.expect(try db.core.loadAppliedSequence(a, "raw") >= target);
        try std.testing.expectEqual(@as(u32, 32), db.core.index_manager.textIndex("raw").?.snapshot().liveDocCount());
        try std.testing.expect(db.core.index_manager.textIndex("generated").?.snapshot().liveDocCount() <= 8);
        try db.runUntilIdle();
        try std.testing.expect(try db.core.loadAppliedSequence(a, "generated") >= target);
        try std.testing.expectEqual(@as(u32, 32), db.core.index_manager.textIndex("generated").?.snapshot().liveDocCount());
        // Repeat across all three scheduling offsets and an overwrite, then
        // verify the full drain still completes both dependency classes.
        for (0..3) |_| {
            try db.batch(.{ .writes = &.{.{ .key = "source-0", .value = "{\"text\":\"gold submarine\"}" }}, .sync_level = .write });
            _ = try db.runBackgroundTurnWithCancellation(.none, .replay);
        }
        try db.runUntilIdle();
        const latest = db.core.nextDerivedSequence();
        try std.testing.expect(try db.core.loadAppliedSequence(a, "raw") >= latest);
        try std.testing.expect(try db.core.loadAppliedSequence(a, "generated") >= latest);
    }
}

test "capi native staged text replay distinguishes foreign index changes from its own target" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    for ([_][]const u8{ "graph", "text", "catalog" }) |changed_index| {
        var fixture = try AdmissionFollowupFixture.init("scoped-text-replay-progress", 5000, .batch);
        defer fixture.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
        try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "accepted", .value = "{\"text\":\"one admitted write\"}" }}, .sync_level = .write });
        const owner = fixture.connection.owner;
        var target: u64 = 0;
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            target = (try owner.refresh()).db.core.nextDerivedSequence();
        }
        const Probe = struct {
            connection: *Connection,
            changed_index: []const u8,
            publications: usize = 0,
            broad_refusals: usize = 0,
            failure: ?anyerror = null,
            fn notify(raw: *anyopaque, detached: *h.Handle) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.changeOtherIndex() catch |err| {
                    self.failure = err;
                    return;
                };
                if (std.mem.eql(u8, self.changed_index, "catalog")) {
                    Owner.enterMaintenanceWriter(detached, .allocator_service) catch |err| {
                        self.failure = err;
                        return;
                    };
                    Owner.leaveMaintenanceWriter(detached);
                    return;
                }
                // Exercise the former broad admission on this exact captured
                // runtime as a control, without building another library.
                Owner.enterMaintenanceWriter(detached, .index) catch |err| {
                    if (err == error.EnrichmentWaitCanceled) self.broad_refusals += 1 else self.failure = err;
                    return;
                };
                Owner.leaveMaintenanceWriter(detached);
                self.failure = error.TestExpectedBroadIndexRefusal;
            }
            fn changeOtherIndex(self: *@This()) !void {
                var lease = try self.connection.acquire();
                defer self.connection.gate.mutex.unlock(h.handleLockIo());
                defer lease.close();
                const root = try self.connection.owner.refresh();
                if (std.mem.eql(u8, self.changed_index, "catalog")) {
                    self.publications += 1;
                    var value: [8]u8 = undefined;
                    std.mem.writeInt(u64, &value, self.publications, .little);
                    try root.owned_lite_backend.?.native_docstore.?.file.putCatalogRecord("scoped-replay-catalog-canary", &value);
                    return;
                }
                const path = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}/replay-progress-canary", .{ root.db.core.index_manager.base_path, self.changed_index });
                defer alloc.free(path);
                var value: [8]u8 = undefined;
                self.publications += 1;
                std.mem.writeInt(u64, &value, self.publications, .little);
                try root.owned_lite_backend.?.native_docstore.?.file.putIndexCatalogRecord(path, &value);
            }
        };
        var probe: Probe = .{ .connection = fixture.connection, .changed_index = changed_index };
        owner.test_before_replay_index_publication = .{ .ptr = &probe, .notify = Probe.notify };
        defer owner.test_before_replay_index_publication = null;
        var caught_up = false;
        for (0..32) |_| {
            _ = owner.maintenanceTurnForTest() catch |err| switch (err) {
                error.EnrichmentWaitCanceled => false,
                else => return err,
            };
            if (probe.failure) |err| return err;
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const root = try owner.refresh();
            if (try root.db.core.loadAppliedSequence(alloc, "text") >= target) {
                caught_up = true;
                break;
            }
        }
        try std.testing.expect(probe.publications != 0);
        if (!std.mem.eql(u8, changed_index, "catalog")) try std.testing.expectEqual(probe.publications, probe.broad_refusals);
        try std.testing.expectEqual(std.mem.eql(u8, changed_index, "graph"), caught_up);
        owner.test_before_replay_index_publication = null;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(fixture.id));
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try owner.refresh();
        const text_domain = try std.fmt.allocPrint(alloc, "{s}/indexes/text", .{root.db.core.index_manager.base_path});
        defer alloc.free(text_domain);
        try std.testing.expect(try root.owned_lite_backend.?.native_docstore.?.file.indexScopeIdentity(text_domain) != null);
        try std.testing.expect((try root.owned_lite_backend.?.native_docstore.?.checkWithCancel(null)).valid);
        const snapshot = root.db.core.index_manager.textIndex("text").?.acquireSnapshot();
        defer snapshot.release();
        try std.testing.expectEqual(@as(u32, 1), snapshot.liveDocCount());
    }
}

test "capi native staged replay rejects a dropped and recreated target before activation" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("staged-replay-replacement", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "accepted", .value = "{\"text\":\"one admitted write\"}" }}, .sync_level = .write });
    const owner = fixture.connection.owner;
    const Probe = struct {
        id: ?*anyopaque,
        fired: bool = false,
        deleted: bool = false,
        result: h.capi.ErrorCode = .ok,
        fn notify(raw: *anyopaque, _: *h.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fired) return;
            self.fired = true;
            self.result = api.antfly_db_delete_index(self.id, .fromSlice("text"), &self.deleted);
            if (self.result != .ok) return;
            self.result = api.antfly_db_add_index_json(self.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}"));
        }
    };
    var probe: Probe = .{ .id = fixture.id };
    owner.test_before_replay_index_publication = .{ .ptr = &probe, .notify = Probe.notify };
    defer owner.test_before_replay_index_publication = null;
    for (0..32) |_| {
        _ = owner.maintenanceTurnForTest() catch |err| switch (err) {
            error.EnrichmentWaitCanceled => false,
            else => return err,
        };
        if (probe.fired) break;
    }
    try std.testing.expect(probe.fired and probe.deleted);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, probe.result);
    try std.testing.expect(owner.publication_stats.refused > 0);
    owner.test_before_replay_index_publication = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(fixture.id));
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const root = try owner.refresh();
    try std.testing.expect((try root.owned_lite_backend.?.native_docstore.?.checkWithCancel(null)).valid);
    const snapshot = root.db.core.index_manager.textIndex("text").?.acquireSnapshot();
    defer snapshot.release();
    try std.testing.expectEqual(@as(u32, 1), snapshot.liveDocCount());
}

test "capi native mixed index turns retain bounded immutable sources after catching up" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("mixed-settled-source-cache", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    const owner = fixture.connection.owner;
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const root = try owner.refresh();
        try root.db.addIndex(.{ .name = "graph", .kind = .graph, .config_json = "{}" });
        if (root.db.detachedTextSourceBlock() == null) return error.ExpectedMixedIndexCacheExclusion;
    }
    // Direct native graph metadata has no SST source to retain. Exercise a
    // real range-backed text artifact, rather than accidentally counting an
    // unrelated inner-LSM graph file as evidence of text source handoff.
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(std.testing.allocator);
    for (0..32768) |word| try text.print(std.testing.allocator, "handoff{d:0>8} ", .{word});
    const value = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .text = text.items }, .{});
    defer std.testing.allocator.free(value);
    var write: h.capi.WriteIntent = .{ .key = .fromSlice("large-text"), .value = .fromSlice(value), .is_delete = false };
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch(fixture.id, @ptrCast(&write), 1, null, 0, 0, 0));
    var complete = false;
    for (0..32) |_| {
        // Clear prior partial-turn pins: this assertion concerns the turn
        // that completes the target, rather than a cache left by earlier work.
        owner.immutable_sources.clear();
        owner.clearImmutableTextHandoff();
        _ = try owner.maintenanceTurnForTest();
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const root = try owner.refresh();
        const target = root.db.core.nextDerivedSequence();
        const text_applied = try root.db.core.loadAppliedSequence(std.testing.allocator, "text");
        const graph_applied = try root.db.core.loadAppliedSequence(std.testing.allocator, "graph");
        if (text_applied < target or graph_applied < target) continue;
        try std.testing.expect(owner.text_source_runtime_cache == null);
        // An empty control sweep can already have retired the actor on this
        // completion turn. Otherwise exercise the same retirement explicitly.
        // Both paths must retain independently leased immutable sources.
        if (owner.maintenance_snapshot) |actor| if (actor.db_live) owner.discardMaintenanceRuntime();
        if (owner.maintenance_snapshot) |actor| try std.testing.expect(!actor.db_live);
        if (owner.immutable_sources.count == 0) return error.MissingImmutableSourceHandoff;
        try std.testing.expect(owner.immutable_sources.range_bytes <= artifact_source.WorkingSet.max_range_bytes);
        complete = true;
        break;
    }
    if (!complete) return error.MixedIndexReplayDidNotSettle;
}

test "capi native registered replay fragments oversized batches across maintenance runtime replacement" {
    const a = std.testing.allocator;
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("registered-replay-fragments", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    var keys_buffer: [1024][32]u8 = undefined;
    var writes: [1024]h.capi.WriteIntent = undefined;
    for (&writes, 0..) |*write, i| write.* = .{
        .key = .fromSlice(try std.fmt.bufPrint(&keys_buffer[i], "source-{d}", .{i})),
        .value = .fromSlice("{\"text\":\"native fragment canary\"}"),
        .is_delete = false,
    };
    var replay_writes: [1024]h.db_mod.types.BatchWrite = undefined;
    for (&replay_writes, writes) |*write, source| write.* = .{ .key = source.key.bytes(), .value = source.value.bytes() };
    try fixture.writeReplaySource(.{ .writes = &replay_writes, .sync_level = .write });
    const owner = fixture.connection.owner;
    var target: u64 = 0;
    {
        var lease = try fixture.connection.acquire();
        defer {
            lease.close();
            fixture.connection.gate.mutex.unlock(h.handleLockIo());
        }
        const live = try owner.refresh();
        target = live.db.core.nextDerivedSequence();
        const state = try owner.groupSourceStateNamed(&live.db, "default");
        state.replay_budget.max_records = 8;
        state.replay_budget.lanes[0].records = 8;
    }
    const Probe = struct {
        captures: usize = 0,
        registered: bool = true,
        fn notify(ptr: *anyopaque, root: *h.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (root.maintenance_publication) |publication| if (publication.source_read != null) {
                self.captures += 1;
                self.registered = self.registered and root.db.executor.hasWorkers();
            };
        }
    };
    var probe: Probe = .{};
    owner.test_after_capture = .{ .ptr = &probe, .notify = Probe.notify };
    defer owner.test_after_capture = null;
    for ([_]u64{ 8, 16, 64, 72 }) |expected| {
        var observed: u64 = 0;
        for (0..64) |_| {
            _ = try owner.maintenanceTurnForTest();
            var lease = try fixture.connection.acquire();
            defer {
                lease.close();
                fixture.connection.gate.mutex.unlock(h.handleLockIo());
            }
            const live = try owner.refresh();
            const position = (try live.db.loadReplayFragmentState("text")) orelse continue;
            try std.testing.expectEqual(target, position.sequence);
            try std.testing.expectEqual(@as(u64, 1024), position.total_items);
            try std.testing.expect(try live.db.core.loadAppliedSequence(a, "text") < target);
            observed = position.next_item;
            const debts = try live.db.listDerivedReplayDebt(a);
            defer {
                for (debts) |*debt| debt.deinit(a);
                a.free(debts);
            }
            try std.testing.expectEqual(@as(usize, 1), debts.len);
            try std.testing.expectEqual(target, debts[0].fragment_sequence);
            try std.testing.expectEqual(observed, debts[0].fragment_next_item);
            try std.testing.expectEqual(@as(u64, 1024), debts[0].fragment_total_items);
            if (observed == expected) {
                // Close the actual actor; the next registered worker must
                // resume from durable fragment state rather than RAM.
                owner.discardMaintenanceRuntime();
                break;
            }
        }
        try std.testing.expectEqual(expected, observed);
    }
    try std.testing.expect(probe.captures >= 2 and probe.registered);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(fixture.id));
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const live = try owner.refresh();
    try std.testing.expect(try live.db.core.loadAppliedSequence(a, "text") >= target);
    const snapshot = live.db.core.index_manager.textIndex("text").?.acquireSnapshot();
    defer snapshot.release();
    try std.testing.expectEqual(@as(u32, 1024), snapshot.liveDocCount());
    try std.testing.expect((try live.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}

test "capi native maintenance partial text replay earns another source turn without advancing coverage" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("fragment-source-priority", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const database = &fixture.connection.owner.current.?.db;
    const cfg = database.core.index_manager.get("text").?;
    try std.testing.expect(cfg.coverage_generation != 0);
    const a = std.testing.allocator;
    const managed = [_]@import("../storage/db/catalog/index_manager.zig").ManagedIndexRef{.{ .name = "text", .kind = .full_text }};
    const before = try Owner.ReplayProgress.capture(a, database, &managed);
    defer a.free(before.indexes);
    const scalar = try database.core.loadAppliedSequence(a, "text");
    try @import("../storage/db/derived/apply_state.zig").saveReplayFragment(a, database.core.store, "text", .{
        .generation = cfg.coverage_generation,
        .config_hash = h.db_mod.types.indexConfigHash(cfg.*),
        .incarnation = database.root_incarnation,
    }, .{ .sequence = scalar + 1, .next_item = 8, .total_items = 1024, .fingerprint = @splat(1), .applied = true });
    try std.testing.expect(before.advanced(database));
    try std.testing.expectEqual(scalar, try database.core.loadAppliedSequence(a, "text"));
    const after = try Owner.ReplayProgress.capture(a, database, &managed);
    defer a.free(after.indexes);
    try std.testing.expect(!after.advanced(database));
}

test "capi native maintenance text source reuse refreshes primary observations and rejects changed configurations" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("text-source-runtime-reuse", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const foreground = owner.current.?;
    const source = try owner.refreshMaintenanceSnapshot(foreground, .source);
    owner.text_source_runtime_cache = (try TextSourceRuntimeCache.capture(source)) orelse return error.TestUnexpectedResult;
    const opened = owner.test_runtime_snapshot_opens;
    const store = source.db.core.store;
    _ = store.lastReplaySequence(0);
    const applied = try source.db.core.loadAppliedSequence(std.testing.allocator, "text");
    source.db.executor.removeWorker("text");
    try source.db.executor.addWorker("text", .{ .name = "text", .kind = .full_text }, applied + 100);
    const token = store.source_certificates.capture();
    const generation = store.query_generation_revision.load(.acquire);
    var next: [8]u8 = undefined;
    std.mem.writeInt(u64, &next, 13, .little);
    const file = &foreground.owned_lite_backend.?.native_docstore.?.file;
    // Opening immutable readers can publish leases; adopt their checked tip
    // before this independent foreground wrapper publishes a primary update.
    try foreground.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(source.owned_lite_backend.?.native_docstore.?.file.header);
    try file.putDocumentBatch(&.{
        .{ .key = "source-view-canary", .value = "fresh" },
        .{ .key = &keys.replay_meta_next_sequence_key, .value = &next },
    });
    try std.testing.expectEqual(source, try owner.refreshMaintenanceSnapshot(foreground, .source));
    try std.testing.expectEqual(opened, owner.test_runtime_snapshot_opens);
    try std.testing.expectEqual(@as(u64, 12), store.lastReplaySequence(0));
    try std.testing.expectEqual(applied, source.db.executor.appliedSequence("text").?);
    try std.testing.expect(store.source_certificates.confirm(token) == null);
    try std.testing.expect(store.query_generation_revision.load(.acquire) > generation);
    const value = try store.get(std.testing.allocator, "source-view-canary");
    defer std.testing.allocator.free(value);
    try std.testing.expectEqualStrings("fresh", value);
    const cache = owner.text_source_runtime_cache.?;
    const analysis_path = for (cache.controls) |control| {
        if (std.mem.indexOf(u8, control.path, "text_field_analyzers:") != null) break control.path;
    } else return error.TestUnexpectedResult;
    const previous_analysis = try file.getDocumentAlloc(std.testing.allocator, analysis_path);
    defer if (previous_analysis) |raw| std.testing.allocator.free(raw);
    const index_identity = file.activeCheckpoint().logical.indexes;
    try file.putDocument(analysis_path, "foreign analysis publication");
    try std.testing.expectEqual(index_identity, file.activeCheckpoint().logical.indexes);
    try std.testing.expect(!try cache.matches(file));
    if (previous_analysis) |raw| try file.putDocument(analysis_path, raw) else try file.deleteDocument(analysis_path);
    try std.testing.expect(try cache.matches(file));
    try file.putDocument(@import("../storage/db/resolver_projection.zig").epoch_key, "changed");
    try std.testing.expect(!try owner.text_source_runtime_cache.?.matches(file));
}

test "capi native visibility acknowledgement queues physical replay retirement without returning credits" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("visibility-retirement-service", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice("{\"inserts\":{\"one\":{\"text\":\"visible before physical retirement\"}},\"sync_level\":\"full_index\"}"), &output));
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try fixture.connection.owner.refresh();
        try std.testing.expect(root.db.async_context.replay_retirement_service != null);
        const file = &root.owned_lite_backend.?.native_docstore.?.file;
        var debt = (try file.replayAdmission()).?;
        try std.testing.expect(debt.bytes > 0 and debt.records > 0);
        const target = root.db.core.nextDerivedSequence();
        try std.testing.expect(try root.db.core.loadAppliedSequence(alloc, "text") >= target);
        const snapshot = root.db.core.index_manager.textIndex("text").?.acquireSnapshot();
        defer snapshot.release();
        try std.testing.expectEqual(@as(u32, 1), snapshot.liveDocCount());
        // A visibility acknowledgement releases no physical credits. Fill
        // the producer record quota exactly, then prove admission still fails.
        debt.limits.records = debt.records;
        try file.putDocument(native.replay_admission.key, &debt.encode());
        try std.testing.expectError(error.LiteReplayBacklogExceeded, file.requireReplayProducerCapacity());
    }
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(fixture.id));
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const root = try fixture.connection.owner.refresh();
    const file = &root.owned_lite_backend.?.native_docstore.?.file;
    const debt = (try file.replayAdmission()).?;
    try std.testing.expectEqual(@as(u64, 0), debt.bytes);
    try std.testing.expectEqual(@as(u64, 0), debt.records);
    try file.requireReplayProducerCapacity();
    try std.testing.expect((try file.check()).valid);
}

test "capi native retirement coalesces consumer lanes in one physical publication" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("retirement-one-publication", 1000, .batch);
    defer fixture.deinit();
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const root = try fixture.connection.owner.refresh();
    const store = root.db.core.store;
    const file = &root.owned_lite_backend.?.native_docstore.?.file;
    const refs = @import("../storage/db/derived/replay_reference.zig");
    const journal = @import("../storage/db/derived/change_journal.zig");
    try store.ensureReplayIndexInitialized();
    {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        try batch.put(&keys.replay_meta_init_key, "1");
        for ([_]u64{ 41, 42 }) |sequence| {
            try batch.put(&keys.replayEntryKey(keys.replay_all_kind, sequence), "canonical");
            inline for (@typeInfo(journal.TargetHint).@"enum".field_values) |ordinal| {
                try batch.put(&keys.replayEntryKey(@intCast(ordinal), sequence), &refs.encodeReference(sequence, @fromBackingInt(@intCast(ordinal))));
            }
        }
        try batch.commit();
    }
    const before = file.activeCheckpoint().commit_sequence;
    const debt_before = (try file.replayAdmission()).?;
    const state = try fixture.connection.owner.groupSourceState(&root.db);
    const previous_state = root.db.borrowed_group_source_state;
    root.db.borrowed_group_source_state = state;
    defer root.db.borrowed_group_source_state = previous_state;
    state.replay_retirement.watermark = 41;
    state.replay_retirement.bounded_continuation = false;
    state.replay_retirement.batch.first_pending_ns = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + std.time.ns_per_min;
    const deadline = state.replay_retirement.batch.deadline();
    // Background notifications leave physical records and credits unchanged
    // until a batch is due. Repeated visits do not renew the debt's age.
    for (0..2) |_| {
        try std.testing.expect(try root.db.runBackgroundTurnWithCancellation(.none, .retirement_batched));
        try std.testing.expectEqual(before, file.activeCheckpoint().commit_sequence);
        try std.testing.expectEqual(deadline, state.replay_retirement.batch.deadline());
        try std.testing.expectEqualDeep(debt_before, (try file.replayAdmission()).?);
    }
    // Explicit draining and pressure service bypass coalescing immediately.
    _ = try root.db.runBackgroundTurnWithCancellation(.none, .retirement);
    try std.testing.expectEqual(before + 1, file.activeCheckpoint().commit_sequence);
    try std.testing.expectError(error.NotFound, store.get(std.testing.allocator, &keys.replayEntryKey(keys.replay_all_kind, 41)));
    inline for (@typeInfo(journal.TargetHint).@"enum".field_values) |ordinal| {
        try std.testing.expectError(error.NotFound, store.get(std.testing.allocator, &keys.replayEntryKey(@intCast(ordinal), 41)));
        const retained = try store.get(std.testing.allocator, &keys.replayEntryKey(@intCast(ordinal), 42));
        defer std.testing.allocator.free(retained);
        try std.testing.expect(refs.isReference(retained));
    }
    const retained = try store.get(std.testing.allocator, &keys.replayEntryKey(keys.replay_all_kind, 42));
    defer std.testing.allocator.free(retained);
    try std.testing.expectEqualStrings("canonical", retained);
    const debt = (try file.replayAdmission()).?;
    const scanned = try file.scanReplayAdmission();
    try std.testing.expectEqual(debt.bytes, scanned.bytes);
    try std.testing.expectEqual(debt.records, scanned.records);
}

test "capi native retirement selection never materializes primary payload pages" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("retirement-keys-only", 1000, .batch);
    defer fixture.deinit();
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const root = try fixture.connection.owner.refresh();
    const value = try std.testing.allocator.alloc(u8, 1024 * 1024);
    defer std.testing.allocator.free(value);
    @memset(value, 'v');
    try root.db.core.store.put("retirement-payload", value);
    var txn = try root.db.core.store.beginReadTxn();
    defer txn.abort();
    // Read transactions use their own checkpoint-generation file, rather
    // than the writer's file and counters.
    const file = &root.owned_lite_backend.?.native_docstore.?.read_generation.?.file;
    const cached = file.page_cache_enabled.swap(false, .monotonic);
    defer file.page_cache_enabled.store(cached, .monotonic);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const pages = @import("../storage/db/group_source_page.zig");
    const before = file.test_backing_read_bytes.load(.monotonic);
    const retired = try pages.prepare(arena.allocator(), &txn, 7, 3, .retire, "", "retirement-payload");
    try std.testing.expectEqual(@as(usize, 1), retired.candidates.len);
    try std.testing.expect(retired.done);
    // Index navigation and the liveness header are small; the external
    // payload must remain unread, even with all page-cache hits disabled.
    const retired_bytes = file.test_backing_read_bytes.load(.monotonic) - before;
    try std.testing.expect(retired_bytes < 64 * 1024);
    const scan_before = file.test_backing_read_bytes.load(.monotonic);
    const scanned = try pages.prepare(arena.allocator(), &txn, 7, 3, .scan, "", "retirement-payload");
    try std.testing.expectEqual(@as(usize, 1), scanned.candidates.len);
    try std.testing.expectEqual(@import("../storage/db/group_sources.zig").digest(value), scanned.candidates[0].digest);
    try std.testing.expect(file.test_backing_read_bytes.load(.monotonic) - scan_before >= value.len);
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
        fn apply(ptr: *anyopaque, handle: *h.Handle, primary_committed: *bool) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.attempts += 1;
            if (self.attempts == 1) try h.handleLockIo().sleep(.fromMilliseconds(750), .awake);
            try handle.database().*.batchTracked(self.batch.req, primary_committed);
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
    try std.testing.expectError(error.OutcomeReconciliationRequired, owner.refresh());
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
    try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "later", .value = "{\"text\":\"journal epoch retention canary\"}" }}, .sync_level = .write });
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

test "capi native allocator proof verified replacement preserves the checked digest cache" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("verified-replacement-digest", 1000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const owner = fixture.connection.owner;
    const previous = owner.current.?;
    // A quiescent maintenance owner has already authenticated this exact tip.
    // Changing metadata forces serving-runtime replacement; its cache can move
    // under the path lease without repeating allocator-wide admission work.
    const snapshot = try owner.refreshMaintenanceSnapshot(previous, .storage);
    const file = &snapshot.owned_lite_backend.?.native_docstore.?.file;
    try file.putCatalogRecord("runtime-affecting-metadata", "checked publication");
    try std.testing.expect(file.free_pages_verified);
    try std.testing.expect(try file.certifyAllocator());
    const ledger = owner.gate.writer_allocator_cache.holder.?.ledger.?;
    const rebuilds = file.allocatorWorkSnapshot().digest_rebuilds;
    const loads = file.allocatorWorkSnapshot().full_loads;
    const digest_work = ledger.digestWork();
    const current = try owner.refresh();
    const adopted = &current.owned_lite_backend.?.native_docstore.?.file;
    try std.testing.expect(current != previous);
    try std.testing.expect(adopted.free_pages_verified);
    try std.testing.expect(owner.gate.writer_allocator_cache.holder.?.ledger != null);
    try std.testing.expectEqual(ledger, owner.gate.writer_allocator_cache.holder.?.ledger.?);
    try std.testing.expectEqual(loads, adopted.allocatorWorkSnapshot().full_loads);
    try std.testing.expectEqual(rebuilds, adopted.allocatorWorkSnapshot().digest_rebuilds);
    try std.testing.expectEqual(digest_work, ledger.digestWork());
    try std.testing.expect((try adopted.check()).valid);
}

test "capi native allocator proof storage-only foreign tip avoids runtime refresh" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("physical-tip-without-runtime", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const owner = fixture.connection.owner;
    const foreground = owner.current.?;
    const entry_header = foreground.owned_lite_backend.?.native_docstore.?.file.header;
    foreground.sql_commit_recovery_needed = false;
    foreground.sql_ddl_recovery_needed = false;
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        var peer = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), fixture.path, .{ .externally_locked = true, .no_sync = true });
        defer peer.close();
        try peer.inheritAllocatorVerification(&foreground.owned_lite_backend.?.native_docstore.?.file);
        try peer.putCatalogRecord("runtime-affecting-metadata", "peer publication");
        try std.testing.expect(try peer.certifyAllocator());
    }
    const refreshes = owner.publication_stats.serving_runtime_refreshes;
    const runtime_opens = owner.test_runtime_snapshot_opens;
    const cache_transfers_before = foreground.owned_lite_backend.?.native_docstore.?.file.allocatorWorkSnapshot().cache_transfers;
    try std.testing.expect(try owner.storageMaintenanceTurn());
    try std.testing.expectEqual(foreground, owner.current.?);
    try std.testing.expectEqualDeep(entry_header, foreground.owned_lite_backend.?.native_docstore.?.file.header);
    try std.testing.expectEqual(refreshes, owner.publication_stats.serving_runtime_refreshes);
    try std.testing.expectEqual(runtime_opens, owner.test_runtime_snapshot_opens);
    try std.testing.expect(owner.maintenance_snapshot != null);
    try std.testing.expect(!owner.maintenance_snapshot.?.db_live);
    try std.testing.expect(owner.maintenance_snapshot.?.sql_commit_recovery_needed);
    try std.testing.expect(owner.maintenance_snapshot.?.sql_ddl_recovery_needed);
    const physical_file = &owner.maintenance_snapshot.?.owned_lite_backend.?.native_docstore.?.file;
    try std.testing.expect(physical_file.free_pages_verified);
    try std.testing.expect(owner.gate.writer_allocator_cache.holder.?.ledger != null);
    // Physical telemetry is absorbed into the serving owner after service.
    try std.testing.expect(foreground.owned_lite_backend.?.native_docstore.?.file.allocatorWorkSnapshot().cache_transfers > cache_transfers_before);
    try std.testing.expectEqual(@as(usize, 0), foreground.query_borrows);
    // Logical admission must still refresh and expose the real data; physical
    // housekeeping never certifies an obsolete schema/index runtime as current.
    try fixture.expectWritten(.batch, true);
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const current = try owner.refresh();
    try std.testing.expect((try current.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}

test "capi native uncertain owner remains fenced while reopen reconstructs a fresh runtime" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    inline for (.{ "sql", "checkpoint", "secret" }) |kind| {
        var fixture = try AdmissionFollowupFixture.init("uncertain-owner-reopen-" ++ kind, 5000, .batch);
        defer fixture.deinit();
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
        const owner = fixture.connection.owner;
        const root = owner.current.?;
        const store = root.owned_lite_backend.?.native_docstore.?;
        owner.gate.mutex.lockUncancelable(h.handleLockIo());
        var lease = try native.lockWriterPathWithIo(alloc, h.handleLockIo(), fixture.path);
        if (comptime std.mem.eql(u8, kind, "sql")) root.sql_decision_uncertain = true else if (comptime std.mem.eql(u8, kind, "checkpoint")) store.file.checkpoint_publication_uncertain = true else store.secret_store_uncertain = true;
        const refusal = owner.refresh();
        lease.close();
        owner.gate.mutex.unlock(h.handleLockIo());
        defer {
            owner.gate.mutex.lockUncancelable(h.handleLockIo());
            root.sql_decision_uncertain = false;
            store.file.checkpoint_publication_uncertain = false;
            store.secret_store_uncertain = false;
            owner.gate.mutex.unlock(h.handleLockIo());
        }
        try std.testing.expectError(error.OutcomeReconciliationRequired, refusal);
        // Keep the original handle alive: reopen must not select its compatible
        // cached owner, even though the on-disk checkpoint is unchanged.
        const reopened = try open(fixture.path, .{ .profile = .native, .no_sync = true, .busy_timeout_ms = 5000 }, false);
        defer h.closeHandle(reopened);
        try std.testing.expect(reopened.lite_connection.?.owner != owner);
        const connection = reopened.lite_connection.?;
        var held: ?native.PathWriterLock = try connection.acquire();
        defer {
            connection.detach();
            if (held) |*owned| owned.close();
            connection.gate.mutex.unlock(h.handleLockIo());
        }
        const recovered = try connection.refresh(&held, .read_only);
        try std.testing.expect(!Owner.needsOutcomeReconciliation(recovered));
        try std.testing.expectError(error.OutcomeReconciliationRequired, owner.refresh());
    }
}

test "capi native allocator proof zero timeout preserves process local serialization" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    inline for (.{ false, true }) |connection_lane| {
        var fixture = try AdmissionFollowupFixture.init(if (connection_lane) "zero-connection-lane" else "zero-path-lane", 0, .batch);
        defer fixture.deinit();
        const mutex = if (connection_lane) &fixture.connection.mutex else &fixture.connection.gate.mutex;
        const io = h.handleLockIo();
        const Probe = struct {
            fixture: *AdmissionFollowupFixture,
            finished: std.atomic.Value(bool) = .init(false),
            result: h.capi.ErrorCode = .internal,
            fn run(self: *@This()) void {
                self.result = self.fixture.call(.batch);
                self.finished.store(true, .release);
            }
        };
        mutex.lockUncancelable(io);
        var held = true;
        defer if (held) mutex.unlock(io);
        var probe: Probe = .{ .fixture = &fixture };
        const thread = try std.Thread.spawn(.{ .stack_size = 16 * 1024 * 1024 }, Probe.run, .{&probe});
        var joined = false;
        defer {
            if (held) {
                mutex.unlock(io);
                held = false;
            }
            if (!joined) thread.join();
        }
        // Wait for enrollment or refusal, instead of assuming a sleep lets
        // the caller reach its local gate. Old zero-timeout serialization must
        // retain the queued admission while the holder is still active.
        const deadline = std.Io.Clock.awake.now(io).nanoseconds + 5 * std.time.ns_per_s;
        var queued = false;
        while (!probe.finished.load(.acquire)) {
            mutex.state.lockUncancelable(io);
            queued = mutex.head != null;
            mutex.state.unlock(io);
            if (queued or std.Io.Clock.awake.now(io).nanoseconds >= deadline) break;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        const refused_early = probe.finished.load(.acquire);
        mutex.unlock(io);
        held = false;
        thread.join();
        joined = true;
        try std.testing.expect(queued and !refused_early);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, probe.result);
    }
}

test "capi native allocator proof timed handoffs retain verification without admitting a mutation" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    inline for (.{ false, true }) |during_reader_close| {
        var fixture = try AdmissionFollowupFixture.init(if (during_reader_close) "proof-close-handoff-budget" else "proof-walk-handoff-budget", 50, .batch);
        defer fixture.deinit();
        const owner = fixture.connection.owner;
        const Probe = struct {
            owner: *Owner,
            entered: std.Io.Event = .unset,
            thread: ?std.Thread = null,
            failure: ?anyerror = null,
            watchdog_fired: bool = false,
            fn hold(self: *@This()) void {
                const input_io = h.handleLockIo();
                const mutex = &self.owner.gate.mutex;
                mutex.lockUncancelable(input_io);
                defer mutex.unlock(input_io);
                mutex.state.lockUncancelable(input_io);
                const serving = mutex.serving;
                mutex.state.unlock(input_io);
                self.entered.set(input_io);
                const deadline = std.Io.Clock.awake.now(input_io).nanoseconds + 2 * std.time.ns_per_s;
                while (true) {
                    mutex.state.lockUncancelable(input_io);
                    // Expiring the queued timed admission completes its node
                    // even though this actor still owns the gate. Release only
                    // after that refusal, so mandatory cleanup can drain.
                    const refused = mutex.serving != serving;
                    mutex.state.unlock(input_io);
                    if (refused) return;
                    if (std.Io.Clock.awake.now(input_io).nanoseconds >= deadline) {
                        self.watchdog_fired = true;
                        return;
                    }
                    input_io.sleep(.fromMilliseconds(1), .awake) catch return;
                }
            }
            fn captured(ptr: *anyopaque, _: *native.NativeFile) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.start() catch |err| {
                    self.failure = err;
                };
            }
            fn start(self: *@This()) !void {
                if (self.thread != null) return error.UnexpectedRepeatedProof;
                self.thread = try std.Thread.spawn(.{}, hold, .{self});
                try self.entered.waitTimeout(h.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
            }
            fn join(self: *@This()) void {
                if (self.thread) |thread| thread.join();
                self.thread = null;
            }
        };
        var probe: Probe = .{ .owner = owner };
        defer probe.join();
        if (during_reader_close) owner.test_before_allocator_reader_close = .{ .ptr = &probe, .notify = Probe.captured } else owner.test_after_allocator_capture = .{ .ptr = &probe, .notify = Probe.captured };
        defer {
            owner.test_after_allocator_capture = null;
            owner.test_before_allocator_reader_close = null;
        }
        const walks_before = owner.publication_stats.allocator_proof_walks;
        const result = fixture.call(.batch);
        probe.join();
        owner.test_after_allocator_capture = null;
        owner.test_before_allocator_reader_close = null;
        if (probe.failure) |err| return err;
        try std.testing.expectEqual(h.capi.ErrorCode.busy, result);
        try std.testing.expect(!probe.watchdog_fired);
        try std.testing.expect(owner.allocator_proof_receipt != null or owner.current.?.owned_lite_backend.?.native_docstore.?.file.free_pages_verified);
        try std.testing.expectEqual(walks_before + 1, owner.publication_stats.allocator_proof_walks);
        try fixture.expectWritten(.batch, false);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
        try fixture.expectWritten(.batch, true);
        try std.testing.expectEqual(walks_before + 1, owner.publication_stats.allocator_proof_walks);
    }
}

test "capi native detached source preparation does not require a stale allocator proof" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("source-read-without-writer-proof", 5000, .batch);
    defer fixture.deinit();
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "captured-source", .value = "{\"text\":\"captured source remains valid\"}" }}, .sync_level = .write });
    const owner = fixture.connection.owner;
    const Probe = struct {
        fixture: *AdmissionFollowupFixture,
        fired: bool = false,
        failure: ?anyerror = null,
        fn notify(ptr: *anyopaque, root: *h.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fired) return;
            const publication = root.maintenance_publication orelse return;
            if (publication.source_read == null) return;
            self.fired = true;
            self.check(root) catch |err| {
                self.failure = err;
            };
        }
        fn check(self: *@This(), root: *h.Handle) !void {
            // Only primary state changes. The captured source and its index
            // inputs remain valid; final publication must acquire a current
            // checked allocator rather than authenticate an obsolete cut.
            try self.fixture.writeReplaySource(.{ .writes = &.{.{ .key = "unrelated-a", .value = "{\"text\":\"unrelated primary a\"}" }}, .sync_level = .write });
            {
                var lease = try self.fixture.connection.acquire();
                defer {
                    lease.close();
                    self.fixture.connection.gate.mutex.unlock(h.handleLockIo());
                }
                const current = try self.fixture.connection.owner.refresh();
                const store = root.owned_lite_backend.?.native_docstore.?;
                try store.refreshIndexPublicationCheckpoint(current.owned_lite_backend.?.native_docstore.?.file.header);
                // Model an evicted verification capability independently of
                // the immutable source read retained by this turn.
                store.file.free_pages_verified = false;
            }
            try self.fixture.writeReplaySource(.{ .writes = &.{.{ .key = "unrelated-b", .value = "{\"text\":\"unrelated primary b\"}" }}, .sync_level = .write });
        }
    };
    var probe: Probe = .{ .fixture = &fixture };
    owner.test_after_capture = .{ .ptr = &probe, .notify = Probe.notify };
    defer owner.test_after_capture = null;
    const failures = owner.publication_stats.maintenance_failures.source;
    const publications = owner.publication_stats.replay_index_groups;
    for (0..16) |_| {
        _ = owner.maintenanceTurn() catch |err| switch (err) {
            error.EnrichmentWaitCanceled, error.EnrichmentSourceChanged => true,
            else => return err,
        };
        if (probe.failure) |err| return err;
        if (probe.fired) break;
    }
    try std.testing.expect(probe.fired);
    try std.testing.expectEqual(failures, owner.publication_stats.maintenance_failures.source);
    try std.testing.expect(owner.publication_stats.replay_index_groups > publications);
    var lease = try fixture.connection.acquire();
    defer {
        lease.close();
        fixture.connection.gate.mutex.unlock(h.handleLockIo());
    }
    const current = try owner.refresh();
    try std.testing.expect((try current.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}

test "capi native a parked text consumer does not starve independent replay" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("independent-replay-consumers", 5000, .batch);
    defer fixture.deinit();
    defer test_pause_private_merge.store(false, .release);
    const owner = fixture.connection.owner;
    owner.test_text_merge_segment_limit = 12;
    defer owner.test_text_merge_segment_limit = null;
    _ = try seedPausedPrivateMergeWithDeletes(&fixture, null, false);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"healthy\",\"type\":\"full_text\"}")));
    var initial: u32 = 0;
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try owner.refresh();
        initial = root.db.core.index_manager.textIndexEntry("healthy").?.persistent.snapshot().liveDocCount();
    }
    try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "independent-source", .value = "{\"text\":\"healthy sibling must advance\"}" }}, .sync_level = .write });
    for (0..64) |_| {
        _ = try owner.maintenanceTurn();
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try owner.refresh();
        if (root.db.core.index_manager.textIndexEntry("healthy").?.persistent.snapshot().liveDocCount() == initial + 1) break;
    }
    try std.testing.expect(owner.publication_stats.replay_capacity_yields != 0);
    try std.testing.expectEqual(@as(u64, 0), owner.publication_stats.text_merge_jobs_applied);
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const root = try owner.refresh();
    try std.testing.expectEqual(initial + 1, root.db.core.index_manager.textIndexEntry("healthy").?.persistent.snapshot().liveDocCount());
    try std.testing.expect((try root.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}

test "capi native replay capacity yield starts a merge before any result is pending" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("replay-capacity-starts-merge", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    const owner = fixture.connection.owner;
    owner.test_text_merge_segment_limit = 12;
    defer owner.test_text_merge_segment_limit = null;
    var live_before: u32 = 0;
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try owner.refresh();
        const entry = root.db.core.index_manager.textIndexEntry("text").?;
        for (0..12) |i| {
            var key: [32]u8 = undefined;
            var writer = segment_mod.SegmentWriter.init(std.testing.allocator);
            defer writer.deinit();
            try writer.addStoredDoc(try std.fmt.bufPrint(&key, "capacity-{d}", .{i}), "{}");
            const bytes = try writer.build();
            defer std.testing.allocator.free(bytes);
            try entry.persistent.indexSegment(bytes);
        }
        live_before = entry.persistent.snapshot().liveDocCount();
    }
    try std.testing.expect(owner.pending_text_merge == null);
    try fixture.writeReplaySource(.{ .writes = &.{.{ .key = "capacity-replay-source", .value = "{\"text\":\"source after capacity is restored\"}" }}, .sync_level = .write });
    const failures_before = owner.publication_stats.maintenance_failures.source;
    const ColdSource = struct {
        owner: *Owner,
        cleared: bool = false,
        fn afterCapture(_: *anyopaque, root: *h.Handle) void {
            // Optional graph cleanup cannot run until its independent reader
            // fence opens. Keep that service refused while text needs merge
            // capacity: neither its failure nor its retry cursor may starve
            // the text lane that returns replay credits.
            root.db.core.index_manager.graph_retirement_closed.store(true, .release);
            root.db.borrowed_group_source_state.?.control_maintenance_slot = 5;
        }
        fn beforeCapture(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const pending = self.owner.pending_text_merge orelse return;
            if (!pending.ready) return;
            // A reopened/cold namespace has no new foreground notification.
            // Its capacity refusal made no replay progress to requeue either.
            // Remove unrelated hints before the ready publication so only an
            // explicit capacity-restored wake can resume this source.
            while (self.owner.runnable_sources.pending(h.handleLockIo()))
                _ = self.owner.runnable_sources.choose(h.handleLockIo());
            self.cleared = true;
        }
    };
    var cold: ColdSource = .{ .owner = owner };
    owner.test_before_maintenance_capture = .{ .ptr = &cold, .notify = ColdSource.beforeCapture };
    owner.test_after_capture = .{ .ptr = &cold, .notify = ColdSource.afterCapture };
    defer owner.test_after_capture = null;
    defer owner.test_before_maintenance_capture = null;
    for (0..512) |_| {
        _ = try owner.maintenanceTurn();
        if (owner.publication_stats.text_merge_jobs_applied != 0) break;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    owner.test_before_maintenance_capture = null;
    owner.test_after_capture = null;
    try std.testing.expectEqual(@as(u64, 0), owner.publication_stats.maintenance_failures.artifacts);
    if (!cold.cleared) return error.TestColdSourceQueueWasNotCleared;
    if (owner.publication_stats.text_merge_jobs_applied == 0) return error.TestColdSourceMergeDidNotPublish;
    if (!owner.runnable_sources.pending(h.handleLockIo())) return error.TestCapacityRestoredWithoutSourceWake;
    for (0..512) |_| {
        _ = try owner.maintenanceTurn();
        if (owner.publication_stats.replay_index_groups != 0) break;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(owner.publication_stats.replay_capacity_yields != 0);
    try std.testing.expect(owner.publication_stats.text_merge_jobs_started != 0);
    try std.testing.expect(owner.publication_stats.text_merge_jobs_applied != 0);
    try std.testing.expect(owner.publication_stats.replay_index_groups != 0);
    try std.testing.expectEqual(failures_before, owner.publication_stats.maintenance_failures.source);
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const root = try owner.refresh();
    try std.testing.expectEqual(live_before + 1, root.db.core.index_manager.textIndexEntry("text").?.persistent.snapshot().liveDocCount());
    try std.testing.expect((try root.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}

test "capi native foreground text shares the replay segment capacity" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("foreground-text-capacity", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    const owner = fixture.connection.owner;
    for (0..8) |i| {
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const root = try owner.refresh();
            root.db.core.index_manager.text_merge_segment_limit.store(2, .release);
        }
        var key: [32]u8 = undefined;
        const writes = [_]h.capi.WriteIntent{.{ .key = .fromSlice(try std.fmt.bufPrint(&key, "capacity-{d}", .{i})), .value = .fromSlice("{\"text\":\"bounded foreground publication\"}"), .is_delete = false }};
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch(fixture.id, &writes, writes.len, null, 0, 0, 0));
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try owner.refresh();
        const snapshot = root.db.core.index_manager.textIndexEntry("text").?.persistent.acquireSnapshot();
        defer snapshot.release();
        try std.testing.expect(snapshot.segments.len <= 2);
    }
    try std.testing.expectEqual(@as(u64, 8), owner.current.?.db.core.store.lastReplaySequence(0));
    try std.testing.expect((try owner.current.?.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}

test "capi native maintenance keeps page failures distinct from cooperative yields" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("maintenance-page-failure-count", 5000, .batch);
    defer fixture.deinit();
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const owner = fixture.connection.owner;
    const before = owner.publication_stats.maintenance_page_errors;
    owner.noteMaintenanceFailure(error.EnrichmentWaitCanceled, 0, .artifacts);
    try std.testing.expectEqual(before, owner.publication_stats.maintenance_page_errors);
    owner.noteMaintenanceFailure(error.UnexpectedNativePageKind, 0, .artifacts);
    try std.testing.expectEqual(before + 1, owner.publication_stats.maintenance_page_errors);
    owner.noteMaintenanceFailure(error.NativePageChecksumMismatch, 0, .source);
    owner.noteMaintenanceFailure(error.EnrichmentSourceChanged, 0, .artifacts);
    try std.testing.expectEqual(before + 2, owner.publication_stats.maintenance_page_errors);
    try std.testing.expectEqualStrings("EnrichmentSourceChanged", owner.publication_stats.last_maintenance_failure.?);
}

test "capi native batch visibility releases publisher admission and never repeats a committed transform" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("batch-visibility-handoff", 5000, .batch);
    defer fixture.deinit();
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice("{\"inserts\":{\"counter\":{\"text\":\"counter token\",\"count\":0}},\"sync_level\":\"write\"}"), &out));
    api.antfly_buffer_free_zero(&out);
    // One segment of foreground capacity deliberately routes later records to
    // owner replay. The fixture has no worker, so no progress is possible until
    // the request releases admission and explicitly services the owner.
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try fixture.connection.owner.refresh();
        root.db.core.index_manager.text_merge_segment_limit.store(1, .release);
    }
    const Probe = struct {
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        id: *anyopaque,
        owner: *Owner,
        admission_released: bool = false,
        code: h.capi.ErrorCode = .internal,
        fn waiting(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.admission_released = self.owner.gate.mutex.tryLock();
            if (self.admission_released) self.owner.gate.mutex.unlock(h.handleLockIo());
            self.entered.set(h.handleLockIo());
            self.release.waitUncancelable(h.handleLockIo());
        }
        fn run(self: *@This()) void {
            var result: h.capi.Buffer = .{};
            defer api.antfly_buffer_free_zero(&result);
            self.code = api.antfly_db_batch_json(self.id, .fromSlice("{\"transforms\":[{\"key\":\"counter\",\"operations\":[{\"op\":\"$inc\",\"path\":\"count\",\"value\":1}]}],\"sync_level\":\"full_index\"}"), &result);
            if (self.code != .ok) std.debug.print("visibility handoff: {s}\n", .{std.mem.span(h.capi.lastErrorDetail())});
        }
    };
    var probe: Probe = .{ .id = fixture.id, .owner = fixture.connection.owner };
    api.test_before_batch_visibility_wait = .{ .ptr = &probe, .run = Probe.waiting };
    defer api.test_before_batch_visibility_wait = null;
    const thread = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Probe.run, .{&probe});
    var joined = false;
    defer {
        probe.release.set(h.handleLockIo());
        if (!joined) thread.join();
    }
    try probe.entered.waitTimeout(h.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } });
    try std.testing.expect(probe.admission_released);
    // Another logical writer can publish before the first caller's visibility
    // completes; it must not be trapped behind the queue leader or path lease.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice("{\"inserts\":{\"peer\":{\"text\":\"peer token\"}},\"sync_level\":\"write\"}"), &out));
    api.antfly_buffer_free_zero(&out);
    probe.release.set(h.handleLockIo());
    thread.join();
    joined = true;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, probe.code);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_lookup_json(fixture.id, .fromSlice("counter"), &out));
    const value = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, out.ptr.?[0..out.len], .{});
    defer value.deinit();
    try std.testing.expectEqual(@as(i64, 1), value.value.object.get("count").?.integer);
    try std.testing.expect((try fixture.connection.owner.current.?.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}

test "capi native consumerless source observes new tips without replaying empty work" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("consumerless-source", 5000, .batch);
    defer fixture.deinit();
    const owner = fixture.connection.owner;
    try std.testing.expectEqual(@as(usize, 0), owner.current.?.db.core.index_manager.text_indexes.items.len);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, fixture.call(.batch));
    const key: Owner.MaintenanceKey = .{ .table_id = owner.current.?.db.core.identity_namespace.table_id, .incarnation = owner.current.?.db.root_incarnation };
    var warmed = false;
    for (0..64) |_| {
        _ = try owner.maintenanceTurnForTest();
        if (owner.maintenance_schedules.getPtr(key)) |schedule| {
            if (schedule.covered_text_controls != null and !schedule.database_pending and !schedule.startup_replay_pending and !owner.maintenance_storage_pending) {
                warmed = true;
                break;
            }
        }
    }
    if (!warmed) return error.TestConsumerlessWarmupMissing;
    const far_future = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + std.time.ns_per_hour;
    owner.maintenance_schedules.getPtr(key).?.next_database_audit_ns = far_future;
    const source_turns = owner.publication_stats.maintenance_source_turns;
    const runtime_opens = owner.test_runtime_snapshot_opens;
    const observations = owner.publication_stats.covered_source_observations;
    for (0..16) |i| {
        var bytes: [192]u8 = undefined;
        const request = try std.fmt.bufPrint(&bytes, "{{\"inserts\":{{\"doc-{d}\":{{\"n\":{d}}}}},\"sync_level\":\"write\"}}", .{ i, i });
        var output: h.capi.Buffer = .{};
        defer api.antfly_buffer_free_zero(&output);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice(request), &output));
        for (0..8) |_| {
            _ = try owner.maintenanceTurnForTest();
            if (owner.maintenance_schedules.getPtr(key).?.serviced_sequence == owner.current.?.db.core.nextDerivedSequence()) break;
        }
    }
    try std.testing.expectEqual(source_turns, owner.publication_stats.maintenance_source_turns);
    try std.testing.expectEqual(runtime_opens, owner.test_runtime_snapshot_opens);
    if (owner.publication_stats.covered_source_observations <= observations) return error.TestConsumerlessTipObservationMissing;
    // Catalog changes revoke the fast witness even without a source append.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(fixture.id, .fromSlice("{\"name\":\"text\",\"type\":\"full_text\"}")));
    var changed: h.capi.Buffer = .{};
    defer api.antfly_buffer_free_zero(&changed);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(fixture.id, .fromSlice("{\"inserts\":{\"new-index-source\":{\"text\":\"catalog change needs service\"}},\"sync_level\":\"write\"}"), &changed));
    // Manual turns still honor real maintenance retry/housekeeping deadlines.
    // A fixed number of immediate turns can finish before a deferred service
    // becomes runnable on a fast or differently scheduled test host.
    const deadline = std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds + 5 * std.time.ns_per_s;
    while (std.Io.Clock.awake.now(h.handleLockIo()).nanoseconds < deadline) {
        _ = try owner.maintenanceTurnForTest();
        if (owner.publication_stats.maintenance_source_turns > source_turns) break;
        try h.handleLockIo().sleep(.fromMilliseconds(1), .awake);
    }
    if (owner.publication_stats.maintenance_source_turns <= source_turns) return error.TestConsumerlessCatalogChangeNotServiced;
    // Detached publication can leave the serving writer on an older header.
    // Allocator inspection needs current metadata, under admission; a cached
    // logical source cut does not retain that checkpoint's allocator pages.
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const refreshed = try owner.refresh();
    const current_file = &refreshed.owned_lite_backend.?.native_docstore.?.file;
    const current_check = try current_file.check();
    if (!current_check.valid) return error.TestConsumerlessIntegrityFailed;
}

test "capi native group discovery releases admission and rebases individual commands" {
    if (h.builtin.single_threaded or h.builtin.os.tag == .freestanding) return error.SkipZigTest;
    var fixture = try AdmissionFollowupFixture.init("group-discovery-command-admission", 1000, .batch);
    defer fixture.deinit();
    const owner = fixture.connection.owner;
    {
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try owner.refresh();
        try root.db.addEnrichment(.{ .name = "turns", .kind = .group, .source = .{ .group_by = "thread_id", .order_by = "ordinal", .segment_by = "turn_id", .field = "text" } });
        var txn = try root.db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const prefix = "\x00\x00__group_cohort_cursor:";
        try txn.put(prefix ++ "a", "first");
        try txn.put(prefix ++ "b", "second");
        const view = @import("../storage/db/group_source_mutations.zig").Reader.fromTxn(&txn);
        var cursor = try view.cursor_fn(view.ptr, std.testing.allocator);
        {
            defer cursor.close();
            try std.testing.expectEqualStrings(prefix ++ "a", (try cursor.seekAtOrAfter(prefix)).?.key);
            try std.testing.expectEqualStrings(prefix ++ "b", (try cursor.next()).?.key);
        }
        // Closing a borrowed cursor must not abort its cohort transaction.
        try txn.put(prefix ++ "c", "third");
        try txn.commit();
    }
    try fixture.writeReplaySource(.{ .writes = &.{ .{ .key = "message", .value = "{\"thread_id\":\"thread\",\"turn_id\":\"turn\",\"ordinal\":1,\"text\":\"History episode\"}" }, .{ .key = "other-message", .value = "{\"thread_id\":\"thread\",\"turn_id\":\"turn\",\"ordinal\":2,\"text\":\"Other episode\"}" } }, .sync_level = .write });
    const Probe = struct {
        fixture: *AdmissionFollowupFixture,
        called: bool = false,
        admission_free: bool = false,
        foreground_status: h.capi.ErrorCode = .internal,
        source_edited: bool = false,
        first_publish_revision: u64 = 0,
        source_edit_failure: ?anyerror = null,
        fn beforeCommand(raw: *anyopaque, command: @import("../storage/db/group_source_contract.zig").Command) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (command.action != .publish or self.source_edited) return;
            self.source_edited = true;
            self.first_publish_revision = command.publish.?.held.lease.revision;
            self.fixture.writeReplaySource(.{ .writes = &.{.{ .key = "message", .value = "{\"thread_id\":\"thread\",\"turn_id\":\"turn\",\"ordinal\":1,\"text\":\"Replaced episode\"}" }}, .sync_level = .write }) catch |err| {
                self.source_edit_failure = err;
            };
        }
        fn notify(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.called) return;
            self.called = true;
            self.admission_free = self.fixture.connection.gate.mutex.tryLock();
            if (!self.admission_free) return;
            self.fixture.connection.gate.mutex.unlock(h.handleLockIo());
            // Move the primary tip after service capture, before discovery and
            // its first command. Command replay sequence and CAS observations
            // must be based on the new cut, preserving this independent write.
            self.foreground_status = self.fixture.call(.batch);
        }
    };
    var probe: Probe = .{ .fixture = &fixture };
    owner.test_before_group_source = .{ .ptr = &probe, .notify = Probe.notify };
    defer owner.test_before_group_source = null;
    for (0..64) |_| {
        _ = try owner.maintenanceTurn();
        if (probe.called) {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const root = try owner.refresh();
            if ((try owner.groupSourceStateNamed(&root.db, "default")).submitted.load(.monotonic) != 0) break;
        }
    }
    try std.testing.expect(probe.called);
    try std.testing.expect(probe.admission_free);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, probe.foreground_status);
    owner.test_before_group_command = .{ .ptr = &probe, .notify = Probe.beforeCommand };
    defer owner.test_before_group_command = null;
    var replacement_published = false;
    for (0..256) |_| {
        {
            var lease = try fixture.connection.acquire();
            defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
            defer lease.close();
            const root = try owner.refresh();
            const state = try owner.groupSourceStateNamed(&root.db, "default");
            _ = state;
            owner.maintenance_schedules.getPtr(.{ .table_id = root.db.core.identity_namespace.table_id, .incarnation = root.db.root_incarnation }).?.next_group_source_ns = 0;
        }
        _ = try owner.maintenanceTurn();
        if (!probe.source_edited) continue;
        if (probe.source_edit_failure) |err| return err;
        var lease = try fixture.connection.acquire();
        defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
        defer lease.close();
        const root = try owner.refresh();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const definitions = (try root.db.lookup(a, "", .{ .group_source_query_json = "{\"action\":\"definitions\"}" })).?;
        const entries = try std.json.parseFromSliceLeaky([]@import("../storage/db/group_source_worker.zig").Definition, a, definitions.json, .{});
        for (entries) |entry| {
            if (entry.retirement) continue;
            const request = try std.json.Stringify.valueAlloc(a, @import("../storage/db/group_source_contract.zig").Query{ .action = .status, .artifact = entry.artifact, .definition = entry.definition, .group = "thread", .segment = "turn", .cell = 0 }, .{});
            const response = (try root.db.lookup(a, "", .{ .group_source_query_json = request })).?;
            const status = try std.json.parseFromSliceLeaky(struct { snapshot: ?@import("../storage/db/group_source_state.zig").Snapshot = null }, a, response.json, .{ .ignore_unknown_fields = true });
            if (status.snapshot) |snapshot| for (snapshot.units) |unit| {
                const key = try keys.documentKeyAlloc(a, unit);
                const payload = try root.db.core.store.get(a, key);
                // The stale fan-in must never become visible, including in
                // the interval before the replacement event is invalidated.
                try std.testing.expect(std.mem.indexOf(u8, payload, "History episode") == null);
                if (std.mem.indexOf(u8, payload, "Replaced episode") != null) {
                    try std.testing.expect(std.mem.indexOf(u8, payload, "Other episode") != null);
                    replacement_published = true;
                }
            };
        }
        if (replacement_published) break;
    }
    try std.testing.expect(probe.source_edited);
    try std.testing.expect(probe.first_publish_revision >= 2);
    try std.testing.expect(replacement_published);
    try std.testing.expect(owner.publication_stats.group_source_control_batches != 0);
    try std.testing.expect(owner.publication_stats.group_source_control_commands > owner.publication_stats.group_source_control_batches);
    try std.testing.expect(owner.publication_stats.group_source_control_commits <= owner.publication_stats.group_source_control_batches);
    var lease = try fixture.connection.acquire();
    defer fixture.connection.gate.mutex.unlock(h.handleLockIo());
    defer lease.close();
    const root = try owner.refresh();
    try std.testing.expect((try owner.groupSourceStateNamed(&root.db, "default")).submitted.load(.monotonic) != 0);
    var row = (try root.db.getDocument(std.testing.allocator, "accepted", .{})).?;
    defer row.deinit(std.testing.allocator);
    try std.testing.expect((try root.owned_lite_backend.?.native_docstore.?.file.check()).valid);
}
