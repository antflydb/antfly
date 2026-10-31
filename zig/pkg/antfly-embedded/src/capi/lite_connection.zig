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

/// A small proof over the existing immutable native metadata records. The
/// captured read pin prevents page reuse until the detached turn ends. Stable
/// record references therefore prove identical content without reading a large
/// catalog, even as unrelated document commits replace B-tree index leaves.
/// Rewritten records use exact content comparison as a conservative slow path.
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
    pin: Txn,
    inode: u64,
    paths: [metadata_keys.len][]const u8,
    references: [metadata_keys.len]?u64,

    fn capture(a: std.mem.Allocator, root: *h.Handle, database: *h.db_mod.DB) !PublicationCatalog {
        const backend = &root.owned_lite_backend.?;
        const store = backend.native_docstore.?;
        var pin = try store.beginRead();
        errdefer pin.abort();
        const prefix = if (database == &root.db) "" else prefix: {
            const namespace = try @import("../storage/lite/embedded_catalog.zig").namespaceAlloc(a, database.core.identity_namespace.table_id);
            break :prefix try backend.documentPrefixAlloc(a, namespace);
        };
        var result: PublicationCatalog = .{ .pin = pin, .inode = (try (try pin.readFile()).file.stat((try pin.readFile()).runtime())).inode, .paths = undefined, .references = undefined };
        for (metadata_keys, 0..) |key, i| {
            result.paths[i] = try std.mem.concat(a, u8, &.{ prefix, key });
            result.references[i] = try result.pin.valueIdentity(result.paths[i]);
        }
        return result;
    }
    fn verify(self: *PublicationCatalog, root: *h.Handle) !void {
        var live = try root.owned_lite_backend.?.native_docstore.?.beginRead();
        defer live.abort();
        const file = try live.readFile();
        if ((try file.file.stat(file.runtime())).inode != self.inode) return error.EnrichmentSourceChanged;
        for (self.paths, self.references) |path, captured| {
            if (try live.valueIdentity(path) == captured) continue;
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            const before = self.pin.getOwned(scratch.allocator(), path) catch |err| if (err == error.NotFound) null else return err;
            const after = live.getOwned(scratch.allocator(), path) catch |err| if (err == error.NotFound) null else return err;
            if ((before == null) != (after == null)) return error.EnrichmentSourceChanged;
            if (before) |value| if (!std.mem.eql(u8, value, after.?)) return error.EnrichmentSourceChanged;
        }
    }
    fn deinit(self: *PublicationCatalog) void {
        self.pin.abort();
    }
};

// Completed producer bytes are delivered to the current runtime, rather than
// replaying inference whenever an unrelated primary commit moves the file tip.
pub const ShadowScope = struct { name: []const u8, generation: u64, prefix: []const u8 };

pub const PreparedPublication = struct {
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
    relational_rows: bool,
    index_inputs_changed: bool = false,
    resolver_dependency: ?struct { table: []const u8, template: []const u8, table_id: ?u64, head: ?@import("resolver_authority.zig").Stamp } = null,

    fn current(self: *@This()) !*h.db_mod.DB {
        const root = try self.owner.refresh();
        if (Owner.needsOutcomeReconciliation(root)) return error.EnrichmentWaitCanceled;
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

    fn commit(ptr: *anyopaque, batch: GeneratedBatch, promotions: Promotions, deletes: []const []const u8, guards: SourceGuards, fence: ?WriteFence) !@typeInfo(@typeInfo(std.meta.Child(@FieldType(PublicationPort, "commit"))).@"fn".return_type.?).error_union.payload {
        const self: *@This() = @ptrCast(@alignCast(ptr));
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
        const result = database.publishPreparedGeneratedBatch(batch, promotions, deletes, guards, fence) catch |err| switch (err) {
            error.EnrichmentSourceChanged, error.EnrichmentLeaseFenceLost => return error.EnrichmentWaitCanceled,
            else => return err,
        };
        self.owner.notifyMutation();
        return result;
    }

    const HeldStore = struct {
        publication: *PreparedPublication,
        path_lease: native.PathWriterLock,
        database: *h.db_mod.DB,
        refusal_recorded: bool = false,

        fn release(ptr: *anyopaque) void {
            const held: *@This() = @ptrCast(@alignCast(ptr));
            const p = held.publication;
            p.refreshSnapshotCheckpoint();
            held.path_lease.close();
            p.owner.gate.mutex.unlock(h.handleLockIo());
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

    fn guardDocument(self: *@This(), database: *h.db_mod.DB, doc_key: []const u8) !void {
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
    // Owner lifetime, rather than DB-runtime lifetime: snapshot refreshes must
    // not reset a table's replay/construction alternation.
    maintenance_schedules: std.AutoHashMapUnmanaged(MaintenanceKey, MaintenanceSchedule) = .empty,
    maintenance_root_incarnation: ?u128 = null,
    wake: std.Io.Event = .unset,
    current: ?*h.Handle = null,
    retired: std.ArrayList(*h.Handle) = .empty,
    maintenance_snapshot: ?*h.Handle = null,
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
    } = .{},
    test_runtime_snapshot_opens: if (h.builtin.is_test) u64 else void = if (h.builtin.is_test) 0 else {},
    embedding_memo: ?memo_mod.QueryEmbeddingCache = null,
    embedding_memo_budget: cache_budget.CacheBudget = .init(embedding_memo_bytes),
    test_after_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *h.Handle) void } else void = if (h.builtin.is_test) null else {},
    test_after_allocator_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *native.NativeFile) void } else void = if (h.builtin.is_test) null else {},
    test_before_allocator_reader_close: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *native.NativeFile) void } else void = if (h.builtin.is_test) null else {},
    test_allocator_wait: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},
    test_after_stop: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},
    test_before_maintenance_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque) void } else void = if (h.builtin.is_test) null else {},

    /// Caller holds the path lease, so the checkpoint and catalog cannot race
    /// a different process's publication. Cursor continuation deliberately
    /// uses its pinned generation without reopening the connection.
    pub fn refresh(self: *Owner) !*h.Handle {
        var known_maintenance: ?*h.Handle = null;
        if (self.current) |current| {
            if (current.lite_scheduling_owner == null) try self.bindGroupSourceState(current);
            var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
            defer probe.close();
            const file = &current.owned_lite_backend.?.native_docstore.?.file;
            const same_inode = (try probe.file.stat(probe.runtime())).inode == (try file.file.stat(file.runtime())).inode;
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
            if (try certified.?.inheritAllocatorCertificate()) {
                options.allocator_verification_owner = &certified.?;
            } else if (certified.?.header.indexed_reclamation) {
                retained_turn = try self.retainMaintenanceTurnAssumeLocked();
                if (!retained_turn) return error.FileBusy;
            }
        }
        const replacement = try api.openLiteHandleAllocWithSharedInference(alloc, self.path, options, false, h.handleLockIo(), null, self.current);
        errdefer h.closeHandle(replacement);
        try self.bindGroupSourceState(replacement);
        if (self.current) |previous| {
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

    fn loadedIndexInputsUnchanged(root: *h.Handle, before: *native.NativeFile, after: *native.NativeFile) !bool {
        if (before.activeCheckpoint().index_catalog_root_page == after.activeCheckpoint().index_catalog_root_page) return true;
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

    pub fn notifyMutation(self: *Owner) void {
        self.allocator_certificate_pending = true;
        if (self.worker == null) return;
        self.maintenance_pending = true;
        self.wake.set(h.handleLockIo());
    }

    /// Caller holds the owner gate. Captures and validated publications need
    /// turns alongside foreground writers, otherwise a continuously occupied
    /// kernel queue prevents index replay from ever acquiring its lease.
    /// Reclamation and detached-proof reacquisition remain nonblocking.
    fn acquireMaintenancePathLeaseAssumeLocked(self: *Owner) !native.PathWriterLock {
        if (self.stopping.load(.acquire)) return error.Canceled;
        const io = h.handleLockIo();
        if (native.lockWriterPathWithIo(alloc, io, self.path)) |held| return held else |err| {
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
            return held;
        }
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
        var lease = (if (scope == .reclamation) native.lockWriterPathWithIo(alloc, h.handleLockIo(), self.path) else self.acquireMaintenancePathLeaseAssumeLocked()) catch |err| switch (err) {
            error.FileBusy, error.WouldBlock => return error.EnrichmentWaitCanceled,
            else => return err,
        };
        errdefer lease.close();
        var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
        defer probe.close();
        const file = &root.owned_lite_backend.?.native_docstore.?.file;
        if ((try probe.file.stat(probe.runtime())).inode != (try file.file.stat(file.runtime())).inode) return error.EnrichmentWaitCanceled;
        if (!std.meta.eql(probe.header, file.header)) {
            if (scope == .reclamation) return error.EnrichmentWaitCanceled;
            const publication = root.maintenance_publication orelse return error.EnrichmentWaitCanceled;
            _ = try publication.current();
            // Rebase before opening a primary transaction as well as before
            // publishing immutable index bytes. Lease acquisition and replay
            // checkpoints must not require a quiet interval between capture
            // and publication. Unchanged physical index inputs and catalog
            // identity keep detached computations valid; transactions opened
            // after this admission read the current primary checkpoint.
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
        // A changed tip invalidates a detached proof. Retry outside the gate
        // instead of falling back to a whole-file scan in this publication.
        if ((scope == .primary or scope == .index) and !file.free_pages_verified) {
            self.publication_stats.allocator_refusals +|= 1;
            return error.EnrichmentWaitCanceled;
        }
        root.maintenance_writer_lease = lease;
        root.maintenance_writer_references = 1;
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

    fn inheritReclamationTelemetry(destination: anytype, source: anytype) void {
        const stats = &destination.maintenance_policy.status;
        const previous = source.maintenance_policy.status;
        if (previous.rewrite_count > stats.rewrite_count) stats.last_reclaimed_bytes = previous.last_reclaimed_bytes;
        stats.assessment_count = @max(stats.assessment_count, previous.assessment_count);
        stats.rewrite_count = @max(stats.rewrite_count, previous.rewrite_count);
    }

    fn refreshMaintenanceSnapshot(self: *Owner, foreground: *h.Handle, need_database: bool) !*h.Handle {
        const file = &foreground.owned_lite_backend.?.native_docstore.?.file;
        if (self.maintenance_snapshot) |snapshot| {
            const captured = &snapshot.owned_lite_backend.?.native_docstore.?.file;
            if ((!need_database or snapshot.db_live) and std.meta.eql(file.header, captured.header) and
                (try file.file.stat(file.runtime())).inode == (try captured.file.stat(captured.runtime())).inode) return snapshot;
        }
        var options = self.options;
        options.externally_locked = true;
        options.artifact_reader_fence = true;
        options.allocator_verification_owner = file;
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
        if (self.options.open_mode == .writer and try hasPendingReplay(database)) self.maintenance_pending = true;
        var authority_arena = std.heap.ArenaAllocator.init(alloc);
        defer authority_arena.deinit();
        if (self.options.open_mode == .writer and try @import("resolver_authority.zig").hasProjectionDebt(foreground, database, authority_arena.allocator())) self.maintenance_pending = true;
        const group_state = try self.groupSourceState(database);
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
        const root = if (independent) try self.refreshMaintenanceSnapshot(foreground, run_database) else foreground;
        if (independent and run_database) {
            database = if (selected) |table| try @import("tables.zig").get(root, table.name) else &root.db;
            // Only construction uses inode-fenced reads. Sources opened by
            // mutation/publication retain the normal immutable-range leases.
            root.owned_lite_backend.?.native_docstore.?.artifact_reader_fence = false;
        }
        const publication_name = try alloc.dupe(u8, if (selected) |table| table.name else "default");
        defer alloc.free(publication_name);
        const index_prefix = try std.fmt.allocPrint(alloc, "{s}/", .{database.core.index_manager.base_path});
        defer alloc.free(index_prefix);
        var source_read = if (run_database) try database.core.store.runtime_store.beginRead() else null;
        defer if (source_read) |*read| read.abort();
        var catalog = if (run_database) try PublicationCatalog.capture(turn_alloc, root, database) else null;
        defer if (catalog) |*proof| proof.deinit();
        var publication: PreparedPublication = .{ .owner = self, .name = publication_name, .identity = database.core.identity_namespace, .incarnation = database.root_incarnation, .catalog = if (catalog) |*proof| proof else null, .index_prefix = index_prefix, .shadow_scopes = shadow_scopes.items, .snapshot_root = root, .snapshot_database = database, .source_read = if (source_read) |*read| read else null, .relational_rows = if (database.enrichment_runtime) |runtime| runtime.config.relational_base_rows else false };
        root.maintenance_references += 1;
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
        if (run_database) database.async_context.replay_publication = .{ .ptr = &publication, .publish = PreparedPublication.publishReplay };
        store.writer_admission = .{ .ptr = root, .enter = enterMaintenanceWriter, .leave = leaveMaintenanceWriter };
        lease.close();
        lease_held = false;
        self.gate.mutex.unlock(io);
        gate_locked = false;

        if (comptime h.builtin.is_test) if (self.test_after_capture) |hook| hook.notify(hook.ptr, root);
        const cancellation = h.db_mod.types.CancellationToken.fromAtomic(&self.stopping);
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
        var pending = if (maintenance_error != null) true else if (run_database and !projected) database.runBackgroundTurnWithCancellation(cancellation, shadow_scopes.items.len != 0) catch |err| blk: {
            maintenance_error = err;
            break :blk true;
        } else projected;
        if (run_database) if (database.enrichment_runtime) |runtime| {
            runtime.dense_result_cache = null;
            runtime.generated_publication = null;
        };
        if (run_database) database.async_context.replay_publication = null;
        root.maintenance_publication = null;
        if (source_read) |*read| read.abort();
        source_read = null;
        if (independent and root.db_live) {
            // End the input fences before reclamation. Keeping them in an
            // idle runtime would needlessly prevent in-place page service.
            @import("tables.zig").closeAll(root);
            root.embedded_tables = .empty;
            root.db.closeImmutableSnapshot();
            root.db_live = false;
        }
        store.maintainOnce(false) catch |err| {
            if (maintenance_error == null) maintenance_error = err;
            pending = true;
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
        // The raw native pin borrows this snapshot's store. Release it before
        // collectRetired can destroy a superseded hosted/serving root.
        if (catalog) |*proof| proof.deinit();
        catalog = null;
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
        return self.maintenance_storage_pending or self.maintenance_pending or self.maintenance_table_cursor != 0 or changed or ttl_pending;
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
        const root = try self.refreshMaintenanceSnapshot(foreground, false);
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
        const result = store.maintainOnce(false);
        self.gate.mutex.lockUncancelable(io);
        gate_locked = true;
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
                self.publication_stats.maintenance_errors +|= 1;
                self.publication_stats.last_maintenance_error = @errorName(err);
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
        return if (self.maintenance_storage_pending or self.maintenance_table_cursor != 0) 0 else if (pending) 100 else 1000;
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
        lease.* = if (connection) |caller| try caller.acquirePathLeaseAssumeLocked() else try native.lockWriterPathWithIo(alloc, io, self.path);
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
            if (lease) |held| return held else |err| {
                if (err != error.WouldBlock and err != error.FileBusy) return err;
                const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
                if (remaining <= 0) return error.FileBusy;
                if (comptime h.builtin.is_test) if (self.test_path_lease_busy) |hook| hook.notify(hook.ptr);
                if (native.waitPathLockWithIo(alloc, self.owner.path_lock_io.io(), self.path, h.liteOpenModeCanWrite(self.options.open_mode), deadline)) |held| return held else |wait_err| switch (wait_err) {
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
    try std.testing.expectEqual(@as(u32, 0), proof.pin.snapshot_reads.count());
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
