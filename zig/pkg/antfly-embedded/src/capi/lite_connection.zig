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

fn catalogStamp(database: *h.db_mod.DB) ![32]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var hash = std.crypto.hash.Blake3.init(.{});
    if (try database.getSchemaJson(a)) |schema| hash.update(schema);
    const indexes = try database.core.listIndexes(a);
    std.mem.sort(@TypeOf(indexes[0]), indexes, {}, struct {
        fn less(_: void, lhs: @TypeOf(indexes[0]), rhs: @TypeOf(indexes[0])) bool {
            return std.mem.lessThan(u8, lhs.name, rhs.name);
        }
    }.less);
    hash.update(try std.json.Stringify.valueAlloc(a, indexes, .{}));
    const resolvers = try database.core.listResolvers(a);
    std.mem.sort(@TypeOf(resolvers[0]), resolvers, {}, struct {
        fn less(_: void, lhs: @TypeOf(resolvers[0]), rhs: @TypeOf(resolvers[0])) bool {
            return std.mem.lessThan(u8, lhs.name, rhs.name);
        }
    }.less);
    hash.update("resolvers\x00");
    hash.update(try std.json.Stringify.valueAlloc(a, resolvers, .{}));
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

// Completed producer bytes are delivered to the current runtime, rather than
// replaying inference whenever an unrelated primary commit moves the file tip.
pub const ShadowScope = struct { name: []const u8, generation: u64, prefix: []const u8 };

pub const PreparedPublication = struct {
    owner: *Owner,
    name: []const u8,
    identity: std.meta.Child(@FieldType(h.db_mod.OpenOptions, "identity_namespace")),
    incarnation: @FieldType(h.db_mod.DB, "root_incarnation"),
    catalog: [32]u8,
    index_prefix: []const u8,
    shadow_scopes: []const ShadowScope = &.{},
    snapshot_root: *h.Handle,
    snapshot_database: *h.db_mod.DB,
    source_read: ?*SourceRead,
    relational_rows: bool,
    index_inputs_changed: bool = false,

    fn current(self: *@This()) !*h.db_mod.DB {
        const root = try self.owner.refresh();
        if (Owner.needsOutcomeReconciliation(root)) return error.EnrichmentWaitCanceled;
        try @import("sql_commit.zig").recover(root);
        try @import("sql_ddl.zig").recover(root);
        const database = try @import("tables.zig").get(root, self.name);
        if (database.root_incarnation != self.incarnation or !std.meta.eql(database.core.identity_namespace, self.identity) or
            !std.mem.eql(u8, &(try catalogStamp(database)), &self.catalog)) return error.EnrichmentSourceChanged;
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
        var lease = try native.lockWriterPathWithIo(alloc, io, self.owner.path);
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
        var lease = try native.lockWriterPathWithIo(alloc, io, self.owner.path);
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
        var lease = try native.lockWriterPathWithIo(alloc, io, self.owner.path);
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
        var path_lease = try native.lockWriterPathWithIo(alloc, io, self.owner.path);
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
    ready: std.Io.Condition = .init,
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
    references: usize = 1,
    owners: std.ArrayList(*Owner) = .empty,
};
var registry_mutex: std.Io.Mutex = .init;
var gates: std.StringHashMapUnmanaged(*PathGate) = .empty;
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
    const MaintenanceKey = struct { table_id: u64, incarnation: u128 };
    const MaintenanceSchedule = struct {
        shadow_next: bool = true,
        source_recovery_next: bool = true,
        group_source_next: bool = true,
        // Separately allocated: table admission can grow the schedule map
        // while a visibility/idle drain borrows this state.
        group_sources: ?*h.db_mod.DB.GroupSourceMaintenanceState = null,
        next_source_recovery_ns: i96 = 0,
        live: bool = true,
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
    stopping: std.atomic.Value(bool) = .init(false),
    maintenance_pending: bool = false,
    // Owner lifetime, rather than DB-runtime lifetime: snapshot refreshes must
    // not reset a table's replay/construction alternation.
    maintenance_schedules: std.AutoHashMapUnmanaged(MaintenanceKey, MaintenanceSchedule) = .empty,
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
        attempts: u64 = 0,
        refused: u64 = 0,
        rebased: u64 = 0,
        generated_attempts: u64 = 0,
        generated_refused: u64 = 0,
        replay_attempts: u64 = 0,
        replay_refused: u64 = 0,
        maintenance_errors: u64 = 0,
        last_maintenance_error: ?[]const u8 = null,
    } = .{},
    test_runtime_snapshot_opens: if (h.builtin.is_test) u64 else void = if (h.builtin.is_test) 0 else {},
    embedding_memo: ?memo_mod.QueryEmbeddingCache = null,
    embedding_memo_budget: cache_budget.CacheBudget = .init(embedding_memo_bytes),
    test_after_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *h.Handle) void } else void = if (h.builtin.is_test) null else {},

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
            if (current.maintenance_references == 0 and same_inode and std.meta.eql(probe.header, file.header)) return current;
            if (current.maintenance_references == 0 and same_inode and !needsOutcomeReconciliation(current) and
                try file.runtimeMetadataUnchanged(&probe) and try loadedIndexInputsUnchanged(current, file, &probe))
            {
                // Lease bookkeeping and unpublished shadow files do not change
                // a serving runtime. Reopening it would publish another lease
                // for every immutable input while foreground traffic waits.
                try current.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(probe.header);
                if (self.maintenance_snapshot) |snapshot| try file.inheritAllocatorVerification(&snapshot.owned_lite_backend.?.native_docstore.?.file);
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
            try self.retired.append(alloc, previous);
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
                if (retired.query_borrows > 0 or retired.maintenance_references != 0) break :retained true;
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
        if (self.worker == null) return;
        self.maintenance_pending = true;
        self.wake.set(h.handleLockIo());
    }

    fn hasPendingReplay(database: *h.db_mod.DB) !bool {
        if (database.hasPendingGraphReplacements()) return true;
        const target = database.core.nextDerivedSequence();
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
        if (scope == .index) if (root.maintenance_publication) |p| if (p.index_inputs_changed) return error.EnrichmentWaitCanceled;
        if (self.current) |current| if (needsOutcomeReconciliation(current)) return error.EnrichmentWaitCanceled;
        var lease = native.lockWriterPathWithIo(alloc, h.handleLockIo(), self.path) catch |err| switch (err) {
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
                    if (scope == .index) return error.EnrichmentWaitCanceled;
                }
            }
            try root.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(latest.header);
            if (self.current) |current| try file.inheritAllocatorVerification(&current.owned_lite_backend.?.native_docstore.?.file);
            self.publication_stats.rebased +|= 1;
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
            const opened = try alloc.create(h.Handle);
            opened.* = .{ .alloc = alloc, .db = undefined, .db_live = false, .owned_lite_backend = backend, .open_mode = options.open_mode };
            break :blk opened;
        };
        snapshot.lease_snapshot = true;
        snapshot.sql_commit_recovery_needed = foreground.sql_commit_recovery_needed;
        snapshot.sql_ddl_recovery_needed = foreground.sql_ddl_recovery_needed;
        const store = snapshot.owned_lite_backend.?.native_docstore.?;
        try store.file.inheritAllocatorVerification(file);
        if (self.maintenance_snapshot) |old| {
            try store.file.inheritAllocatorVerification(&old.owned_lite_backend.?.native_docstore.?.file);
            // Policy scheduling and telemetry belong to this connection;
            // assessment caches and page roots belong to the new generation.
            store.maintenance_policy = old.owned_lite_backend.?.native_docstore.?.maintenance_policy;
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
    fn adoptIdleMaintenanceCheckpoint(self: *Owner, foreground: *h.Handle, entry_header: native.Header, maintenance: *NativeStore) !void {
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
        // This turn ran no DB work. Reclamation admission never rebases over
        // external commits, so only lease metadata and allocator state may
        // differ. Primary, namespace and immutable index roots must all match.
        if (!std.meta.eql(before, after) or store.file.header.page_size != probe.header.page_size or
            store.file.header.packed_records != probe.header.packed_records or
            store.file.header.indexed_reclamation != probe.header.indexed_reclamation) return;
        try store.refreshIndexPublicationCheckpoint(probe.header);
        try store.file.inheritAllocatorVerification(&maintenance.file);
    }

    /// Captures one table under the connection fence. The pinned runtime owns
    /// all provider/catalog state while computation runs outside that fence.
    /// Native writer admission below revalidates inode and checkpoint for every
    /// short publication, including staged artifacts and index segments.
    fn maintenanceTurn(self: *Owner) !bool {
        const io = h.handleLockIo();
        if (self.stopping.load(.acquire)) return false;
        // Yield briefly to foreground work, then join the fair queue. Capture
        // stays short; queued traffic must not indefinitely postpone replay/TTL.
        if (!acquireMaintenanceCapture(&self.gate.mutex, &self.maintenance_capture_misses)) return true;
        var gate_locked = true;
        defer if (gate_locked) self.gate.mutex.unlock(io);
        var lease = try native.lockWriterPathWithIo(alloc, io, self.path);
        var lease_held = true;
        defer if (lease_held) lease.close();
        const foreground = try self.refresh();
        if (needsOutcomeReconciliation(foreground)) return false;
        try @import("sql_commit.zig").recover(foreground);
        try @import("sql_ddl.zig").recover(foreground);
        try @import("tables.zig").loadAvailable(foreground);
        if (self.options.open_mode == .writer) {
            if (try hasPendingReplay(&foreground.db)) self.maintenance_pending = true;
            var tables = foreground.embedded_tables.valueIterator();
            while (tables.next()) |table| {
                if (try hasPendingReplay(&table.*.db)) self.maintenance_pending = true;
            }
        }
        // Prune dropped/recreated namespaces without retaining names or old
        // runtimes. IDs never repeat; the root incarnation also fences file
        // replacement. This remains bounded by the current table catalog.
        var schedules = self.maintenance_schedules.valueIterator();
        while (schedules.next()) |schedule| schedule.live = false;
        if (self.maintenance_schedules.getPtr(.{ .table_id = foreground.db.core.identity_namespace.table_id, .incarnation = foreground.db.root_incarnation })) |schedule| schedule.live = true;
        var live_tables = foreground.embedded_tables.valueIterator();
        while (live_tables.next()) |table| {
            if (self.maintenance_schedules.getPtr(.{ .table_id = table.*.db.core.identity_namespace.table_id, .incarnation = table.*.db.root_incarnation })) |schedule| schedule.live = true;
        }
        var stale = self.maintenance_schedules.iterator();
        while (stale.next()) |entry| if (!entry.value_ptr.live) {
            if (entry.value_ptr.group_sources) |state| {
                state.deinit();
                alloc.destroy(state);
            }
            self.maintenance_schedules.removeByPtr(entry.key_ptr);
        };
        const count = foreground.embedded_tables.count() + 1;
        const ordinal = self.maintenance_table_cursor % count;
        var database = &foreground.db;
        var selected: ?*@import("tables.zig").Table = null;
        if (ordinal != 0) {
            var tables = foreground.embedded_tables.valueIterator();
            for (0..ordinal) |_| selected = tables.next().?.*;
            database = &selected.?.db;
        }
        const group_state = try self.groupSourceState(database);
        const schedule_entry = try self.maintenance_schedules.getOrPut(alloc, .{
            .table_id = database.core.identity_namespace.table_id,
            .incarnation = database.root_incarnation,
        });
        if (!schedule_entry.found_existing) schedule_entry.value_ptr.* = .{};
        const schedule = schedule_entry.value_ptr;
        self.maintenance_table_cursor = (ordinal + 1) % count;
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
        var publication: PreparedPublication = .{ .owner = self, .name = publication_name, .identity = database.core.identity_namespace, .incarnation = database.root_incarnation, .catalog = if (run_database) try catalogStamp(database) else @splat(0), .index_prefix = index_prefix, .shadow_scopes = shadow_scopes.items, .snapshot_root = root, .snapshot_database = database, .source_read = if (source_read) |*read| read else null, .relational_rows = if (database.enrichment_runtime) |runtime| runtime.config.relational_base_rows else false };
        root.maintenance_references += 1;
        root.maintenance_owner = self;
        root.maintenance_publication = &publication;
        const store = root.owned_lite_backend.?.native_docstore.?;
        const foreground_header = foreground.owned_lite_backend.?.native_docstore.?.file.header;
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
        // A shadow owns disjoint unpublished index inputs. Give it a bounded
        // turn before replay, whose serving-index publication invalidates the
        // broad capture. Unrelated serving writes may rebase this turn, while
        // changes to any selected shadow generation still reject publication.
        var pending = if (run_database) database.runBackgroundTurnWithCancellation(cancellation, shadow_scopes.items.len != 0) catch |err| blk: {
            maintenance_error = err;
            break :blk true;
        } else false;
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
        store.maintainOnce(false) catch {
            pending = true;
        };

        self.gate.mutex.lockUncancelable(io);
        defer self.gate.mutex.unlock(io);
        if (maintenance_error) |err| {
            self.publication_stats.maintenance_errors +|= 1;
            self.publication_stats.last_maintenance_error = @errorName(err);
        } else self.publication_stats.last_maintenance_error = null;
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
        if (independent and !run_database) try self.adoptIdleMaintenanceCheckpoint(foreground, foreground_header, store);
        // A moved physical tip requires a prompt refresh, not another DB
        // replay pass. Reader leases/index housekeeping also move that tip;
        // treating them as enrichment debt makes idle runtime opens perpetual.
        // The next capture checks the durable journal for foreground work.
        // A primary rebase may have fenced immutable index work without
        // making the replay driver itself fail. Retry on a fresh snapshot
        // even when its cached replay target looked complete this turn.
        pending = pending or publication.index_inputs_changed;
        self.maintenance_round_pending = self.maintenance_round_pending or pending;
        const ttl_pending = self.maintenance_round_ttl_pending;
        if (self.maintenance_table_cursor == 0) {
            self.maintenance_round_ttl_pending = false;
            // Mutations arriving during the detached turn still wake the owner;
            // replay debt is inspected again under the next capture fence.
            self.maintenance_pending = self.maintenance_round_pending;
            self.maintenance_round_pending = false;
        }
        self.collectRetired();
        return self.maintenance_pending or self.maintenance_table_cursor != 0 or changed or ttl_pending;
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
                // Coalesce mutation wakes while a round is already pending;
                // an event must not bypass its per-table scheduling quantum.
                next_wait_ms = @intCast(@divCeil(earliest_turn_ns - now, std.time.ns_per_ms));
                continue;
            }
            const pending = self.maintenanceTurn() catch true;
            next_wait_ms = if (pending) 100 else 1000;
            earliest_turn_ns = std.Io.Clock.awake.now(io).nanoseconds + if (pending) @as(i96, 100 * std.time.ns_per_ms) else 0;
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

    fn deinit(self: *Owner) void {
        if (self.worker) |worker| worker.join();
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
        if (self.owned_ttl_owner_id) |id| alloc.free(id);
        if (self.owned_models_dir) |directory| alloc.free(directory);
        alloc.destroy(self);
    }
};

fn compatible(a: api.LiteResolvedOpenOptions, b: api.LiteResolvedOpenOptions) bool {
    var left = a;
    var right = b;
    // This controls an individual caller's admission deadline, not storage.
    left.busy_timeout_ms = 0;
    right.busy_timeout_ms = 0;
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
    current: ?*h.Handle = null,
    attached: bool = false,
    sql_sessions: @FieldType(h.Handle, "sql_sessions") = .empty,
    next_sql_session_id: u64 = 1,
    sql_cursors: @FieldType(h.Handle, "sql_cursors") = .empty,
    next_sql_cursor_id: u64 = 1,
    table_handles: @FieldType(h.Handle, "table_handles") = .empty,
    readable_lease_hook: ?h.ReadableLeaseHook = null,

    pub fn acquire(self: *Connection) !native.PathWriterLock {
        const io = h.handleLockIo();
        self.gate.mutex.lockUncancelable(io);
        errdefer self.gate.mutex.unlock(io);
        const deadline = std.Io.Clock.awake.now(io).nanoseconds + @as(i96, self.options.busy_timeout_ms) * std.time.ns_per_ms;
        var backoff: i96 = std.time.ns_per_ms;
        while (true) {
            const lease = if (h.liteOpenModeCanWrite(self.options.open_mode))
                native.lockWriterPathWithIo(alloc, io, self.path)
            else
                native.lockReaderPathWithIo(alloc, io, self.path);
            if (lease) |held| return held else |err| {
                if (err != error.WouldBlock and err != error.FileBusy) return err;
                const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
                if (remaining <= 0) return error.FileBusy;
                try io.sleep(.fromNanoseconds(@min(backoff, remaining)), .awake);
                backoff = @min(backoff * 2, 50 * std.time.ns_per_ms);
            }
        }
    }

    pub fn refresh(self: *Connection) !*h.Handle {
        const root = try self.owner.refresh();
        self.current = root;
        self.attach(root);
        return root;
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
        owner.* = .{ .path = gate.path, .gate = gate, .options = resolved_options, .current = initial, .owned_ttl_owner_id = ttl_owner_id, .owned_models_dir = models_dir };
    }
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
                _ = try owner.refresh();
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
