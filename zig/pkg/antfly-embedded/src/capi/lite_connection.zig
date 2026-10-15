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
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

// Completed producer bytes are delivered to the current runtime, rather than
// replaying inference whenever an unrelated primary commit moves the file tip.
pub const PreparedPublication = struct {
    owner: *Owner,
    name: []const u8,
    identity: std.meta.Child(@FieldType(h.db_mod.OpenOptions, "identity_namespace")),
    incarnation: @FieldType(h.db_mod.DB, "root_incarnation"),
    catalog: [32]u8,
    index_prefix: []const u8,
    snapshot_root: *h.Handle,
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
        return database;
    }

    fn commit(ptr: *anyopaque, batch: GeneratedBatch, promotions: Promotions, deletes: []const []const u8, guards: SourceGuards, fence: ?WriteFence) !@typeInfo(@typeInfo(std.meta.Child(@FieldType(PublicationPort, "commit"))).@"fn".return_type.?).error_union.payload {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const io = h.handleLockIo();
        self.owner.gate.mutex.lockUncancelable(io);
        defer self.owner.gate.mutex.unlock(io);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
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

        fn release(ptr: *anyopaque) void {
            const held: *@This() = @ptrCast(@alignCast(ptr));
            const p = held.publication;
            const snapshot = p.snapshot_root.owned_lite_backend.?.native_docstore.?;
            // Opening the live coordinator can add immutable-reader leases.
            // Rebind primary reads after publication, but remember any changed
            // LSM inputs: adopting a header must never erase that evidence.
            var latest = native.NativeFile.openWithIo(alloc, h.handleLockIo(), p.owner.path, .{ .read_only = true, .internal_reader = true }) catch null;
            if (latest) |*file| {
                defer file.close();
                const live_stat = file.file.stat(file.runtime()) catch null;
                const captured_stat = snapshot.file.file.stat(snapshot.file.runtime()) catch null;
                const same_inode = live_stat != null and captured_stat != null and live_stat.?.inode == captured_stat.?.inode;
                if (same_inode) {
                    if (snapshot.file.activeCheckpoint().index_catalog_root_page != file.activeCheckpoint().index_catalog_root_page) {
                        const unchanged = snapshot.file.indexCatalogPrefixUnchanged(file, p.index_prefix) catch false;
                        if (!unchanged) p.index_inputs_changed = true;
                    }
                    snapshot.refreshIndexPublicationCheckpoint(file.header) catch {
                        p.index_inputs_changed = true;
                    };
                } else p.index_inputs_changed = true;
            } else p.index_inputs_changed = true;
            held.path_lease.close();
            p.owner.gate.mutex.unlock(h.handleLockIo());
            alloc.destroy(held);
        }

        fn guardKey(ptr: *anyopaque, key: []const u8) !void {
            const held: *@This() = @ptrCast(@alignCast(ptr));
            const p = held.publication;
            const doc_key = (try keys.decodeDocumentComponentAlloc(alloc, key)) orelse return;
            defer alloc.free(doc_key);
            const captured_read = p.source_read orelse return error.EnrichmentWaitCanceled;
            var expected = try captured_read.openReadScope(alloc);
            defer expected.close();
            const row_key = if (p.relational_rows) try @import("../storage/db/relational_store.zig").keyAlloc(alloc, doc_key) else try keys.documentKeyAlloc(alloc, doc_key);
            defer alloc.free(row_key);
            const ordinal_key = try keys.identityDocToOrdinalKeyAlloc(alloc, doc_key);
            defer alloc.free(ordinal_key);
            var actual = try held.database.enrichment_runtime.?.store.beginRead();
            defer actual.abort();
            for ([_][]const u8{ row_key, ordinal_key }) |source_key| {
                const before = expected.get(source_key) catch |err| if (err == error.NotFound) null else return err;
                const after = actual.get(source_key) catch |err| if (err == error.NotFound) null else return err;
                if ((before == null) != (after == null)) return error.EnrichmentWaitCanceled;
                if (before) |bytes| if (!std.mem.eql(u8, bytes, after.?)) return error.EnrichmentWaitCanceled;
            }
            if (expected.get(ordinal_key)) |ordinal| {
                if (ordinal.len != 4) return error.EnrichmentWaitCanceled;
                const state_key = keys.identityOrdinalStateKey(std.mem.readInt(u32, ordinal[0..4], .big));
                const before = try expected.get(&state_key);
                const after = try actual.get(&state_key);
                if (!std.mem.eql(u8, before, after)) return error.EnrichmentWaitCanceled;
            } else |err| if (err != error.NotFound) return err;
        }
    };

    fn acquireStore(ptr: *anyopaque, fence: ?WriteFence, now_ms: u64) !StoreLease {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const io = h.handleLockIo();
        self.owner.gate.mutex.lockUncancelable(io);
        errdefer self.owner.gate.mutex.unlock(io);
        if (self.owner.stopping.load(.acquire)) return error.Canceled;
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
    path: []const u8,
    options: api.LiteResolvedOpenOptions,
    owned_ttl_owner_id: ?[]u8 = null,
    gate: *PathGate,
    connections: std.ArrayList(*Connection) = .empty,
    worker: ?std.Thread = null,
    stopping: std.atomic.Value(bool) = .init(false),
    maintenance_pending: bool = false,
    next_ttl_ns: i96 = 0,
    wake: std.Io.Event = .unset,
    current: ?*h.Handle = null,
    retired: std.ArrayList(*h.Handle) = .empty,
    maintenance_snapshot: ?*h.Handle = null,
    maintenance_table_cursor: usize = 0,
    maintenance_round_pending: bool = false,
    maintenance_capture_misses: u8 = 0,
    embedding_memo: ?memo_mod.QueryEmbeddingCache = null,
    embedding_memo_budget: cache_budget.CacheBudget = .init(embedding_memo_bytes),
    test_after_capture: if (h.builtin.is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, *h.Handle) void } else void = if (h.builtin.is_test) null else {},

    /// Caller holds the path lease, so the checkpoint and catalog cannot race
    /// a different process's publication. Cursor continuation deliberately
    /// uses its pinned generation without reopening the connection.
    pub fn refresh(self: *Owner) !*h.Handle {
        var known_maintenance: ?*h.Handle = null;
        if (self.current) |current| {
            var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
            defer probe.close();
            const file = &current.owned_lite_backend.?.native_docstore.?.file;
            const same_inode = (try probe.file.stat(probe.runtime())).inode == (try file.file.stat(file.runtime())).inode;
            if (current.maintenance_references == 0 and same_inode and std.meta.eql(probe.header, file.header)) return current;
            if (self.maintenance_snapshot) |snapshot| {
                const captured = &snapshot.owned_lite_backend.?.native_docstore.?.file;
                if (std.meta.eql(probe.header, captured.header) and
                    (try probe.file.stat(probe.runtime())).inode == (try captured.file.stat(captured.runtime())).inode) known_maintenance = snapshot;
            }
        }
        var options = self.options;
        options.externally_locked = true;
        const replacement = try api.openLiteHandleAllocWithRuntime(alloc, self.path, options, false, h.handleLockIo(), null);
        errdefer h.closeHandle(replacement);
        if (self.current) |previous| {
            if (known_maintenance) |snapshot| {
                replacement.sql_commit_recovery_needed = previous.sql_commit_recovery_needed or snapshot.sql_commit_recovery_needed;
                replacement.sql_ddl_recovery_needed = previous.sql_ddl_recovery_needed or snapshot.sql_ddl_recovery_needed;
            }
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

    pub fn notifyMutation(self: *Owner) void {
        if (self.worker == null) return;
        self.maintenance_pending = true;
        self.wake.set(h.handleLockIo());
    }

    fn hasPendingGeneratedEnrichment(database: *h.db_mod.DB) bool {
        if (database.hasPendingGraphReplacements()) return true;
        const runtime = database.enrichment_runtime orelse return false;
        return database.core.hasGeneratedEnrichmentTargets() and
            runtime.stats().applied_sequence < database.core.nextEnrichmentSequence();
    }

    fn lookupEmbedding(ptr: *anyopaque, a: std.mem.Allocator, key: [32]u8) !?[]f32 {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        return self.embedding_memo.?.lookup(&self.embedding_memo_budget, a, key);
    }

    fn rememberEmbedding(ptr: *anyopaque, key: [32]u8, vector: []const f32) void {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        self.embedding_memo.?.remember(&self.embedding_memo_budget, key, vector);
    }

    // Index publications queue behind foreground operations. Their DB/index
    // locks belong to the detached runtime; foreground refresh opens a separate
    // runtime. Primary/reclamation admission yields on contention.
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
        if (scope == .index) self.gate.mutex.lockUncancelable(h.handleLockIo()) else if (!self.gate.mutex.tryLock()) return error.EnrichmentWaitCanceled;
        errdefer self.gate.mutex.unlock(h.handleLockIo());
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
            if (scope != .index) return error.EnrichmentWaitCanceled;
            const publication = root.maintenance_publication orelse return error.EnrichmentWaitCanceled;
            _ = try publication.current();
            // Foreground refresh may create reader leases. Those are outside
            // the index's physical path and cannot invalidate its LSM inputs.
            var latest = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
            defer latest.close();
            if ((try latest.file.stat(latest.runtime())).inode != (try file.file.stat(file.runtime())).inode) return error.EnrichmentWaitCanceled;
            if (file.activeCheckpoint().index_catalog_root_page != latest.activeCheckpoint().index_catalog_root_page) {
                if (!try file.indexCatalogPrefixUnchanged(&latest, publication.index_prefix)) return error.EnrichmentWaitCanceled;
            }
            try root.owned_lite_backend.?.native_docstore.?.refreshIndexPublicationCheckpoint(latest.header);
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
        self.notifyMutation();
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

    fn refreshMaintenanceSnapshot(self: *Owner, foreground: *h.Handle) !*h.Handle {
        const file = &foreground.owned_lite_backend.?.native_docstore.?.file;
        if (self.maintenance_snapshot) |snapshot| {
            const captured = &snapshot.owned_lite_backend.?.native_docstore.?.file;
            if (snapshot.db_live and std.meta.eql(file.header, captured.header) and
                (try file.file.stat(file.runtime())).inode == (try captured.file.stat(captured.runtime())).inode) return snapshot;
        }
        var options = self.options;
        options.externally_locked = true;
        options.artifact_reader_fence = true;
        const snapshot = try api.openLiteHandleAllocWithRuntime(alloc, self.path, options, false, h.handleLockIo(), null);
        snapshot.lease_snapshot = true;
        snapshot.sql_commit_recovery_needed = foreground.sql_commit_recovery_needed;
        snapshot.sql_ddl_recovery_needed = foreground.sql_ddl_recovery_needed;
        const store = snapshot.owned_lite_backend.?.native_docstore.?;
        if (self.maintenance_snapshot) |old| {
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
            if (hasPendingGeneratedEnrichment(&foreground.db)) self.maintenance_pending = true;
            var tables = foreground.embedded_tables.valueIterator();
            while (tables.next()) |table| {
                if (hasPendingGeneratedEnrichment(&table.*.db)) self.maintenance_pending = true;
            }
        }
        const count = foreground.embedded_tables.count() + 1;
        const ordinal = self.maintenance_table_cursor % count;
        var database = &foreground.db;
        var selected: ?*@import("tables.zig").Table = null;
        if (ordinal != 0) {
            var tables = foreground.embedded_tables.valueIterator();
            for (0..ordinal) |_| selected = tables.next().?.*;
            database = &selected.?.db;
        }
        self.maintenance_table_cursor = (ordinal + 1) % count;
        // TTL is a short publication for the selected table, never a drain of
        // every namespace under one file lease.
        const now = std.Io.Clock.awake.now(io).nanoseconds;
        if (now >= self.next_ttl_ns) {
            if (database.ttl_runtime) |ttl| try ttl.runOnce();
            if (self.maintenance_table_cursor == 0) {
                const config = self.options.ttl_cleanup orelse h.db_mod.ttl_runtime.Config{};
                self.next_ttl_ns = now + @as(i96, config.interval_ms) * std.time.ns_per_ms;
            }
        }
        const run_database = self.maintenance_pending;
        // Native maintenance owns independent catalog/provider state. Opening
        // only its selected namespace avoids rebuilding every foreground table
        // merely because inference or segment construction is still running.
        // Hosted/manual runtimes retain their caller-supplied provider borrow.
        const independent = self.options.profile == .native;
        const root = if (independent) try self.refreshMaintenanceSnapshot(foreground) else foreground;
        if (independent) {
            database = if (selected) |table| try @import("tables.zig").get(root, table.name) else &root.db;
            // Only construction uses inode-fenced reads. Sources opened by
            // mutation/publication retain the normal immutable-range leases.
            root.owned_lite_backend.?.native_docstore.?.artifact_reader_fence = false;
        }
        const publication_name = try alloc.dupe(u8, if (selected) |table| table.name else "default");
        defer alloc.free(publication_name);
        const index_prefix = try std.fmt.allocPrint(alloc, "{s}/", .{database.core.index_manager.base_path});
        defer alloc.free(index_prefix);
        var source_read = if (database.enrichment_runtime) |runtime| try runtime.store.beginRead() else null;
        defer if (source_read) |*read| read.abort();
        var publication: PreparedPublication = .{ .owner = self, .name = publication_name, .identity = database.core.identity_namespace, .incarnation = database.root_incarnation, .catalog = try catalogStamp(database), .index_prefix = index_prefix, .snapshot_root = root, .source_read = if (source_read) |*read| read else null, .relational_rows = if (database.enrichment_runtime) |runtime| runtime.config.relational_base_rows else false };
        root.maintenance_references += 1;
        root.maintenance_owner = self;
        root.maintenance_publication = &publication;
        const store = root.owned_lite_backend.?.native_docstore.?;
        if (database.enrichment_runtime) |runtime| {
            if (self.embedding_memo == null) self.embedding_memo = memo_mod.QueryEmbeddingCache.init(alloc, io, .{ .max_bytes = embedding_memo_bytes, .ttl_ns = std.time.ns_per_min });
            runtime.dense_result_cache = .{ .ptr = self, .lookup = lookupEmbedding, .remember = rememberEmbedding };
            runtime.generated_publication = .{ .ptr = &publication, .commit = PreparedPublication.commit, .heartbeat = PreparedPublication.heartbeat, .acquire_store = PreparedPublication.acquireStore };
        }
        store.writer_admission = .{ .ptr = root, .enter = enterMaintenanceWriter, .leave = leaveMaintenanceWriter };
        lease.close();
        lease_held = false;
        self.gate.mutex.unlock(io);
        gate_locked = false;

        if (comptime h.builtin.is_test) if (self.test_after_capture) |hook| hook.notify(hook.ptr, root);
        const cancellation = h.db_mod.types.CancellationToken.fromAtomic(&self.stopping);
        var pending = if (run_database) database.runBackgroundMaintenanceWithCancellation(cancellation) catch true else false;
        if (database.enrichment_runtime) |runtime| {
            runtime.dense_result_cache = null;
            runtime.generated_publication = null;
        }
        root.maintenance_publication = null;
        if (source_read) |*read| read.abort();
        source_read = null;
        if (independent) {
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
        self.maintenance_round_pending = self.maintenance_round_pending or pending or changed;
        if (self.maintenance_table_cursor == 0) {
            // Mutations arriving during the detached turn still wake the owner;
            // replay debt is inspected again under the next capture fence.
            self.maintenance_pending = self.maintenance_round_pending;
            self.maintenance_round_pending = false;
        }
        self.collectRetired();
        return self.maintenance_pending or self.maintenance_table_cursor != 0;
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
        if (self.owned_ttl_owner_id) |id| alloc.free(id);
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
        owner.* = .{ .path = gate.path, .gate = gate, .options = resolved_options, .current = initial, .owned_ttl_owner_id = ttl_owner_id };
    }
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
    if (existing == null) ttl_owner_id = null;
    return wrapper;
}
