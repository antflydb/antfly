// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Connection lifetimes are independent of writer ownership. A kernel lease
//! covers a complete exported operation (including SQL's commit coordinator).
//! Cached runtimes are immutable to other connections: after an external
//! publication we reopen them, retaining generations borrowed by SQL streams.
const h = @import("handles.zig");
const api = @import("db.zig");
const std = h.std;
const native = h.lite_backend.native;
const alloc = std.heap.c_allocator;

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

// Connections in one process queue on a canonical file gate. The kernel
// sidecar remains the authority between processes and with CLI/server owners.
const PathGate = struct { path: []u8, mutex: QueuedMutex = .{}, references: usize = 1 };
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
    alloc.free(gate.path);
    alloc.destroy(gate);
    if (gates.count() == 0) {
        gates.deinit(alloc);
        gates = .empty;
    }
}

pub const Connection = struct {
    path: []u8,
    options: api.LiteResolvedOpenOptions,
    owned_ttl_owner_id: ?[]u8 = null,
    gate: *PathGate = undefined,
    worker: ?std.Thread = null,
    stopping: std.atomic.Value(bool) = .init(false),
    maintenance_pending: bool = false,
    next_ttl_ns: i96 = 0,
    wake: std.Io.Event = .unset,
    mutex: QueuedMutex = .{},
    current: ?*h.Handle = null,
    retired: std.ArrayList(*h.Handle) = .empty,

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

    /// Caller holds the path lease, so the checkpoint and catalog cannot race
    /// a different process's publication. Cursor continuation deliberately
    /// uses its pinned generation without reopening the connection.
    pub fn refresh(self: *Connection) !*h.Handle {
        if (self.current) |current| {
            var probe = try native.NativeFile.openWithIo(alloc, h.handleLockIo(), self.path, .{ .read_only = true, .internal_reader = true });
            defer probe.close();
            const file = &current.owned_lite_backend.?.native_docstore.?.file;
            const same_inode = (try probe.file.stat(probe.runtime())).inode == (try file.file.stat(file.runtime())).inode;
            if (same_inode and std.meta.eql(probe.header, file.header)) return current;
        }
        var options = self.options;
        options.externally_locked = true;
        const replacement = try api.openLiteHandleAllocWithRuntime(alloc, self.path, options, false, h.handleLockIo(), null);
        errdefer h.closeHandle(replacement);
        if (self.current) |previous| {
            try self.retired.append(alloc, previous);
            replacement.readable_lease_hook = previous.readable_lease_hook;
            replacement.sql_sessions = previous.sql_sessions;
            replacement.next_sql_session_id = previous.next_sql_session_id;
            previous.sql_sessions = .empty;
            var sessions = replacement.sql_sessions.valueIterator();
            while (sessions.next()) |session| session.*.handle = replacement;
            replacement.sql_cursors = previous.sql_cursors;
            replacement.next_sql_cursor_id = previous.next_sql_cursor_id;
            previous.sql_cursors = .empty;
            replacement.table_handles = previous.table_handles;
            previous.table_handles = .empty;
            previous.lease_snapshot = true;
            // Cursor continuation can only read this generation. Make every
            // namespace's shared native store reject publication immediately.
            previous.owned_lite_backend.?.native_docstore.?.read_only = true;
            previous.owned_lite_backend.?.native_docstore.?.file.read_only = true;
        }
        self.current = replacement;
        self.collectRetired();
        return replacement;
    }

    pub fn collectRetired(self: *Connection) void {
        const current = self.current orelse return;
        var i: usize = 0;
        while (i < self.retired.items.len) {
            const retired = self.retired.items[i];
            var cursors = current.sql_cursors.valueIterator();
            const retained = while (cursors.next()) |cursor| {
                if (cursor.*.adapter.handle == retired) break true;
            } else false;
            if (retained) {
                i += 1;
            } else {
                _ = self.retired.swapRemove(i);
                h.closeHandle(retired);
            }
        }
    }

    pub fn notifyMutation(self: *Connection) void {
        if (self.worker == null) return;
        self.maintenance_pending = true;
        self.wake.set(h.handleLockIo());
    }

    fn hasPendingGeneratedEnrichment(database: *h.db_mod.DB) bool {
        const runtime = database.enrichment_runtime orelse return false;
        return database.core.hasGeneratedEnrichmentTargets() and
            runtime.stats().applied_sequence < database.core.nextEnrichmentSequence();
    }

    fn maintenanceLoop(self: *Connection) void {
        const io = h.handleLockIo();
        while (!self.stopping.load(.acquire)) {
            self.wake.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } }) catch {};
            self.wake.reset();
            if (self.stopping.load(.acquire)) break;
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            if (self.stopping.load(.acquire)) break;
            if (self.current == null) continue;
            self.gate.mutex.lockUncancelable(io);
            defer self.gate.mutex.unlock(io);
            if (self.stopping.load(.acquire)) break;
            var lease = native.lockWriterPathWithIo(alloc, io, self.path) catch continue;
            defer lease.close();
            const root = self.refresh() catch continue;
            const cancellation = h.db_mod.types.CancellationToken.fromAtomic(&self.stopping);
            // A reopened generation can have durable producer debt without
            // this connection ever receiving a mutation notification. Inspect
            // every table under the lease, including namespaces not yet used
            // by a foreground call. Keep writer_no_replay's explicit policy.
            if (self.options.open_mode == .writer) {
                @import("tables.zig").load(root) catch continue;
                if (hasPendingGeneratedEnrichment(&root.db)) self.maintenance_pending = true;
                var tables = root.embedded_tables.valueIterator();
                while (tables.next()) |table| {
                    if (hasPendingGeneratedEnrichment(&table.*.db)) self.maintenance_pending = true;
                }
            }
            if (self.maintenance_pending) {
                self.maintenance_pending = false;
                root.db.runUntilIdleWithoutWaitingForEnrichmentRetriesWithCancellation(cancellation) catch {
                    self.maintenance_pending = true;
                };
                var tables = root.embedded_tables.valueIterator();
                while (tables.next()) |table| {
                    if (self.stopping.load(.acquire)) break;
                    table.*.db.runUntilIdleWithoutWaitingForEnrichmentRetriesWithCancellation(cancellation) catch {
                        self.maintenance_pending = true;
                    };
                }
            }
            if (!self.stopping.load(.acquire)) {
                const now = std.Io.Clock.awake.now(io).nanoseconds;
                if (now >= self.next_ttl_ns) {
                    const config = self.options.ttl_cleanup orelse h.db_mod.ttl_runtime.Config{};
                    self.next_ttl_ns = now + @as(i96, config.interval_ms) * std.time.ns_per_ms;
                    if (root.db.ttl_runtime) |ttl| ttl.runOnce() catch {};
                    var tables = root.embedded_tables.valueIterator();
                    while (tables.next()) |table| if (table.*.db.ttl_runtime) |ttl| ttl.runOnce() catch {};
                }
                root.owned_lite_backend.?.native_docstore.?.maintainOnce(false) catch {};
            }
            self.collectRetired();
        }
    }

    pub fn close(self: *Connection) void {
        self.stopping.store(true, .release);
        self.wake.set(h.handleLockIo());
        if (self.worker) |worker| worker.join();

        // Registry close has already drained calls on this connection and its
        // children enter the same parent slot. Teardown must never publish a
        // stale generation, including when another writer currently owns it.
        if (self.current) |current| @import("sql_cursor.zig").closeAll(current);
        if (self.current) |current| current.sql_cursors = .empty;
        for (self.retired.items) |retired| h.closeHandle(retired);
        if (self.current) |current| {
            current.lease_snapshot = true;
            h.closeHandle(current);
        }
        releaseGate(self.gate);
        self.retired.deinit(alloc);
        alloc.free(self.path);
        if (self.owned_ttl_owner_id) |owner_id| alloc.free(owner_id);
        alloc.destroy(self);
    }
};

pub fn open(path: []const u8, options: api.LiteResolvedOpenOptions, create: bool) !*h.Handle {
    if (!h.lite_backend.isAflitePath(path)) return error.InvalidArgument;
    if (create and !h.liteOpenModeCanWrite(options.open_mode)) return error.InvalidArgument;
    const connection = try alloc.create(Connection);
    errdefer alloc.destroy(connection);
    connection.* = .{ .path = undefined, .options = options };
    if (options.ttl_cleanup) |ttl| {
        connection.owned_ttl_owner_id = try alloc.dupe(u8, ttl.owner_id);
        connection.options.ttl_cleanup.?.owner_id = connection.owned_ttl_owner_id.?;
    }
    errdefer if (connection.owned_ttl_owner_id) |owner_id| alloc.free(owner_id);
    var initial: ?*h.Handle = null;
    errdefer if (initial) |handle| {
        handle.lease_snapshot = true;
        h.closeHandle(handle);
    };
    if (create) {
        var resolved = connection.options;
        resolved.externally_locked = true;
        initial = try api.openLiteHandleAllocWithRuntime(alloc, path, resolved, true, h.handleLockIo(), null);
        initial.?.owned_lite_backend.?.native_docstore.?.file.releaseWriterOwnership();
    } else {
        // Check existence and permissions without reading a header that another
        // writer may be publishing. Validate the format under the path lease
        // below, or on the first call if an operation currently holds it.
        const probe = try std.Io.Dir.cwd().openFile(h.handleLockIo(), path, .{
            .mode = if (h.liteOpenModeCanWrite(options.open_mode)) .read_write else .read_only,
        });
        probe.close(h.handleLockIo());
    }
    connection.path = try native.realPathAlloc(alloc, h.handleLockIo(), path);
    errdefer alloc.free(connection.path);
    connection.current = initial;
    connection.gate = try retainGate(connection.path);
    errdefer releaseGate(connection.gate);
    if (!create and connection.gate.mutex.tryLock()) {
        // Initialize eagerly when no writer is active. If initialization would
        // contend, keep the validated connection open and defer it to a call.
        const io = h.handleLockIo();
        defer connection.gate.mutex.unlock(io);
        const attempt = if (h.liteOpenModeCanWrite(options.open_mode))
            native.lockWriterPathWithIo(alloc, io, connection.path)
        else
            native.lockReaderPathWithIo(alloc, io, connection.path);
        if (attempt) |held| {
            var lease = held;
            defer lease.close();
            initial = try connection.refresh();
        } else |err| switch (err) {
            error.WouldBlock, error.FileBusy => {},
            else => return err,
        }
    }
    const handle = try alloc.create(h.Handle);
    handle.* = .{ .alloc = alloc, .db = undefined, .db_live = false, .open_mode = options.open_mode, .lite_connection = connection };
    errdefer alloc.destroy(handle);
    if (comptime h.builtin.os.tag != .freestanding and !h.builtin.single_threaded) {
        if (options.profile == .native and h.liteOpenModeCanWrite(options.open_mode)) {
            connection.worker = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Connection.maintenanceLoop, .{connection});
        }
    }
    return handle;
}
