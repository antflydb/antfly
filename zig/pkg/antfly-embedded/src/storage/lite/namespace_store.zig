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

//! Transactional index namespaces in the native catalog. No inner memtable,
//! manifest or WAL: the native checkpoint is the sole publication authority.
//! Preparation owns its overlay; admission validates a semantic generation,
//! independently of obsolete artifact collection in the same index directory.
const std = @import("std");
const native = @import("native.zig");
const docs = @import("docstore.zig");
const artifacts = @import("artifact_source.zig");
const erased = @import("../backend_erased.zig");
const types = @import("../backend_types.zig");
const publication = @import("../native_publication.zig");
const resources = @import("../resource_manager.zig");
const Host = @import("publication_host.zig").Host;
const A = std.mem.Allocator;
const NS = types.Namespace;
const generation_magic = "AFNS1";
fn generationIdentity(raw: []const u8) ![16]u8 {
    if (raw.len != generation_magic.len + 16 or !std.mem.startsWith(u8, raw, generation_magic)) return error.InvalidNativeIndexGeneration;
    return raw[generation_magic.len..][0..16].*;
}
const component_hex_bytes = 128;
fn encode(a: A, raw: []const u8) ![]u8 {
    const count = try std.math.mul(usize, raw.len, 2);
    const separators = if (count == 0) 0 else (count - 1) / component_hex_bytes;
    const out = try a.alloc(u8, try std.math.add(usize, count, separators));
    const digits = "0123456789abcdef";
    var at: usize = 0;
    for (raw, 0..) |byte, i| {
        if (i != 0 and (2 * i) % component_hex_bytes == 0) {
            out[at] = '/';
            at += 1;
        }
        out[at] = digits[byte >> 4];
        out[at + 1] = digits[byte & 15];
        at += 2;
    }
    return out;
}
fn decode(a: A, encoded: []const u8) ![]u8 {
    const compact = try a.alloc(u8, encoded.len);
    var length: usize = 0;
    var components = std.mem.splitScalar(u8, encoded, '/');
    while (components.next()) |component| {
        const more = components.index != null;
        if ((more and component.len != component_hex_bytes) or
            (!more and component.len > component_hex_bytes) or
            (encoded.len != 0 and component.len == 0)) return error.InvalidNativeNamespaceKey;
        for (component) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.InvalidNativeNamespaceKey;
        @memcpy(compact[length..][0..component.len], component);
        length += component.len;
    }
    if (length % 2 != 0) return error.InvalidNativeNamespaceKey;
    const result = try a.alloc(u8, length / 2);
    _ = std.fmt.hexToBytes(result, compact[0..length]) catch return error.InvalidNativeNamespaceKey;
    return result;
}

/// Canonical native layout shared with typed maintenance capabilities. The
/// caller owns the returned bytes; no snapshot or backend lifetime is borrowed.
pub fn catalogKey(a: A, root: []const u8, ns: NS, key: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    return std.mem.concat(a, u8, &.{ root, "/.native-state/ns/", if (ns.name != null) "1" else "0", try encode(scratch, ns.name orelse ""), "/k", try encode(scratch, key) });
}

fn mapNamespace(ns: NS) !NS {
    return ns;
}

pub const Backend = struct {
    alloc: A,
    registry: *artifacts.Registry,
    prefix: []u8,
    generation_key: []u8,
    refs: std.atomic.Value(usize) = .init(1),
    read_only: bool,

    pub fn open(a: A, store: *docs.Store, root: []const u8, read_only: bool) !erased.NamespaceStore {
        const self = try a.create(Backend);
        errdefer a.destroy(self);
        const prefix = try std.mem.concat(a, u8, &.{ root, "/.native-state/" });
        errdefer a.free(prefix);
        const generation_key = try std.mem.concat(a, u8, &.{ prefix, "generation" });
        errdefer a.free(generation_key);
        const registry = try store.artifactRegistry();
        registry.retainOwner();
        self.* = .{ .alloc = a, .registry = registry, .prefix = prefix, .generation_key = generation_key, .read_only = read_only or store.read_only };
        return .{ .allocator = a, .ptr = self, .vtable = &.{ .deinit = close, .capabilities = capabilities, .begin_read = beginRead, .begin_write = beginWrite, .begin_batch = beginBatch } };
    }
    fn cast(raw: *anyopaque) *Backend {
        return @ptrCast(@alignCast(raw));
    }
    fn retain(self: *Backend) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }
    fn release(self: *Backend) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.alloc;
        a.free(self.prefix);
        a.free(self.generation_key);
        self.registry.releaseOwner();
        a.destroy(self);
    }
    fn close(_: A, raw: *anyopaque) void {
        cast(raw).release();
    }
    fn capabilities(_: *anyopaque) types.Capabilities {
        return .{ .native_namespaces = true, .reverse_ranges = true, .ordered_append_puts = true };
    }
    fn beginRead(a: A, raw: *anyopaque) !erased.NamespaceReadTxn {
        var txn = try Txn.create(cast(raw), false);
        errdefer txn.abort();
        return erased.namespaceReadTxnFrom(a, txn, NS, mapNamespace);
    }
    fn beginWrite(a: A, raw: *anyopaque) !erased.NamespaceWriteTxn {
        var txn = try Txn.create(cast(raw), true);
        errdefer txn.abort();
        return erased.namespaceWriteTxnFrom(a, txn, NS, mapNamespace);
    }
    fn beginBatch(a: A, raw: *anyopaque) !erased.NamespaceBatch {
        var txn = try Txn.create(cast(raw), true);
        errdefer txn.abort();
        return erased.namespaceBatchFrom(a, txn, NS, mapNamespace);
    }
};
const State = struct {
    backend: *Backend,
    read_mutex: std.atomic.Mutex = .unlocked,
    budget: ?resources.BudgetedAllocator = null,
    arena: std.heap.ArenaAllocator,
    snapshot: ?native.NativeFile = null,
    expected: ?[16]u8 = null,
    inode: u64,
    file_namespace: [32]u8,
    writable: bool,
    pending: std.StringHashMapUnmanaged(native.CatalogMutation) = .empty,
    participant: ?publication.Participant = null,
    mutations: []native.CatalogMutation = &.{},
    fn a(self: *State) A {
        return self.arena.allocator();
    }
    fn namespacePrefix(self: *State, ns: NS) ![]u8 {
        // Native records remain ordinary entries to generation manifests and
        // subtree cleanup. Ordered hex preserves arbitrary binary namespace
        // and key bytes while keeping those compatibility paths NUL-free.
        return std.mem.concat(self.a(), u8, &.{ self.backend.prefix, "ns/", if (ns.name != null) "1" else "0", try encode(self.a(), ns.name orelse ""), "/k" });
    }
    fn key(self: *State, ns: NS, suffix: []const u8) ![]u8 {
        return std.mem.concat(self.a(), u8, &.{ try self.namespacePrefix(ns), try encode(self.a(), suffix) });
    }
    fn destroy(self: *State) void {
        const backend = self.backend;
        if (self.snapshot) |*snapshot| snapshot.close();
        self.arena.deinit();
        if (self.budget) |*budget| budget.deinit();
        backend.alloc.destroy(self);
        backend.release();
    }
    fn stage(raw: *anyopaque, file: *native.NativeFile) !void {
        const self: *State = @ptrCast(@alignCast(raw));
        const data_root = self.backend.prefix[0 .. self.backend.prefix.len - "/.native-state/".len];
        const root = std.fs.path.dirname(data_root) orelse "";
        var host: Host = .{ .file = file, .allocator = self.a(), .index_prefix = root };
        try self.stageView(host.view());
    }
    fn stageView(self: *State, view: publication.View) !void {
        try view.publishNamespace(.{
            .inode = self.inode,
            .file_namespace = self.file_namespace,
            .prefix = self.backend.prefix,
            .generation_key = self.backend.generation_key,
            .expected_generation = self.expected,
            .mutations = self.mutations,
        });
        if (self.participant) |participant| try participant.apply(view);
    }
    fn stagePrepared(raw: *anyopaque, view: publication.View) !void {
        const self: *State = @ptrCast(@alignCast(raw));
        try self.stageView(view);
    }
    fn releasePrepared(raw: *anyopaque) void {
        const self: *State = @ptrCast(@alignCast(raw));
        self.destroy();
    }

    fn prepare(self: *State) !void {
        const mutations = try self.a().alloc(native.CatalogMutation, self.pending.count() + 1);
        var values = self.pending.valueIterator();
        var i: usize = 0;
        while (values.next()) |value| : (i += 1) mutations[i] = value.*;
        const generation = try self.a().alloc(u8, generation_magic.len + 16);
        @memcpy(generation[0..generation_magic.len], generation_magic);
        try self.backend.registry.io().randomSecure(generation[generation_magic.len..]);
        mutations[i] = .{ .key = self.backend.generation_key, .value = generation };
        self.mutations = mutations;
    }
};
const Txn = struct {
    state: *State,
    fn create(backend: *Backend, writable: bool) !Txn {
        if (writable and backend.read_only) return error.ReadOnly;
        const owner = backend.registry.beginCallback() orelse return error.ReadOnly;
        defer backend.registry.endCallback();
        const admission = owner.writer_admission;
        if (admission) |port| try port.enter(port.ptr, .index_read);
        defer if (admission) |port| port.leave(port.ptr);
        const io = owner.file.runtime();
        owner.generation_lock.lockSharedUncancelable(io);
        defer owner.generation_lock.unlockShared(io);
        owner.mutex.lock();
        var snapshot = owner.file.captureLogicalReadSnapshot() catch |err| {
            owner.mutex.unlock();
            return err;
        };
        owner.mutex.unlock();
        errdefer snapshot.close();
        // The registry and its IO/resource owner outlive Store shutdown.
        snapshot.borrowed_io = backend.registry.io();
        const state = try backend.alloc.create(State);
        errdefer backend.alloc.destroy(state);
        state.* = .{ .backend = backend, .arena = undefined, .inode = (try snapshot.file.stat(snapshot.runtime())).inode, .file_namespace = snapshot.header.reader_epoch_namespace, .writable = writable };
        if (owner.resource_manager) |manager| state.budget = resources.BudgetedAllocator.init(manager, .lite_docstore_snapshot_cache, backend.alloc, 1);
        state.arena = std.heap.ArenaAllocator.init(if (state.budget) |*budget| budget.allocator() else backend.alloc);
        errdefer {
            state.arena.deinit();
            if (state.budget) |*budget| budget.deinit();
        }
        if (try snapshot.getIndexCatalogRecordAlloc(state.a(), backend.generation_key)) |value| {
            state.expected = try generationIdentity(value);
        }
        state.snapshot = snapshot;
        backend.retain();
        return .{ .state = state };
    }
    pub fn abort(self: *Txn) void {
        self.state.destroy();
        self.* = undefined;
    }
    pub fn get(self: *Txn, ns: NS, key: []const u8) ![]const u8 {
        const state = self.state;
        @import("antfly_platform").sync.lockYielding(&state.read_mutex);
        defer state.read_mutex.unlock();
        const snapshot = if (state.snapshot) |*value| value else return error.TransactionClosed;
        const full = try state.key(ns, key);
        if (state.pending.get(full)) |change| return if (change.is_delete) error.NotFound else change.value;
        return (try snapshot.getIndexCatalogRecordAlloc(state.a(), full)) orelse error.NotFound;
    }
    pub fn put(self: *Txn, ns: NS, key: []const u8, value: []const u8) !void {
        if (!self.state.writable) return error.ReadOnly;
        if (self.state.snapshot == null) return error.TransactionClosed;
        const full = try self.state.key(ns, key);
        const owned = try self.state.a().dupe(u8, value);
        try self.state.pending.put(self.state.a(), full, .{ .key = full, .value = owned });
    }
    pub fn appendPut(self: *Txn, ns: NS, key: []const u8, value: []const u8) !void {
        return self.put(ns, key, value);
    }
    pub fn delete(self: *Txn, ns: NS, key: []const u8) !void {
        if (!self.state.writable) return error.ReadOnly;
        if (self.state.snapshot == null) return error.TransactionClosed;
        const full = try self.state.key(ns, key);
        try self.state.pending.put(self.state.a(), full, .{ .key = full, .is_delete = true });
    }
    pub fn prepareNativePublication(self: *Txn) !publication.Prepared {
        const state = self.state;
        if (!state.writable) return error.ReadOnly;
        if (state.snapshot == null) return error.TransactionClosed;
        try state.prepare();
        state.snapshot.?.close();
        state.snapshot = null;
        self.* = undefined;
        return .{ .participant = .{ .ptr = state, .stage = State.stagePrepared }, .release_fn = State.releasePrepared };
    }
    pub fn commitWithPublication(self: *Txn, participant: publication.Participant) !void {
        self.state.participant = participant;
        try self.commit();
    }
    pub fn commit(self: *Txn) !void {
        const state = self.state;
        if (!state.writable) return error.ReadOnly;
        if (state.snapshot == null) return error.TransactionClosed;
        if (state.pending.count() != 0 or state.participant != null) try state.prepare();
        // No borrowed metadata survives the writer queue. The exact semantic
        // witness, owned mutations and immutable artifacts are sufficient.
        state.snapshot.?.close();
        state.snapshot = null;
        const registry = state.backend.registry;
        const owner = registry.beginCallback() orelse return error.ReadOnly;
        defer registry.endCallback();
        if (state.pending.count() != 0 or state.participant != null) {
            var possible = false;
            owner.submitIndexMutationWithPublicationState(state, State.stage, true, &possible) catch |err| {
                if (possible) {
                    if (types.commit_progress) |progress| progress.maybe_applied = true;
                }
                return err;
            };
        }
        state.destroy();
        self.* = undefined;
    }
    pub fn openCursor(self: *Txn, ns: NS) !Cursor {
        const state = self.state;
        @import("antfly_platform").sync.lockYielding(&state.read_mutex);
        defer state.read_mutex.unlock();
        const snapshot = if (state.snapshot) |*value| value else return error.TransactionClosed;
        const prefix = try state.namespacePrefix(ns);
        var base = try snapshot.indexCatalogCursor(snapshot.activeCheckpoint(), prefix);
        errdefer base.deinit();
        var pending = std.ArrayListUnmanaged(native.CatalogMutation).empty;
        var values = state.pending.valueIterator();
        while (values.next()) |value| if (std.mem.startsWith(u8, value.key, prefix)) {
            try pending.append(state.a(), value.*);
        };
        std.mem.sort(native.CatalogMutation, pending.items, {}, struct {
            fn less(_: void, l: native.CatalogMutation, r: native.CatalogMutation) bool {
                return std.mem.lessThan(u8, l.key, r.key);
            }
        }.less);
        return .{ .state = state, .base = base, .prefix = prefix, .pending = pending.items, .scratch = std.heap.ArenaAllocator.init(if (state.budget) |*budget| budget.allocator() else state.backend.alloc) };
    }
};
const Cursor = struct {
    state: *State,
    base: native.CatalogCursor,
    prefix: []const u8,
    pending: []const native.CatalogMutation,
    scratch: std.heap.ArenaAllocator,
    position: ?[]const u8 = null,
    keys_only: bool = false,
    pub fn setKeysOnly(self: *Cursor) void {
        self.keys_only = true;
    }
    pub fn close(self: *Cursor) void {
        self.base.deinit();
        self.scratch.deinit();
        self.* = undefined;
    }
    pub fn first(self: *Cursor) !erased.Entry {
        return self.move(self.prefix, false, true);
    }
    pub fn last(self: *Cursor) !erased.Entry {
        // Namespace encoding ends with the name, not a guaranteed sentinel.
        const bound = try self.scratch.allocator().dupe(u8, self.prefix);
        var end = bound.len;
        while (end > 0 and bound[end - 1] == 255) end -= 1;
        if (end == 0) return error.InvalidNativeIndexPath;
        bound[end - 1] += 1;
        return self.move(bound[0..end], true, false);
    }
    pub fn seekAtOrAfter(self: *Cursor, key: []const u8) !erased.Entry {
        const bound = try std.mem.concat(self.scratch.allocator(), u8, &.{ self.prefix, try encode(self.scratch.allocator(), key) });
        return self.move(bound, false, true);
    }
    pub fn seekAtOrBefore(self: *Cursor, key: []const u8) !erased.Entry {
        const bound = try std.mem.concat(self.scratch.allocator(), u8, &.{ self.prefix, try encode(self.scratch.allocator(), key) });
        return self.move(bound, false, false);
    }
    pub fn next(self: *Cursor) !erased.Entry {
        return self.move(self.position orelse return self.first(), true, true);
    }
    pub fn prev(self: *Cursor) !erased.Entry {
        return self.move(self.position orelse return self.last(), true, false);
    }
    fn move(self: *Cursor, borrowed_bound: []const u8, strict: bool, forward: bool) !erased.Entry {
        // Copy the boundary before clearing the previous borrowed result.
        const temporary = self.state.backend.alloc;
        const bound = try temporary.dupe(u8, borrowed_bound);
        defer temporary.free(bound);
        _ = self.scratch.reset(.retain_capacity);
        const a = self.scratch.allocator();
        var disk = if (forward) try self.base.index.seekAtOrAfter(bound, strict) else try self.base.index.seekAtOrBefore(bound, strict);
        defer if (disk) |*entry| entry.deinit(self.base.index.file.allocator);
        var lo: usize = 0;
        var hi = self.pending.len;
        // Locate only the next changed key; do not enumerate the namespace or
        // replay an entire journal record on each continuation.
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const order = std.mem.order(u8, self.pending[mid].key, bound);
            const before = order == .lt or (order == .eq and if (forward) strict else !strict);
            if (before) lo = mid + 1 else hi = mid;
        }
        var change: ?usize = if (forward) (if (lo < self.pending.len) lo else null) else (if (lo > 0) lo - 1 else null);
        while (true) {
            const disk_key: ?[]const u8 = if (disk) |entry| (if (std.mem.startsWith(u8, entry.key, self.prefix)) entry.key else null) else null;
            const changed: ?native.CatalogMutation = if (change) |i| self.pending[i] else null;
            const use_change = if (changed) |entry| if (disk_key) |key| blk: {
                const order = std.mem.order(u8, entry.key, key);
                break :blk order == .eq or if (forward) order == .lt else order == .gt;
            } else true else false;
            if (use_change) {
                const entry = changed.?;
                if (!entry.is_delete) return self.result(a, entry.key, entry.value);
                if (change) |i| change = if (forward) (if (i + 1 < self.pending.len) i + 1 else null) else (if (i > 0) i - 1 else null);
                if (disk_key) |key| if (std.mem.eql(u8, key, entry.key)) {
                    disk.?.deinit(self.base.index.file.allocator);
                    disk = null;
                    disk = if (forward) try self.base.index.next() else try self.base.index.prev();
                };
            } else if (disk_key) |key| {
                if (try self.base.valueForEntryAlloc(a, disk.?, self.keys_only)) |value| return self.result(a, key, value);
                disk.?.deinit(self.base.index.file.allocator);
                disk = null;
                disk = if (forward) try self.base.index.next() else try self.base.index.prev();
            } else {
                self.position = try a.dupe(u8, bound);
                return error.NotFound;
            }
        }
    }
    fn result(self: *Cursor, a: A, key: []const u8, value: []const u8) !erased.Entry {
        self.position = try a.dupe(u8, key);
        const encoded = self.position.?[self.prefix.len..];
        return .{ .key = try decode(a, encoded), .value = if (self.keys_only) &.{} else value };
    }
};

/// Select only complete native roots. An existing inner LSM remains the
/// authority until an explicit migration has installed a complete replacement;
/// never interpret its files as an empty native namespace.
pub fn openIfSupported(a: A, owner: *docs.Store, root: []const u8, read_only: bool) !?erased.NamespaceStore {
    if (!owner.file.header.indexed_reclamation or owner.file.header.indexed_revision < 13) return null;
    var store = try Backend.open(a, owner, root, read_only);
    var keep = false;
    defer if (!keep) store.deinit();
    var probe = try Txn.create(Backend.cast(store.ptr), false);
    defer probe.abort();
    if (probe.state.expected == null) {
        const prefix = try std.mem.concat(probe.state.a(), u8, &.{ root, "/" });
        const snapshot = &probe.state.snapshot.?;
        var cursor = try snapshot.indexCatalogCursor(snapshot.activeCheckpoint(), prefix);
        defer cursor.deinit();
        if (try cursor.next()) |entry| {
            snapshot.allocator.free(entry.key);
            return null;
        }
    }
    keep = true;
    return store;
}
