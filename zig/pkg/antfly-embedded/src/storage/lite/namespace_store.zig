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
const PreparedValue = @import("../native_publication_value.zig").Value;

const State = struct {
    backend: *Backend,
    read_mutex: std.atomic.Mutex = .unlocked,
    budget: ?resources.BudgetedAllocator = null,
    arena: std.heap.ArenaAllocator,
    snapshot: ?native.NativeFile = null,
    expected: ?[16]u8 = null,
    next_generation: ?[16]u8 = null,
    inode: u64,
    file_namespace: [32]u8,
    writable: bool,
    pending: std.StringHashMapUnmanaged(native.CatalogMutation) = .empty,
    bound_values: std.StringHashMapUnmanaged(PreparedValue) = .empty,
    participant: ?publication.Participant = null,
    mutations: []native.CatalogMutation = &.{},
    fn a(self: *State) A {
        return self.arena.allocator();
    }
    fn identity(self: *const State, generation: ?[16]u8) publication.NamespaceIdentity {
        var root: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(self.backend.prefix, &root, .{});
        return .{ .inode = self.inode, .file_namespace = self.file_namespace, .root = root, .generation = generation };
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
        var values = self.bound_values.valueIterator();
        while (values.next()) |value| value.deinit();
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
        try self.validateView(host.view());
        try self.stageView(host.view());
    }
    fn validateView(self: *State, view: publication.View) !void {
        if (self.participant) |participant| try participant.validate(view);
    }
    fn stageView(self: *State, view: publication.View) !void {
        // Mutation descriptors already reference these fixed buffers. Acquire
        // every immutable seal before publishing any of their bytes. Neither
        // descriptor rebinding nor allocation occurs under writer admission.
        var values = self.bound_values.valueIterator();
        while (values.next()) |value| _ = try value.read();
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
    fn validatePrepared(raw: *anyopaque, view: publication.View) !void {
        const self: *State = @ptrCast(@alignCast(raw));
        try self.validateView(view);
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
        self.next_generation = generation[generation_magic.len..][0..16].*;
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
        if (state.pending.get(full)) |change| {
            if (change.is_delete) return error.NotFound;
            return if (state.bound_values.get(full)) |value| try value.read() else change.value;
        }
        return (try snapshot.getIndexCatalogRecordAlloc(state.a(), full)) orelse error.NotFound;
    }
    pub fn put(self: *Txn, ns: NS, key: []const u8, value: []const u8) !void {
        if (!self.state.writable) return error.ReadOnly;
        if (self.state.snapshot == null) return error.TransactionClosed;
        const full = try self.state.key(ns, key);
        const owned = try self.state.a().dupe(u8, value);
        try self.state.pending.put(self.state.a(), full, .{ .key = full, .value = owned });
        if (self.state.bound_values.fetchRemove(full)) |removed| {
            var previous = removed.value;
            previous.deinit();
        }
    }
    pub fn putPreparedValue(self: *Txn, ns: NS, key: []const u8, value: PreparedValue) !void {
        const state = self.state;
        if (!state.writable) return error.ReadOnly;
        if (state.snapshot == null) return error.TransactionClosed;
        const full = try state.key(ns, key);
        // After both reservations succeed, replacement cannot fail and owns
        // exactly one reference. An allocation error preserves the old value.
        try state.pending.ensureUnusedCapacity(state.a(), 1);
        try state.bound_values.ensureUnusedCapacity(state.a(), 1);
        const entry = state.bound_values.getOrPutAssumeCapacity(full);
        // The incoming borrowed handle may alias this overlay's sole owned
        // reference. Retain before releasing a replaced binding.
        const retained = value.retained();
        if (entry.found_existing) entry.value_ptr.deinit();
        entry.value_ptr.* = retained;
        state.pending.putAssumeCapacity(full, .{ .key = full, .value = value.initialization_view });
    }
    pub fn appendPut(self: *Txn, ns: NS, key: []const u8, value: []const u8) !void {
        return self.put(ns, key, value);
    }
    pub fn delete(self: *Txn, ns: NS, key: []const u8) !void {
        if (!self.state.writable) return error.ReadOnly;
        if (self.state.snapshot == null) return error.TransactionClosed;
        const full = try self.state.key(ns, key);
        try self.state.pending.put(self.state.a(), full, .{ .key = full, .is_delete = true });
        if (self.state.bound_values.fetchRemove(full)) |removed| {
            var previous = removed.value;
            previous.deinit();
        }
    }
    pub fn prepareNativePublication(self: *Txn) !publication.Prepared {
        const state = self.state;
        if (!state.writable) return error.ReadOnly;
        if (state.snapshot == null) return error.TransactionClosed;
        try state.prepare();
        state.snapshot.?.close();
        state.snapshot = null;
        self.* = undefined;
        return .{ .before = state.identity(state.expected), .after = state.identity(state.next_generation), .participant = .{ .ptr = state, .stage = State.stagePrepared, .validate_fn = State.validatePrepared }, .release_fn = State.releasePrepared };
    }
    pub fn commitWithPublication(self: *Txn, participant: publication.Participant) !void {
        self.state.participant = participant;
        try self.commit();
    }
    pub fn nativeIdentity(self: *Txn) !publication.NamespaceIdentity {
        if (self.state.snapshot == null) return error.TransactionClosed;
        return self.state.identity(self.state.expected);
    }
    pub fn commitWithNativeIdentity(self: *Txn, participant: ?publication.Participant, out: *publication.NamespaceIdentity) !void {
        self.state.participant = participant;
        return self.commitImpl(out);
    }
    pub fn commit(self: *Txn) !void {
        return self.commitImpl(null);
    }
    fn commitImpl(self: *Txn, identity_out: ?*publication.NamespaceIdentity) !void {
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
        if (identity_out) |out| out.* = state.identity(if (state.pending.count() != 0 or state.participant != null) state.next_generation else state.expected);
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
        var pending = std.ArrayListUnmanaged(Cursor.Pending).empty;
        errdefer for (pending.items) |*entry| if (entry.value) |*value| value.deinit();
        var values = state.pending.valueIterator();
        while (values.next()) |value| if (std.mem.startsWith(u8, value.key, prefix)) {
            var entry: Cursor.Pending = .{ .change = value.*, .value = if (state.bound_values.get(value.key)) |bound| bound.retained() else null };
            errdefer if (entry.value) |*bound| bound.deinit();
            try pending.append(state.a(), entry);
            entry.value = null;
        };
        std.mem.sort(Cursor.Pending, pending.items, {}, struct {
            fn less(_: void, l: Cursor.Pending, r: Cursor.Pending) bool {
                return std.mem.lessThan(u8, l.change.key, r.change.key);
            }
        }.less);
        return .{ .state = state, .base = base, .prefix = prefix, .pending = pending.items, .scratch = std.heap.ArenaAllocator.init(if (state.budget) |*budget| budget.allocator() else state.backend.alloc) };
    }
};
const Cursor = struct {
    const Pending = struct { change: native.CatalogMutation, value: ?PreparedValue = null };
    state: *State,
    base: native.CatalogCursor,
    prefix: []const u8,
    pending: []Pending,
    scratch: std.heap.ArenaAllocator,
    position: ?[]const u8 = null,
    keys_only: bool = false,
    pub fn setKeysOnly(self: *Cursor) void {
        self.keys_only = true;
    }
    pub fn close(self: *Cursor) void {
        for (self.pending) |*entry| if (entry.value) |*value| value.deinit();
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
            const order = std.mem.order(u8, self.pending[mid].change.key, bound);
            const before = order == .lt or (order == .eq and if (forward) strict else !strict);
            if (before) lo = mid + 1 else hi = mid;
        }
        var change: ?usize = if (forward) (if (lo < self.pending.len) lo else null) else (if (lo > 0) lo - 1 else null);
        while (true) {
            const disk_key: ?[]const u8 = if (disk) |entry| (if (std.mem.startsWith(u8, entry.key, self.prefix)) entry.key else null) else null;
            const changed: ?native.CatalogMutation = if (change) |i| self.pending[i].change else null;
            const use_change = if (changed) |entry| if (disk_key) |key| blk: {
                const order = std.mem.order(u8, entry.key, key);
                break :blk order == .eq or if (forward) order == .lt else order == .gt;
            } else true else false;
            if (use_change) {
                const entry = changed.?;
                if (!entry.is_delete) {
                    const value = if (self.keys_only) &.{} else if (self.pending[change.?].value) |bound_value| try bound_value.read() else entry.value;
                    return self.result(a, entry.key, value);
                }
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

test "native namespace binding refuses unbound publication and commits after producer and backend close" {
    const a = std.testing.allocator;
    const values = @import("../native_publication_value.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/bound-native-publication.aflite", .{tmp.sub_path});
    defer a.free(path);
    var first = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    var first_live = true;
    defer if (first_live) first.close();
    first.maintenance_start_suppressed = true;
    var backend = try Backend.open(a, &first, "/indexes/bound", false);
    var backend_live = true;
    defer if (backend_live) backend.deinit();
    const ns: NS = .{ .name = "meta" };
    var builder = try values.Builder.create(a, 8);
    var builder_live = true;
    defer if (builder_live) builder.deinit();
    var value = builder.value();
    defer value.deinit();
    var pending = try backend.beginWrite();
    var pending_live = true;
    defer if (pending_live) pending.abort();
    try pending.putPreparedValue(ns, "identity", value);
    try std.testing.expectError(error.NativePublicationValueNotBound, pending.get(ns, "identity"));
    {
        var cursor = try pending.openCursor(ns);
        defer cursor.close();
        try std.testing.expectError(error.NativePublicationValueNotBound, cursor.first());
        cursor.setKeysOnly();
        const key = (try cursor.first()) orelse return error.MissingBoundKey;
        try std.testing.expectEqualStrings("identity", key.key);
        try std.testing.expectEqual(@as(usize, 0), key.value.len);
        try std.testing.expectError(error.TransactionCursorActive, pending.prepareNativePublication());
    }
    var prepared = try pending.prepareNativePublication();
    pending_live = false;
    defer prepared.deinit();
    backend.deinit();
    backend_live = false;
    first.close();
    first_live = false;
    var second = try docs.Store.openWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer second.close();
    second.maintenance_start_suppressed = true;
    const before = second.file.activeCheckpoint();
    {
        var batch = try second.beginBatch();
        defer batch.abort();
        try batch.put("primary", "committed");
        try batch.setNativePublication(prepared.participant);
        try std.testing.expectError(error.NativePublicationValueNotBound, batch.commit());
    }
    try std.testing.expectEqualDeep(before, second.file.activeCheckpoint());
    try std.testing.expect((try second.file.getDocumentAlloc(a, "primary")) == null);
    try builder.write(0, "identity");
    try builder.seal();
    builder.deinit();
    builder_live = false;
    {
        var batch = try second.beginBatch();
        errdefer batch.abort();
        try batch.put("primary", "committed");
        try batch.setNativePublication(prepared.participant);
        try batch.commit();
    }
    try std.testing.expectEqual(before.commit_sequence + 1, second.file.activeCheckpoint().commit_sequence);
    var reopened = try Backend.open(a, &second, "/indexes/bound", false);
    defer reopened.deinit();
    var read = try reopened.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("identity", try read.get(ns, "identity"));
    const primary = (try second.file.getDocumentAlloc(a, "primary")).?;
    defer a.free(primary);
    try std.testing.expectEqualStrings("committed", primary);
    try std.testing.expect((try second.file.check()).valid);
}

test "native namespace binding cursor retains its value across pending overwrite and delete" {
    const a = std.testing.allocator;
    const values = @import("../native_publication_value.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/bound-native-cursor.aflite", .{tmp.sub_path});
    defer a.free(path);
    var store = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    var backend = try Backend.open(a, &store, "/indexes/bound-cursor", false);
    defer backend.deinit();
    const ns: NS = .{ .name = "meta" };
    for ([_]bool{ false, true }) |delete| {
        var pending = try backend.beginWrite();
        var pending_live = true;
        defer if (pending_live) pending.abort();
        {
            var builder = try values.Builder.create(a, 8);
            defer builder.deinit();
            try builder.write(0, "original");
            try builder.seal();
            var value = builder.value();
            defer value.deinit();
            try pending.putPreparedValue(ns, "identity", value);
        }
        {
            var cursor = try pending.openCursor(ns);
            defer cursor.close();
            if (delete) try pending.delete(ns, "identity") else try pending.put(ns, "identity", "replacement");
            if (delete) try std.testing.expectError(error.NotFound, pending.get(ns, "identity")) else try std.testing.expectEqualStrings("replacement", try pending.get(ns, "identity"));
            const original = (try cursor.first()) orelse return error.MissingCapturedBinding;
            try std.testing.expectEqualStrings("original", original.value);
        }
        try pending.commit();
        pending_live = false;
        var read = try backend.beginRead();
        defer read.abort();
        if (delete) try std.testing.expectError(error.NotFound, read.get(ns, "identity")) else try std.testing.expectEqualStrings("replacement", try read.get(ns, "identity"));
    }
}

test "native namespace binding replacement retains an aliased sole owner" {
    const a = std.testing.allocator;
    const values = @import("../native_publication_value.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/bound-native-alias.aflite", .{tmp.sub_path});
    defer a.free(path);
    var store = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    var backend = try Backend.open(a, &store, "/indexes/bound-alias", false);
    defer backend.deinit();
    var pending = try Txn.create(Backend.cast(backend.ptr), true);
    defer pending.abort();
    const ns: NS = .{ .name = "meta" };
    {
        var builder = try values.Builder.create(a, 8);
        defer builder.deinit();
        try builder.write(0, "original");
        try builder.seal();
        var value = builder.value();
        defer value.deinit();
        try pending.putPreparedValue(ns, "identity", value);
    }
    const key = try pending.state.key(ns, "identity");
    const borrowed = pending.state.bound_values.get(key).?;
    try pending.putPreparedValue(ns, "identity", borrowed);
    try std.testing.expectEqualStrings("original", try pending.get(ns, "identity"));
}

test "native namespace binding cannot refresh a stale namespace witness by sealing later" {
    const a = std.testing.allocator;
    const values = @import("../native_publication_value.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/bound-native-stale.aflite", .{tmp.sub_path});
    defer a.free(path);
    var store = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    var backend = try Backend.open(a, &store, "/indexes/bound-stale", false);
    defer backend.deinit();
    const ns: NS = .{ .name = "meta" };
    var builder = try values.Builder.create(a, 8);
    defer builder.deinit();
    var value = builder.value();
    defer value.deinit();
    var pending = try backend.beginWrite();
    var pending_live = true;
    defer if (pending_live) pending.abort();
    try pending.putPreparedValue(ns, "identity", value);
    var prepared = try pending.prepareNativePublication();
    pending_live = false;
    defer prepared.deinit();
    {
        var foreign = try backend.beginWrite();
        errdefer foreign.abort();
        try foreign.put(ns, "foreign", "new generation");
        try foreign.commit();
    }
    try builder.write(0, "identity");
    try builder.seal();
    const before = store.file.activeCheckpoint();
    {
        var batch = try store.beginBatch();
        defer batch.abort();
        try batch.put("primary", "must not commit");
        try batch.setNativePublication(prepared.participant);
        try std.testing.expectError(error.FileBusy, batch.commit());
    }
    try std.testing.expectEqualDeep(before, store.file.activeCheckpoint());
    try std.testing.expect((try store.file.getDocumentAlloc(a, "primary")) == null);
    var read = try backend.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("new generation", try read.get(ns, "foreign"));
    try std.testing.expectError(error.NotFound, read.get(ns, "identity"));
}

test "native namespace binding allocation failures release parameter and cursor ownership" {
    const a = std.testing.allocator;
    const values = @import("../native_publication_value.zig");
    const Probe = struct {
        references: usize = 1,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references -= 1;
        }
        fn run(fault: A, store: *docs.Store, value: PreparedValue) !void {
            var backend = try Backend.open(fault, store, "/indexes/bound-allocations", false);
            defer backend.deinit();
            var pending = try backend.beginWrite();
            var pending_live = true;
            defer if (pending_live) pending.abort();
            const ns: NS = .{ .name = "meta" };
            try pending.putPreparedValue(ns, "left", value);
            try pending.putPreparedValue(ns, "right", value);
            {
                var cursor = try pending.openCursor(ns);
                defer cursor.close();
                const first = (try cursor.first()) orelse return error.MissingBoundKey;
                try std.testing.expectEqualStrings("left", first.key);
                try std.testing.expectEqualStrings("original", first.value);
                try pending.put(ns, "left", "replacement");
                const next = (try cursor.next()) orelse return error.MissingBoundKey;
                try std.testing.expectEqualStrings("right", next.key);
                try std.testing.expectEqualStrings("original", next.value);
            }
            var prepared = try pending.prepareNativePublication();
            pending_live = false;
            defer prepared.deinit();
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/bound-native-allocation-faults.aflite", .{tmp.sub_path});
    defer a.free(path);
    var store = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    var owner: Probe = .{};
    var builder = try values.Builder.createWithOwner(a, 8, .{ .ptr = &owner, .retain_fn = Probe.retain, .release_fn = Probe.release });
    var builder_live = true;
    defer if (builder_live) builder.deinit();
    try builder.write(0, "original");
    try builder.seal();
    var value = builder.value();
    var value_live = true;
    defer if (value_live) value.deinit();
    builder.deinit();
    builder_live = false;
    const checkpoint = store.file.activeCheckpoint();
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{ &store, value });
    try std.testing.expectEqualDeep(checkpoint, store.file.activeCheckpoint());
    value.deinit();
    value_live = false;
    try std.testing.expectEqual(@as(usize, 1), owner.references);
}

test "native namespace identity follows committed snapshots and prepared generation without consuming a live cursor" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/native-namespace-identity.aflite", .{tmp.sub_path});
    defer a.free(path);
    var owner = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer owner.close();
    owner.maintenance_start_suppressed = true;
    var backend = try Backend.open(a, &owner, "/indexes/identity", false);
    defer backend.deinit();
    var other = try Backend.open(a, &owner, "/indexes/other", false);
    defer other.deinit();
    var initial = try backend.beginRead();
    defer initial.abort();
    const before = (try initial.nativeIdentity()).?;
    var other_read = try other.beginRead();
    defer other_read.abort();
    try std.testing.expect(!before.eql((try other_read.nativeIdentity()).?));
    var write = try backend.beginWrite();
    var write_live = true;
    defer if (write_live) write.abort();
    try write.put(.{}, "key", "value");
    {
        var cursor = try write.openCursor(.{});
        defer cursor.close();
        try std.testing.expectError(error.TransactionCursorActive, write.commitWithNativeIdentity(null));
        try std.testing.expectEqualStrings("value", try write.get(.{}, "key"));
    }
    const committed = (try write.commitWithNativeIdentity(null)).?;
    write_live = false;
    try std.testing.expect(!before.eql(committed));
    try std.testing.expect(before.eql((try initial.nativeIdentity()).?));
    var after = try backend.beginRead();
    defer after.abort();
    try std.testing.expect(committed.eql((try after.nativeIdentity()).?));
    var next = try backend.beginWrite();
    var next_live = true;
    defer if (next_live) next.abort();
    try next.put(.{}, "key", "next");
    var prepared = try next.prepareNativePublication();
    next_live = false;
    defer prepared.deinit();
    try std.testing.expect(committed.eql(prepared.before.?));
    try std.testing.expect(!prepared.before.?.eql(prepared.after.?));
    try std.testing.expect(committed.eql((try after.nativeIdentity()).?));
}
