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

//! The concrete `storage/native_catalog_backend.zig:Sink` for Lite: durably
//! persists `PersistentIndex`'s main-backend records (segment ranges, the
//! active segment set, generation metadata) as native `.aflite`
//! index-catalog records, through the same `docstore.Store.submitIndexMutation`
//! group-commit queue every other Lite index-storage write already funnels
//! through (`lite/index_storage.zig`).
//!
//! One `Sink` instance wraps one `docstore.Store` and is shared by every
//! `PersistentIndex` (every full-text/dense/sparse/graph managed index) that
//! opens with `main_backend == .native_catalog` in that Lite DB; the
//! `scope` parameter each call carries (`PersistentIndex`'s own index path,
//! passed by `persistent.zig`'s `openMainStore`) disambiguates one index's
//! records from another's so they never collide in the shared `.index`
//! catalog root.
//!
//! Durable key shape: `reserved_prefix ++ scope ++ "\x00" ++ namespace ++
//! "\x00" ++ key`. `reserved_prefix` (`"\x00ncat/"`) can never collide with
//! an `lite/index_storage.zig` virtual-filesystem path (those are validated
//! to start with `/`) or with `native.NativeFile.artifact_lease_key_prefix`
//! (`"\x00AFLEASE/"`, and in a different catalog root besides). `namespace`
//! is one of `PersistentIndex`'s three fixed, NUL-free keyspace names
//! (`segments`/`meta`/`deletions`) or empty for the unnamed namespace, so
//! splitting on the *first* NUL after the scope separator unambiguously
//! recovers `namespace` even when `key` itself contains embedded NULs.

const std = @import("std");
const Allocator = std.mem.Allocator;
const docstore = @import("docstore.zig");
const native = @import("native.zig");
const native_catalog_backend = @import("../native_catalog_backend.zig");

pub const reserved_prefix = "\x00ncat/";

/// Exposed so WS4 phase 2's inline single-commit path (`lite/docstore.zig`'s
/// `Txn.putIndexCatalogBatch`) can encode durable index-catalog keys the
/// exact same way `Sink.submit`'s own `ApplyCtx.apply` does, whichever path
/// a given write takes.
pub fn durableKeyAlloc(allocator: Allocator, scope: []const u8, namespace: ?[]const u8, key: []const u8) ![]u8 {
    return try std.mem.concat(allocator, u8, &.{
        reserved_prefix,
        scope,
        &.{0},
        namespace orelse "",
        &.{0},
        key,
    });
}

pub fn scopePrefixAlloc(allocator: Allocator, scope: []const u8) ![]u8 {
    return try std.mem.concat(allocator, u8, &.{ reserved_prefix, scope, &.{0} });
}

/// Splits a durable key known to start with `reserved_prefix ++ scope ++
/// "\x00"` back into `(namespace, key)`. Returns `null` if `full_key` does
/// not belong to `scope` at all (defensive; callers are expected to have
/// already prefix-matched via a catalog cursor).
fn decodeAlloc(allocator: Allocator, scope: []const u8, full_key: []const u8, value: []const u8) !?native_catalog_backend.Entry {
    const head_len = reserved_prefix.len + scope.len + 1;
    if (full_key.len < head_len) return null;
    if (!std.mem.startsWith(u8, full_key, reserved_prefix)) return null;
    if (!std.mem.eql(u8, full_key[reserved_prefix.len .. reserved_prefix.len + scope.len], scope)) return null;
    if (full_key[reserved_prefix.len + scope.len] != 0) return null;
    const remainder = full_key[head_len..];
    const sep = std.mem.indexOfScalar(u8, remainder, 0) orelse return error.InvalidNativeCatalogKey;
    const ns = remainder[0..sep];
    const key = remainder[sep + 1 ..];
    const owned_ns = if (ns.len == 0) null else try allocator.dupe(u8, ns);
    errdefer if (owned_ns) |n| allocator.free(n);
    const owned_key = try allocator.dupe(u8, key);
    errdefer allocator.free(owned_key);
    return .{
        .namespace = owned_ns,
        .key = owned_key,
        .value = try allocator.dupe(u8, value),
    };
}

const ApplyCtx = struct {
    scope: []const u8,
    mutations: []const native_catalog_backend.Mutation,
    allocator: Allocator,

    fn apply(ptr: *anyopaque, file: *native.NativeFile) anyerror!void {
        const self: *ApplyCtx = @ptrCast(@alignCast(ptr));
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        const batch = try a.alloc(native.CatalogMutation, self.mutations.len);
        for (self.mutations, batch) |m, *out| {
            out.* = .{
                .key = try durableKeyAlloc(a, self.scope, m.namespace, m.key),
                .value = m.value,
                .is_delete = m.is_delete,
            };
        }
        // One call: every mutation this transaction accumulated lands in the
        // same native commit (one `syncData`/checkpoint publication) as
        // whatever else `submitIndexMutation`'s group-commit queue already
        // batched with it.
        try file.putIndexCatalogBatch(batch);
    }
};

/// Deep-copies `items` so the caller's own buffer (`NamespaceTxn.pending`'s
/// arena) may be freed the moment `submitDeferred` returns, independent of
/// when the staged group-commit it joins actually applies. Mirrors
/// `sparse/sparse.zig`'s `dupeOwnedSparseMutationsAlloc` for the same
/// `native_catalog_backend.Mutation` shape.
fn dupeMutationsAlloc(allocator: Allocator, items: []const native_catalog_backend.Mutation) ![]native_catalog_backend.Mutation {
    const out = try allocator.alloc(native_catalog_backend.Mutation, items.len);
    errdefer allocator.free(out);
    var filled: usize = 0;
    errdefer freeMutationFields(allocator, out[0..filled]);
    for (items, 0..) |m, i| {
        out[i] = .{
            .namespace = if (m.namespace) |n| try allocator.dupe(u8, n) else null,
            .key = try allocator.dupe(u8, m.key),
            .value = if (m.is_delete) "" else try allocator.dupe(u8, m.value),
            .is_delete = m.is_delete,
        };
        filled = i + 1;
    }
    return out;
}

fn freeMutationFields(allocator: Allocator, items: []const native_catalog_backend.Mutation) void {
    for (items) |m| {
        if (m.namespace) |n| allocator.free(n);
        allocator.free(m.key);
        if (!m.is_delete) allocator.free(m.value);
    }
}

fn freeDupedMutations(allocator: Allocator, items: []const native_catalog_backend.Mutation) void {
    freeMutationFields(allocator, items);
    allocator.free(items);
}

/// Heap-owned context for a ride-along (`durable = false`) commit staged
/// via `Store.stageRideAlong` without waiting (`Sink.submitDeferred`).
/// Unlike the stack-allocated `ApplyCtx` above (valid only for the duration
/// of a blocking `submit` call), this must stay valid from `submitDeferred`
/// until whichever thread eventually drains the group-commit queue calls
/// `apply` -- arbitrarily later, possibly after `submitDeferred` has long
/// since returned -- so it owns a deep copy of `scope`/`mutations` and frees
/// itself only once, in `wait`, after that `apply` has definitely already
/// run (`Store.Ticket.wait` only returns once its group is durable/failed).
const DeferredApplyCtx = struct {
    allocator: Allocator,
    scope: []u8,
    mutations: []native_catalog_backend.Mutation,
    ticket: docstore.Store.Ticket,

    fn apply(ptr: *anyopaque, file: *native.NativeFile) anyerror!void {
        const self: *DeferredApplyCtx = @ptrCast(@alignCast(ptr));
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        const batch = try a.alloc(native.CatalogMutation, self.mutations.len);
        for (self.mutations, batch) |m, *out| {
            out.* = .{
                .key = try durableKeyAlloc(a, self.scope, m.namespace, m.key),
                .value = m.value,
                .is_delete = m.is_delete,
            };
        }
        try file.putIndexCatalogBatch(batch);
    }

    /// `native_catalog_backend.Pending.waitFn`: waits for this ride-along
    /// commit's group, then frees this context -- called exactly once, by
    /// whichever caller `wait()`s the `Pending` `submitDeferred` returned
    /// (directly, or via `Backend.pending_ride_along`'s batched drain).
    fn wait(ptr: *anyopaque) anyerror!void {
        const self: *DeferredApplyCtx = @ptrCast(@alignCast(ptr));
        defer {
            freeDupedMutations(self.allocator, self.mutations);
            self.allocator.free(self.scope);
            self.allocator.destroy(self);
        }
        return self.ticket.wait();
    }
};

pub const Sink = struct {
    store: *docstore.Store,
    allocator: Allocator,

    pub fn sink(self: *Sink) native_catalog_backend.Sink {
        return .{
            .ptr = self,
            .submit = submit,
            .loadAllAlloc = loadAllAlloc,
            .loadKeysAlloc = loadKeysAlloc,
            .deleteScope = deleteScope,
            .submitDeferred = submitDeferred,
            .pauseMaintenance = pauseMaintenance,
            .resumeMaintenance = resumeMaintenance,
        };
    }

    /// perf/lwa-maint-serialize: see `native_catalog_backend.Sink
    /// .pauseMaintenance`'s own doc comment.
    fn pauseMaintenance(ptr: *anyopaque) void {
        const self: *Sink = @ptrCast(@alignCast(ptr));
        self.store.beginMultiCommitSequence();
    }

    fn resumeMaintenance(ptr: *anyopaque) void {
        const self: *Sink = @ptrCast(@alignCast(ptr));
        self.store.endMultiCommitSequence();
    }

    fn submit(ptr: *anyopaque, scope: []const u8, mutations: []const native_catalog_backend.Mutation, durable: bool) anyerror!void {
        const self: *Sink = @ptrCast(@alignCast(ptr));
        if (mutations.len == 0) return;
        var ctx = ApplyCtx{ .scope = scope, .mutations = mutations, .allocator = self.allocator };
        // `durable` threads straight through to docstore.Store's own
        // group-commit queue, exactly as lite/index_storage.zig's
        // appendFileAbsolute already does for its own caller-supplied `sync`
        // flag: `durable=false` makes the mutation visible through the next
        // native checkpoint read without forcing its own `syncData` (see
        // native.zig's commitTransactionWithDurability's deferred-
        // publication path) -- the ride-along commit this backend's
        // `NamespaceTxn.setDurable(false)` asks for.
        try self.store.submitIndexMutationForScope(scope, &ctx, ApplyCtx.apply, durable);
    }

    /// `native_catalog_backend.Sink.submitDeferred`: like `submit` with
    /// `durable = false`, but never blocks. Routes through
    /// `Store.stageRideAlong` directly (not `submitIndexMutation`, which
    /// always leads -- and closes -- its own one-member group via
    /// `Store.stage`, exactly the "11 metric-build passes -> 11 commits"
    /// problem the design doc's §3.2a "group membership, not standalone
    /// commits" note describes): this lets several ride-along commits
    /// issued back to back (one per background maintenance pass) join the
    /// SAME eventual group-commit instead of each leading its own.
    fn submitDeferred(ptr: *anyopaque, scope: []const u8, mutations: []const native_catalog_backend.Mutation) anyerror!native_catalog_backend.Pending {
        const self: *Sink = @ptrCast(@alignCast(ptr));
        // A maintenance snapshot's index content must not ride along: a
        // later group can carry the serving store's own index commits, built
        // from state that does not include this one. Commit it now, under the
        // writer admission that fences stale snapshot publications.
        if (self.store.write_target != null) {
            try submit(ptr, scope, mutations, false);
            return .{ .ptr = undefined, .waitFn = completedWait };
        }
        const ctx = try self.allocator.create(DeferredApplyCtx);
        errdefer self.allocator.destroy(ctx);
        const owned_scope = try self.allocator.dupe(u8, scope);
        errdefer self.allocator.free(owned_scope);
        const owned_mutations = try dupeMutationsAlloc(self.allocator, mutations);
        errdefer freeDupedMutations(self.allocator, owned_mutations);
        ctx.* = .{
            .allocator = self.allocator,
            .scope = owned_scope,
            .mutations = owned_mutations,
            .ticket = undefined,
        };
        // `stageRideAlong` only enqueues (it never leads, so this never
        // blocks on another group's fsync, or on this one's); `ctx.ticket`
        // is written here, on this thread, before `submitDeferred` returns
        // the `Pending` that exposes it -- `apply` above never reads
        // `ctx.ticket`, so there is no race with whichever other thread
        // eventually drains the queue and calls it.
        ctx.ticket = try self.store.stageRideAlong(.{
            .context = ctx,
            .apply = DeferredApplyCtx.apply,
            .index_only = true,
        });
        return .{ .ptr = ctx, .waitFn = DeferredApplyCtx.wait };
    }

    fn completedWait(_: *anyopaque) anyerror!void {}

    fn deleteScope(ptr: *anyopaque, scope: []const u8) anyerror!void {
        const self: *Sink = @ptrCast(@alignCast(ptr));
        const prefix = try scopePrefixAlloc(self.allocator, scope);
        defer self.allocator.free(prefix);
        const Request = struct {
            prefix: []const u8,
            fn run(context: *anyopaque, file: *native.NativeFile) !void {
                const request: *@This() = @ptrCast(@alignCast(context));
                try file.deleteIndexCatalogPrefix(request.prefix, null);
            }
        };
        var request = Request{ .prefix = prefix };
        try self.store.submitIndexMutationForScope(scope, &request, Request.run, true);
    }

    fn loadAllAlloc(ptr: *anyopaque, allocator: Allocator, scope: []const u8) anyerror![]native_catalog_backend.Entry {
        return loadEntriesAlloc(ptr, allocator, scope, false);
    }

    fn loadKeysAlloc(ptr: *anyopaque, allocator: Allocator, scope: []const u8) anyerror![]native_catalog_backend.Entry {
        return loadEntriesAlloc(ptr, allocator, scope, true);
    }

    fn loadEntriesAlloc(ptr: *anyopaque, allocator: Allocator, scope: []const u8, comptime keys_only: bool) ![]native_catalog_backend.Entry {
        const self: *Sink = @ptrCast(@alignCast(ptr));
        // Unconditional: a maintenance snapshot's DB loads its indexes during
        // open, before the snapshot is given its write target.
        self.store.noteLoadedIndexScope(scope);
        var txn = try self.store.beginRead();
        defer txn.abort();
        const file = try txn.readFile();

        const prefix = try scopePrefixAlloc(allocator, scope);
        defer allocator.free(prefix);

        var cursor = try file.indexCatalogCursor(txn.checkpoint, prefix);
        defer cursor.deinit();

        var result = std.ArrayListUnmanaged(native_catalog_backend.Entry).empty;
        errdefer {
            for (result.items) |e| {
                if (e.namespace) |n| allocator.free(n);
                allocator.free(e.key);
                allocator.free(e.value);
            }
            result.deinit(allocator);
        }
        while (true) {
            try result.ensureUnusedCapacity(allocator, 1);
            if (keys_only) {
                const record = (try cursor.next()) orelse break;
                defer file.allocator.free(record.key);
                if (try decodeAlloc(allocator, scope, record.key, "")) |entry| result.appendAssumeCapacity(entry);
            } else {
                const record = (try cursor.nextRecordAlloc(allocator)) orelse break;
                defer allocator.free(record.key);
                defer allocator.free(record.value);
                if (try decodeAlloc(allocator, scope, record.key, record.value)) |entry| result.appendAssumeCapacity(entry);
            }
        }
        return try result.toOwnedSlice(allocator);
    }
};

test "native catalog sink key encoding round-trips through decode" {
    const a = std.testing.allocator;
    const key = try durableKeyAlloc(a, "/lite-text/full_text_index_v0/index", "segments", "\x00\x01\x02binary-key");
    defer a.free(key);

    const decoded = try decodeAlloc(a, "/lite-text/full_text_index_v0/index", key, "value-bytes");
    try std.testing.expect(decoded != null);
    defer {
        if (decoded.?.namespace) |n| a.free(n);
        a.free(decoded.?.key);
        a.free(decoded.?.value);
    }
    try std.testing.expectEqualStrings("segments", decoded.?.namespace.?);
    try std.testing.expectEqualStrings("\x00\x01\x02binary-key", decoded.?.key);
    try std.testing.expectEqualStrings("value-bytes", decoded.?.value);
}

test "native catalog sink key encoding handles the unnamed namespace" {
    const a = std.testing.allocator;
    const key = try durableKeyAlloc(a, "scope-a", null, "plain-key");
    defer a.free(key);

    const decoded = try decodeAlloc(a, "scope-a", key, "v");
    try std.testing.expect(decoded != null);
    defer {
        a.free(decoded.?.key);
        a.free(decoded.?.value);
    }
    try std.testing.expect(decoded.?.namespace == null);
    try std.testing.expectEqualStrings("plain-key", decoded.?.key);
}

test "native catalog sink key encoding disambiguates scopes that share a prefix" {
    const a = std.testing.allocator;
    const key_a = try durableKeyAlloc(a, "a", "meta", "k");
    defer a.free(key_a);
    const key_ab = try durableKeyAlloc(a, "ab", "meta", "k");
    defer a.free(key_ab);

    try std.testing.expect(!std.mem.eql(u8, key_a, key_ab));
    try std.testing.expect(try decodeAlloc(a, "ab", key_a, "v") == null);
    if (try decodeAlloc(a, "a", key_ab, "v")) |decoded| {
        // Must not decode as a ("ab"'s scope prefix match for "a" fails
        // because the byte right after "a" is 'b', not the NUL separator).
        defer {
            if (decoded.namespace) |n| a.free(n);
            a.free(decoded.key);
            a.free(decoded.value);
        }
        return error.TestUnexpectedResult;
    }
}

test "native catalog sink: a mid-stream index admission is visible to loadAllAlloc before any checkpoint" {
    // §9.7c: this is the exact seam every full-text/dense/sparse/graph
    // native-catalog managed index seeds its in-process mirror through
    // at open (`Backend.init`'s own `loadAllAlloc` call) and the exact
    // seam every write afterward goes through (`submit`) -- simulating
    // "an index admits a record mid-stream, with no checkpoint" at the
    // layer the redo-log overlay fix in this branch actually covers.
    //
    // The real end-to-end path (`db.addIndex` actually creating a
    // dense/sparse/graph index) currently fails for an unrelated reason
    // on its very first call -- a write-path LogFull/OutcomeUnknown bug
    // in index creation's own `deleteTree` (`installIndexForReconciliation`
    // -> `addWithAtomicMutation` -> `addWithOptions` ->
    // `prepareStorageForFreshCatalogEntry` -> `deleteTree`, via
    // `submitIndexMutation`'s own `waitSequence`), tracked separately
    // from this branch's own read-path work; see this branch's report.
    // This test exercises the read half of that scenario directly,
    // through `submitIndexMutation`/`loadAllAlloc` rather than through
    // the currently-broken creation path.
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/ncat-sink-mid-stream.aflite", .{tmp.sub_path});
    defer a.free(path);

    var store = try docstore.Store.createWithOptions(a, path, .{ .use_redo_log = true, .io = std.testing.io });
    defer store.close();

    var sink: Sink = .{ .store = &store, .allocator = a };
    const iface = sink.sink();
    const scope = "/dense-index-1";

    try iface.submit(iface.ptr, scope, &.{
        .{ .namespace = "segments", .key = "range:0001", .value = "seg-one" },
        .{ .namespace = "segments", .key = "range:0002", .value = "seg-two" },
        .{ .namespace = "meta", .key = "dims", .value = "768" },
    }, true);

    const findEntry = struct {
        fn run(entries: []const native_catalog_backend.Entry, namespace: []const u8, key: []const u8) ?[]const u8 {
            for (entries) |e| {
                if (e.namespace == null) continue;
                if (std.mem.eql(u8, e.namespace.?, namespace) and std.mem.eql(u8, e.key, key)) return e.value;
            }
            return null;
        }
    }.run;
    const freeEntries = struct {
        fn run(alloc: Allocator, entries: []native_catalog_backend.Entry) void {
            for (entries) |e| {
                if (e.namespace) |n| alloc.free(n);
                alloc.free(e.key);
                alloc.free(e.value);
            }
            alloc.free(entries);
        }
    }.run;

    // No checkpoint call anywhere above: every one of those three
    // records lives only in the redo-log overlay right now. Before the
    // `OverlayScanState` gate fix in this branch, a fresh `beginRead`'s
    // separately-opened `generation.file` silently saw zero records
    // here whenever the checkpoint it was handed (tracked, genuinely
    // current) didn't match `generation.file`'s own frozen snapshot --
    // exactly the same overlay-blind-scan bug `docstore.Cursor` hit.
    const loaded = try iface.loadAllAlloc(iface.ptr, a, scope);
    defer freeEntries(a, loaded);
    try std.testing.expectEqual(@as(usize, 3), loaded.len);
    try std.testing.expectEqualStrings("seg-one", findEntry(loaded, "segments", "range:0001").?);
    try std.testing.expectEqualStrings("seg-two", findEntry(loaded, "segments", "range:0002").?);
    try std.testing.expectEqualStrings("768", findEntry(loaded, "meta", "dims").?);

    // A follow-up write -- delete one record, overwrite none, admit a
    // new one -- still with no checkpoint. The overlay must merge
    // put/delete here exactly like the plain document/catalog scans do.
    try iface.submit(iface.ptr, scope, &.{
        .{ .namespace = "segments", .key = "range:0001", .is_delete = true },
        .{ .namespace = "segments", .key = "range:0003", .value = "seg-three" },
    }, true);

    const reloaded = try iface.loadAllAlloc(iface.ptr, a, scope);
    defer freeEntries(a, reloaded);
    try std.testing.expectEqual(@as(usize, 3), reloaded.len);
    try std.testing.expectEqual(@as(?[]const u8, null), findEntry(reloaded, "segments", "range:0001"));
    try std.testing.expectEqualStrings("seg-two", findEntry(reloaded, "segments", "range:0002").?);
    try std.testing.expectEqualStrings("seg-three", findEntry(reloaded, "segments", "range:0003").?);
    try std.testing.expectEqualStrings("768", findEntry(reloaded, "meta", "dims").?);

    const keys = try iface.loadKeysAlloc.?(iface.ptr, a, scope);
    defer freeEntries(a, keys);
    try std.testing.expectEqual(reloaded.len, keys.len);
    for (keys) |entry| {
        try std.testing.expectEqualStrings("", entry.value);
        try std.testing.expect(findEntry(reloaded, entry.namespace.?, entry.key) != null);
    }
}
