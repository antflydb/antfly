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

fn scopePrefixAlloc(allocator: Allocator, scope: []const u8) ![]u8 {
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
    return .{
        .namespace = if (ns.len == 0) null else try allocator.dupe(u8, ns),
        .key = try allocator.dupe(u8, key),
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

pub const Sink = struct {
    store: *docstore.Store,
    allocator: Allocator,

    pub fn sink(self: *Sink) native_catalog_backend.Sink {
        return .{ .ptr = self, .submit = submit, .loadAllAlloc = loadAllAlloc };
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
        try self.store.submitIndexMutation(&ctx, ApplyCtx.apply, durable);
    }

    fn loadAllAlloc(ptr: *anyopaque, allocator: Allocator, scope: []const u8) anyerror![]native_catalog_backend.Entry {
        const self: *Sink = @ptrCast(@alignCast(ptr));
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
        while (try cursor.nextRecordAlloc(allocator)) |record| {
            defer allocator.free(record.key);
            defer allocator.free(record.value);
            if (try decodeAlloc(allocator, scope, record.key, record.value)) |entry| {
                try result.append(allocator, entry);
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
