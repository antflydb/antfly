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

//! Store-wide durable admission. Reserve before any upload/link; failed and
//! uncertain publications stay charged until maintenance proves reclamation.
//! Equal extent inventories share their byte charge, but every issued binding
//! consumes a slot. Changes are conservative: partially shared cuts may charge
//! the same extent twice. Reader authority is never revoked to make room.
const std = @import("std");
const objectstore = @import("objectstore");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const Namespace = local.storage_db_doc_identity_namespace.Namespace;
const Limits = local.storage_db_native_query_cut.Limits;
pub const Entry = struct {
    id: [64]u8,
    namespace: Namespace,
    expires_ms: u64,
    bytes: u64,
    inventory: [32]u8,
    linked: bool,
};
pub const Position = struct { namespace: Namespace, after: [64]u8 };
pub const ScanUpdate = struct { namespace: Namespace, expected_after: ?[64]u8, after: [64]u8 };
pub const Scan = struct { entries: []const Entry, after: ?[64]u8 };
const Ledger = struct { version: u16 = 1, entries: []const Entry = &.{}, positions: []const Position = &.{} };
const Identity = struct { namespace: Namespace, id: [64]u8 };
const max_ledger_bytes = 4 * 1024 * 1024;
const Bridge = struct {
    token: Cancellation,
    fn cancelled(raw: *const anyopaque) bool {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        self.token.check() catch return true;
        return false;
    }
    fn object(self: *const @This()) objectstore.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = cancelled };
    }
};
pub const Store = struct {
    client: objectstore.Client,
    bucket: []const u8,
    key: []const u8,
    limits: Limits,
    fn read(self: Store, a: A, token: Cancellation) !?objectstore.GetResult {
        var client = self.client;
        client.allocator = a;
        const bridge: Bridge = .{ .token = token };
        return client.getObject(self.bucket, self.key, .{ .max_response_bytes = max_ledger_bytes, .cancellation = bridge.object() }) catch |err| switch (err) {
            error.FileNotFound, error.NotFound, error.ObjectNotFound => null,
            else => return err,
        };
    }
    fn parse(a: A, saved: ?objectstore.GetResult) !Ledger {
        const ledger = if (saved) |value| try std.json.parseFromSliceLeaky(Ledger, a, value.body, .{ .allocate = .alloc_always }) else Ledger{};
        if (ledger.version != 1 or ledger.entries.len > 4096 or ledger.positions.len > 4096) return error.CatalogGenerationChanged;
        return ledger;
    }
    fn save(self: Store, a: A, entries: []const Entry, positions: []const Position, saved: ?objectstore.GetResult, token: Cancellation) !void {
        const bytes = try std.json.Stringify.valueAlloc(a, Ledger{ .entries = entries, .positions = positions }, .{});
        if (bytes.len > max_ledger_bytes) return error.QueryCandidateBudgetExceeded;
        var client = self.client;
        client.allocator = a;
        const bridge: Bridge = .{ .token = token };
        var result = try client.compareAndPut(self.bucket, self.key, bytes, if (saved) |value| value.metadata.writeRevision() orelse return error.MissingObjectRevision else null, .{ .cancellation = bridge.object() });
        result.deinit(a);
    }
    pub fn reserve(self: Store, allocator: A, entry: Entry, token: Cancellation) !void {
        attempts: for (0..16) |_| {
            try token.check();
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const a = arena.allocator();
            var saved = try self.read(a, token);
            defer if (saved) |*value| value.deinit(a);
            const ledger = try parse(a, saved);
            for (ledger.entries, 0..) |old, index| {
                if (old.namespace.eql(entry.namespace) and std.mem.eql(u8, &old.id, &entry.id)) {
                    var comparable = old;
                    comparable.linked = entry.linked;
                    if (!std.meta.eql(comparable, entry)) return error.CatalogGenerationChanged;
                    if (old.linked and !entry.linked) {
                        // Cross-device link fallback may upload chunks. Extend
                        // its reclamation proof requirement before that upload.
                        const updated = try a.dupe(Entry, ledger.entries);
                        updated[index] = entry;
                        self.save(a, updated, ledger.positions, saved, token) catch |err| switch (err) {
                            error.PreconditionFailed, error.ConditionalCheckFailed, error.ObjectAlreadyExists => continue :attempts,
                            else => return err,
                        };
                        return;
                    }
                    return; // Idempotent retry even after limits were reduced.
                }
            }
            if (ledger.entries.len >= self.limits.max_cuts or ledger.entries.len >= 4096) return error.QueryCandidateBudgetExceeded;
            var inventories: std.AutoHashMap([32]u8, u64) = .init(a);
            var total: u64 = 0;
            for (ledger.entries) |old| {
                const slot = try inventories.getOrPut(old.inventory);
                if (slot.found_existing) {
                    if (slot.value_ptr.* != old.bytes) return error.CatalogGenerationChanged;
                } else {
                    slot.value_ptr.* = old.bytes;
                    total = std.math.add(u64, total, old.bytes) catch return error.QueryCandidateBudgetExceeded;
                }
            }
            if (inventories.get(entry.inventory)) |bytes| {
                if (bytes != entry.bytes) return error.CatalogGenerationChanged;
            } else total = std.math.add(u64, total, entry.bytes) catch return error.QueryCandidateBudgetExceeded;
            if (total > self.limits.max_bytes) return error.QueryCandidateBudgetExceeded;
            const entries = try a.alloc(Entry, ledger.entries.len + 1);
            @memcpy(entries[0..ledger.entries.len], ledger.entries);
            entries[ledger.entries.len] = entry;
            self.save(a, entries, ledger.positions, saved, token) catch |err| switch (err) {
                error.PreconditionFailed, error.ConditionalCheckFailed, error.ObjectAlreadyExists => continue,
                else => return err,
            };
            return;
        }
        return error.QueryCandidateBudgetExceeded;
    }
    /// Maintenance proves reclamation before calling this method. Compare
    /// exact entries again on every retry: a stale proof must not remove a
    /// changed reservation, nor may a stale scanner rewind another collector.
    pub fn release(self: Store, allocator: A, entry: Entry, token: Cancellation) !void {
        return self.releaseBatch(allocator, &.{entry}, null, token);
    }
    pub fn releaseBatch(self: Store, allocator: A, proven: []const Entry, update: ?ScanUpdate, token: Cancellation) !void {
        if (proven.len == 0 and update == null) return;
        for (0..16) |_| {
            try token.check();
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const a = arena.allocator();
            var saved = try self.read(a, token);
            defer if (saved) |*value| value.deinit(a);
            const ledger = try parse(a, saved);
            var proofs: std.AutoHashMap(Identity, Entry) = .init(a);
            for (proven) |entry| try proofs.put(.{ .namespace = entry.namespace, .id = entry.id }, entry);
            var entries: std.ArrayList(Entry) = .empty;
            var namespaces: std.AutoHashMap(Namespace, void) = .init(a);
            for (ledger.entries) |old| {
                if (proofs.get(.{ .namespace = old.namespace, .id = old.id })) |proof| {
                    if (std.meta.eql(old, proof)) continue;
                }
                try entries.append(a, old);
                try namespaces.put(old.namespace, {});
            }
            var changed = entries.items.len != ledger.entries.len;
            var positions: std.ArrayList(Position) = .empty;
            var found = false;
            for (ledger.positions) |old| {
                if (!namespaces.contains(old.namespace)) {
                    changed = true;
                    continue;
                }
                var position = old;
                if (update) |scan_update| if (old.namespace.eql(scan_update.namespace)) {
                    found = true;
                    if (std.meta.eql(@as(?[64]u8, old.after), scan_update.expected_after) and !std.meta.eql(old.after, scan_update.after)) {
                        position.after = scan_update.after;
                        changed = true;
                    }
                };
                try positions.append(a, position);
            }
            if (update) |scan_update| if (!found and scan_update.expected_after == null and namespaces.contains(scan_update.namespace)) {
                try positions.append(a, .{ .namespace = scan_update.namespace, .after = scan_update.after });
                changed = true;
            };
            if (!changed) return;
            self.save(a, entries.items, positions.items, saved, token) catch |err| switch (err) {
                error.PreconditionFailed, error.ConditionalCheckFailed, error.ObjectAlreadyExists => continue,
                else => return err,
            };
            return;
        }
        return error.QueryCandidateBudgetExceeded;
    }
    pub fn scan(self: Store, a: A, namespace: Namespace, token: Cancellation) !Scan {
        var saved = try self.read(a, token);
        defer if (saved) |*value| value.deinit(a);
        const ledger = try parse(a, saved);
        var after: ?[64]u8 = null;
        for (ledger.positions) |position| if (position.namespace.eql(namespace)) {
            after = position.after;
            break;
        };
        return .{ .entries = ledger.entries, .after = after };
    }
    pub fn snapshot(self: Store, a: A, token: Cancellation) ![]const Entry {
        var saved = try self.read(a, token);
        defer if (saved) |*value| value.deinit(a);
        return (try parse(a, saved)).entries;
    }
};
