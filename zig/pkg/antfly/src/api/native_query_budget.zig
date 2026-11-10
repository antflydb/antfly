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
const Ledger = struct { version: u16 = 1, entries: []const Entry = &.{} };
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
        if (ledger.version != 1 or ledger.entries.len > 4096) return error.CatalogGenerationChanged;
        return ledger;
    }
    fn save(self: Store, a: A, entries: []const Entry, saved: ?objectstore.GetResult, token: Cancellation) !void {
        const bytes = try std.json.Stringify.valueAlloc(a, Ledger{ .entries = entries }, .{});
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
                        self.save(a, updated, saved, token) catch |err| switch (err) {
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
            self.save(a, entries, saved, token) catch |err| switch (err) {
                error.PreconditionFailed, error.ConditionalCheckFailed, error.ObjectAlreadyExists => continue,
                else => return err,
            };
            return;
        }
        return error.QueryCandidateBudgetExceeded;
    }
    /// Maintenance checks physical reclamation outside the CAS. A removed ID
    /// cannot be republished: requests have already expired before this call.
    pub fn release(self: Store, allocator: A, entry: Entry, token: Cancellation) !void {
        for (0..16) |_| {
            try token.check();
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const a = arena.allocator();
            var saved = try self.read(a, token);
            defer if (saved) |*value| value.deinit(a);
            const ledger = try parse(a, saved);
            var entries: std.ArrayList(Entry) = .empty;
            for (ledger.entries) |old| {
                if (std.meta.eql(old, entry)) continue;
                try entries.append(a, old);
            }
            if (entries.items.len == ledger.entries.len) return;
            self.save(a, entries.items, saved, token) catch |err| switch (err) {
                error.PreconditionFailed, error.ConditionalCheckFailed, error.ObjectAlreadyExists => continue,
                else => return err,
            };
            return;
        }
        return error.QueryCandidateBudgetExceeded;
    }
    pub fn snapshot(self: Store, a: A, token: Cancellation) ![]const Entry {
        var saved = try self.read(a, token);
        defer if (saved) |*value| value.deinit(a);
        return (try parse(a, saved)).entries;
    }
};
