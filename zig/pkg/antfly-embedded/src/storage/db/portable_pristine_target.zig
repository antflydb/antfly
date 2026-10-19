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

//! Bounded logical probes for pristine portable-publication targets.
const std = @import("std");
const DocStore = @import("../docstore.zig").DocStore;
const read_admission = @import("../../common/read_admission.zig");
const keys = @import("../internal_keys.zig");

pub const Catalog = struct { schema_present: bool, indexes_allowed: bool, enrichments_present: bool, resolvers_present: bool };
fn present(read: *DocStore.Txn, key: []const u8, control: ?read_admission.Port) !bool {
    if (control) |port| {
        var values: [1]?[]const u8 = .{null};
        try read.getManySortedWithAdmission(&.{key}, &values, port);
        try port.checkpoint();
        return values[0] != null;
    }
    return read.contains(key);
}
fn prefixPresent(read: *DocStore.Txn, prefix: []const u8, control: ?read_admission.Port) !bool {
    var cursor = if (control) |port| try read.openPhysicalCursorWithAdmission(port) else try read.openPhysicalCursorAdapter();
    defer cursor.close();
    const entry = if (control) |port| try cursor.seekAtOrAfterWithAdmission(prefix, port) else try cursor.seekAtOrAfter(prefix);
    if (control) |port| try port.checkpoint();
    return if (entry) |row| std.mem.startsWith(u8, row.key, prefix) else false;
}

/// Caller holds apply; no CPU suspension is allowed while inspecting the
/// source. Provider work remains subject to its original per-call admission.
pub fn check(store: *DocStore, alloc: std.mem.Allocator, catalog: Catalog, control: ?read_admission.Port) !bool {
    if (control) |port| {
        // Apply and policy are held. Reject lending before any checkpoint can
        // transfer CPU while those dependencies remain owned.
        if (try port.hasExecutionWait()) return error.ExecutionWaitUnsupported;
        try port.checkpoint();
    }
    var point = if (control) |port| try store.beginProbeTxnWithAdmission(.transient, port) else try store.beginProbeTxn();
    defer point.abort();
    if (try present(&point, @import("artifact_reconcile_intent.zig").key, control)) return error.IntegrityTopologyBusy;
    if (catalog.schema_present or !catalog.indexes_allowed) return false;
    var scan = if (control) |port| try store.beginCurrentScanTxnWithAdmission(port) else try store.beginCurrentScanTxn();
    defer scan.abort();
    const user_prefix = [_]u8{keys.user_namespace};
    if (try prefixPresent(&scan, &user_prefix, control)) return false;
    if (!try @import("doc_identity.zig").pristinePortableIdentityFromRead(alloc, &scan, control)) return false;
    if (try prefixPresent(&scan, @import("../transactions.zig").records_prefix, control)) return false;
    if (catalog.enrichments_present or catalog.resolvers_present) return false;
    if (try present(&point, "\x00\x00__metadata__:schema_json", control)) return false;
    if (control) |port| try port.checkpoint();
    return true;
}

pub fn qualify() !void {
    if (!@import("builtin").is_test) @compileError("test fixture only");
    const identity = @import("doc_identity.zig");
    const mem = @import("../mem_backend.zig");
    const native = @import("../../common/runnable_scheduler.zig");
    const Apply = @import("apply_rw_lock.zig").ApplyRwLock;
    const Gate = @import("row_policy_gate.zig").Gate;
    const Collection = @import("mutation_admission.zig").IntentCollection;
    const a = std.testing.allocator;
    const clean: Catalog = .{ .schema_present = false, .indexes_allowed = true, .enrichments_present = false, .resolvers_present = false };
    const Fixture = struct {
        const metadata_keys = [_][]const u8{ &keys.identity_namespace_key, &keys.identity_next_ordinal_key, &keys.identity_visibility_summary_key, &keys.identity_visibility_manifest_key, &keys.identity_live_directory_manifest_key };
        fn populate(store: *DocStore, mask: usize, bad: ?usize, oversized: bool) !void {
            var namespace: [24]u8 = undefined;
            identity.encodeNamespace(&namespace, .{ .table_id = 1, .shard_id = 2, .range_id = 3 });
            const next = [_]u8{ 0, 0, 0, 1 };
            const summary: [40]u8 = @splat(0);
            const manifest = [_]u8{ 1, 0, 0, 0, 0, 1, 0, 0 };
            const invalid: [41]u8 = @splat(0);
            const values = [_][]const u8{ &namespace, &next, &summary, &manifest, keys.identity_live_directory_manifest_value };
            var write = try store.runtime_store.beginWrite();
            errdefer write.abort();
            for (metadata_keys, values, 0..) |key, value, i| {
                if (mask & (@as(usize, 1) << @intCast(i)) == 0) continue;
                try write.put(key, if (bad == i) if (oversized) &invalid else "" else value);
            }
            try write.commit();
        }
        fn legacyIdentity(alloc: std.mem.Allocator, store: *DocStore) !bool {
            identity.validateStoreAlloc(alloc, store) catch |err| switch (err) {
                error.InvalidDocIdentity => return false,
                else => return err,
            };
            const stats = try identity.fullStatsFromStore(store);
            return stats.next_ordinal == 1 and stats.allocated_ordinals == 0 and stats.state_rows == 0 and stats.live_ordinals == 0 and stats.tombstone_ordinals == 0 and stats.visibility_chunks == 0 and stats.visibility_deleted_ordinals == 0 and stats.visibility_mask_bytes == 0 and stats.visibility_repair_count == 0;
        }
        fn oom(alloc: std.mem.Allocator) !void {
            var backend = mem.Backend.init(alloc, .{});
            defer backend.close();
            var store = try DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
            defer store.close();
            try populate(&store, 31, null, false);
            const catalog: Catalog = .{ .schema_present = false, .indexes_allowed = true, .enrichments_present = false, .resolvers_present = false };
            try std.testing.expect(try check(&store, alloc, catalog, null));
        }
    };
    // All supported metadata-presence combinations preserve the prior verdict.
    for (0..32) |mask| {
        var backend = mem.Backend.init(a, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        try Fixture.populate(&store, mask, null, false);
        const expected = try Fixture.legacyIdentity(a, &store);
        try std.testing.expectEqual(expected, try check(&store, a, clean, null));
    }
    for (0..Fixture.metadata_keys.len) |bad| {
        for ([_]bool{ false, true }) |oversized| {
            var backend = mem.Backend.init(a, .{});
            defer backend.close();
            var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
            defer store.close();
            try Fixture.populate(&store, 31, bad, oversized);
            const old = Fixture.legacyIdentity(a, &store);
            const current = check(&store, a, clean, null);
            if (old) |allowed| {
                if (current) |now| try std.testing.expect(!now or allowed) else |_| {}
            } else |_| {
                if (current) |now| try std.testing.expect(!now) else |_| {}
            }
        }
    }
    // Every disqualifying catalog participates, and the durable reconcile
    // marker takes priority even when a catalog already rejects the target.
    for (0..4) |field| {
        var backend = mem.Backend.init(a, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var catalog = clean;
        switch (field) {
            0 => catalog.schema_present = true,
            1 => catalog.indexes_allowed = false,
            2 => catalog.enrichments_present = true,
            3 => catalog.resolvers_present = true,
            else => unreachable,
        }
        try std.testing.expect(!try check(&store, a, catalog, null));
        try store.put(@import("artifact_reconcile_intent.zig").key, "opaque");
        try std.testing.expectError(error.IntegrityTopologyBusy, check(&store, a, catalog, null));
    }
    for ([_][]const u8{
        "\x01row",                          @import("../transactions.zig").records_prefix ++ "txn",
        "\x00\x00__metadata__:schema_json", "\x03\xfeunknown",
    }) |key| {
        var backend = mem.Backend.init(a, .{});
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        var write = try store.runtime_store.beginWrite();
        errdefer write.abort();
        try write.put(key, "opaque");
        try write.commit();
        try std.testing.expect(!try check(&store, a, clean, null));
    }
    const Budget = struct {
        execution: *native.Execution,
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn life(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == self.fail_at) return self.cause;
            if (self.calls > 256) return error.UnboundedPristineProbe;
            try self.execution.checkLifetime();
        }
        fn transition(_: *anyopaque) !void {
            return error.UnexpectedPristineCpuTransfer;
        }
        fn controls(self: *@This()) Apply.Acquisition {
            return .{ .lifetime = .{ .context = self, .check_fn = life }, .execution_wait = .{ .ptr = self, .begin = transition, .end = transition } };
        }
    };
    var backend = mem.Backend.init(a, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try Fixture.populate(&store, 31, null, false);
    var apply: Apply = .{ .io = std.testing.io };
    apply.lockExclusive();
    defer apply.unlockExclusive();
    var gate: Gate = .{};
    var policy = try gate.enterRaw();
    defer policy.release();
    var scheduler = try native.Scheduler.init(.{ .slots = 2 });
    defer scheduler.close();
    var execution: native.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23 }, .write);
    var budget: Budget = .{ .execution = &execution };
    var collection: Collection = .{};
    const port = collection.bindPreDecisionBeforeMutation(budget.controls(), std.testing.io, null).?;
    try std.testing.expect(try check(&store, a, clean, port));
    const completed_checks = budget.calls;
    for ([_]anyerror{ error.NotFound, error.TxnNotFound, error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.InvalidDocIdentity }) |cause| {
        for (1..completed_checks + 1) |fail_at| {
            budget.calls = 0;
            budget.fail_at = fail_at;
            budget.cause = cause;
            try std.testing.expectError(cause, check(&store, a, clean, port));
            try std.testing.expectEqual(@as(usize, 0), store.portable_import_reader_state.load(.acquire));
        }
    }
    budget.fail_at = std.math.maxInt(usize);
    budget.calls = 0;
    try std.testing.expect(backend.mutex.tryLock());
    try std.testing.expectError(error.Backpressured, check(&store, a, clean, port));
    backend.mutex.unlock();
    budget.calls = 0;
    try std.testing.expect(try check(&store, a, clean, port));
    var health: native.Execution = .{};
    try health.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
    health.finish();
    // A large disqualifying history needs one seek, not a history traversal.
    {
        var write = try store.runtime_store.beginWrite();
        errdefer write.abort();
        for (0..10000) |i| {
            var key: [6]u8 = .{ keys.identity_namespace, keys.identity_ordinal_state_kind, 0, 0, 0, 0 };
            std.mem.writeInt(u32, key[2..6], @intCast(i + 1), .big);
            try write.put(&key, "opaque");
        }
        try write.commit();
    }
    budget.calls = 0;
    try std.testing.expect(!try check(&store, a, clean, port));
    try std.testing.expect(budget.calls < completed_checks);
    try std.testing.expectEqual(@as(usize, 0), store.portable_import_reader_state.load(.acquire));
    try std.testing.expect(!apply.tryLockExclusive());
    try std.testing.expectEqual(@as(usize, 1), gate.readers.load(.acquire));
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    try std.testing.checkAllAllocationFailures(a, Fixture.oom, .{});
}
