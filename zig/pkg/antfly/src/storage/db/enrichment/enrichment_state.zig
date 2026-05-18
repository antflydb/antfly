// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const docstore_mod = @import("../../docstore.zig");
const lsm_backend = @import("../../lsm_backend.zig");
const mem_backend = @import("../../mem_backend.zig");

const applied_seq_prefix = "\x00\x00__metadata__:enrichment_applied:";
const runtime_status_prefix = "\x00\x00__metadata__:enrichment_status:";

pub const RuntimeStatus = struct {
    target_sequence: u64 = 0,
    error_count: u64 = 0,
    retryable_error_count: u64 = 0,
    fatal_error_count: u64 = 0,
    retrying: bool = false,
    worker_failed: bool = false,
};

fn appliedSequenceKey(alloc: Allocator, scope: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ applied_seq_prefix, scope });
}

fn runtimeStatusKey(alloc: Allocator, scope: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ runtime_status_prefix, scope });
}

pub fn loadAppliedSequence(alloc: Allocator, store: anytype, scope: []const u8) !u64 {
    const key = try appliedSequenceKey(alloc, scope);
    defer alloc.free(key);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginRead();
    defer txn.abort();
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);
    if (raw.len != 8) return error.InvalidEnrichmentState;
    return std.mem.readInt(u64, raw[0..8], .little);
}

pub fn saveAppliedSequence(store: anytype, scope: []const u8, sequence: u64) !void {
    const key = try appliedSequenceKey(std.heap.page_allocator, scope);
    defer std.heap.page_allocator.free(key);
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, sequence, .little);
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, &buf);
    try txn.commit();
}

pub fn loadRuntimeStatus(alloc: Allocator, store: anytype, scope: []const u8) !RuntimeStatus {
    const key = try runtimeStatusKey(alloc, scope);
    defer alloc.free(key);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginRead();
    defer txn.abort();
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);
    if (raw.len != 34) return error.InvalidEnrichmentState;
    return .{
        .target_sequence = std.mem.readInt(u64, raw[0..8], .little),
        .error_count = std.mem.readInt(u64, raw[8..16], .little),
        .retryable_error_count = std.mem.readInt(u64, raw[16..24], .little),
        .fatal_error_count = std.mem.readInt(u64, raw[24..32], .little),
        .retrying = raw[32] != 0,
        .worker_failed = raw[33] != 0,
    };
}

pub fn saveRuntimeStatus(store: anytype, scope: []const u8, status: RuntimeStatus) !void {
    const key = try runtimeStatusKey(std.heap.page_allocator, scope);
    defer std.heap.page_allocator.free(key);
    var buf: [34]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], status.target_sequence, .little);
    std.mem.writeInt(u64, buf[8..16], status.error_count, .little);
    std.mem.writeInt(u64, buf[16..24], status.retryable_error_count, .little);
    std.mem.writeInt(u64, buf[24..32], status.fatal_error_count, .little);
    buf[32] = if (status.retrying) 1 else 0;
    buf[33] = if (status.worker_failed) 1 else 0;
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, &buf);
    try txn.commit();
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

test "enrichment apply state works with memory backend store" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
    try saveAppliedSequence(runtime, "chunks", 19);
    try std.testing.expectEqual(@as(u64, 19), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
}

test "enrichment apply state works with lsm backend store" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
    try saveAppliedSequence(runtime, "chunks", 23);
    try std.testing.expectEqual(@as(u64, 23), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
}

test "enrichment runtime status persists source target sequence" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveRuntimeStatus(runtime, "generated", .{
        .target_sequence = 7,
        .error_count = 1,
        .retryable_error_count = 2,
        .fatal_error_count = 3,
        .retrying = true,
    });
    const loaded = try loadRuntimeStatus(std.testing.allocator, runtime, "generated");
    try std.testing.expectEqual(@as(u64, 7), loaded.target_sequence);
    try std.testing.expectEqual(@as(u64, 1), loaded.error_count);
    try std.testing.expectEqual(@as(u64, 2), loaded.retryable_error_count);
    try std.testing.expectEqual(@as(u64, 3), loaded.fatal_error_count);
    try std.testing.expect(loaded.retrying);
    try std.testing.expect(!loaded.worker_failed);
}
