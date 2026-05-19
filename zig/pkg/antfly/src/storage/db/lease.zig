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
const backend_erased = @import("../backend_erased.zig");
const lsm_backend = @import("../lsm_backend.zig");
const platform_time = @import("../../platform/time.zig");
const docstore_mod = @import("../docstore.zig");
const mem_backend = @import("../mem_backend.zig");

pub const LeaseRecord = struct {
    owner_id: []const u8,
    expires_at_ms: u64,
};

pub const Lease = struct {
    allocator: Allocator,
    store: RuntimeStoreHandle,
    key: []const u8,

    pub fn init(alloc: Allocator, store: anytype, key: []const u8) !Lease {
        return .{
            .allocator = alloc,
            .store = try initRuntimeStore(alloc, store),
            .key = key,
        };
    }

    pub fn deinit(self: *Lease) void {
        self.store.deinit();
        self.* = undefined;
    }

    pub fn load(self: *Lease, alloc: Allocator) !?LeaseRecord {
        var txn = try self.store.store.beginRead();
        defer txn.abort();
        const raw = txn.get(self.key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const data = try alloc.dupe(u8, raw);
        defer alloc.free(data);

        const parsed = try std.json.parseFromSlice(LeaseRecord, alloc, data, .{
            .allocate = .alloc_always,
        });
        defer parsed.deinit();
        return try cloneRecord(alloc, parsed.value);
    }

    pub fn tryAcquire(self: *Lease, owner_id: []const u8, now_ms: u64, ttl_ms: u64) !bool {
        var txn = try self.store.store.beginWrite();
        var committed = false;
        defer if (!committed) txn.abort();

        const current_raw = txn.get(self.key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };

        if (current_raw) |raw| {
            const parsed = try std.json.parseFromSlice(LeaseRecord, self.allocator, raw, .{
                .allocate = .alloc_always,
            });
            defer parsed.deinit();

            const current = parsed.value;
            if (current.expires_at_ms > now_ms and !std.mem.eql(u8, current.owner_id, owner_id)) {
                return false;
            }
        }

        const payload = try std.json.Stringify.valueAlloc(self.allocator, LeaseRecord{
            .owner_id = owner_id,
            .expires_at_ms = now_ms + ttl_ms,
        }, .{});
        defer self.allocator.free(payload);

        try txn.put(self.key, payload);
        try txn.commit();
        committed = true;
        return true;
    }

    pub fn renew(self: *Lease, owner_id: []const u8, now_ms: u64, ttl_ms: u64) !bool {
        return try self.tryAcquire(owner_id, now_ms, ttl_ms);
    }

    pub fn release(self: *Lease, owner_id: []const u8) !bool {
        var txn = try self.store.store.beginWrite();
        var committed = false;
        defer if (!committed) txn.abort();

        const current_raw = txn.get(self.key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        const parsed = try std.json.parseFromSlice(LeaseRecord, self.allocator, current_raw, .{
            .allocate = .alloc_always,
        });
        defer parsed.deinit();

        if (!std.mem.eql(u8, parsed.value.owner_id, owner_id)) return false;
        try txn.delete(self.key);
        try txn.commit();
        committed = true;
        return true;
    }
};

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

pub fn cloneRecord(alloc: Allocator, record: LeaseRecord) !LeaseRecord {
    return .{
        .owner_id = try alloc.dupe(u8, record.owner_id),
        .expires_at_ms = record.expires_at_ms,
    };
}

pub fn deinitRecord(alloc: Allocator, record: *LeaseRecord) void {
    alloc.free(record.owner_id);
    record.* = undefined;
}

var temp_path_nonce: u64 = 0;

fn tempPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-db-lease-test-";
    const ts = platform_time.monotonicNs();
    const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
    const path = std.fmt.bufPrint(buf, "{s}{d}-{d}\x00", .{ base, ts, nonce }) catch unreachable;
    return @ptrCast(path.ptr);
}

fn cleanupTempDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "lease acquires renews and releases by owner" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var store = try docstore_mod.DocStore.open(alloc, path, .{});
    defer store.close();

    var lease = try Lease.init(alloc, &store, "\x00\x00__metadata__:lease_test");
    defer lease.deinit();
    try std.testing.expect(try lease.tryAcquire("worker-a", 1000, 250));
    try std.testing.expect(!(try lease.tryAcquire("worker-b", 1100, 250)));
    try std.testing.expect(try lease.renew("worker-a", 1200, 250));
    try std.testing.expect(!(try lease.release("worker-b")));
    try std.testing.expect(try lease.release("worker-a"));
    try std.testing.expect(try lease.tryAcquire("worker-b", 1300, 250));
}

test "lease works with memory backend store" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    var lease = try Lease.init(alloc, runtime, "\x00\x00__metadata__:lease_test");
    defer lease.deinit();

    try std.testing.expect(try lease.tryAcquire("worker-a", 1000, 250));
    try std.testing.expect(!(try lease.tryAcquire("worker-b", 1100, 250)));
    try std.testing.expect(try lease.release("worker-a"));
    try std.testing.expect(try lease.tryAcquire("worker-b", 1300, 250));
}

test "lease works with lsm backend store" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    var lease = try Lease.init(alloc, runtime, "\x00\x00__metadata__:lease_test");
    defer lease.deinit();

    try std.testing.expect(try lease.tryAcquire("worker-a", 1000, 250));
    try std.testing.expect(!(try lease.tryAcquire("worker-b", 1100, 250)));
    try std.testing.expect(try lease.release("worker-a"));
    try std.testing.expect(try lease.tryAcquire("worker-b", 1300, 250));
}
