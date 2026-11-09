// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One leased reader per loaded decoder. A decoder handle must pin this cache
//! throughout every borrow. Request tensors/KV never enter the retained entry.
const std = @import("std");
const reader = @import("../models/memorilla_runtime.zig");
const memory = @import("../runtime/root.zig").tier.memory;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const A = std.mem.Allocator;

pub const Identity = struct {
    path: [32]u8,
    generation: [32]u8,
};

/// Crypto validation remains mandatory on misses. Hits fence replacement,
/// mutation, symlink retargeting and directory changes without hashing 388 MB.
pub fn identity(a: A, io: std.Io, directory: []const u8, control: Control) !Identity {
    try control.check();
    const canonical = try std.Io.Dir.cwd().realPathFileAlloc(io, directory, a);
    defer a.free(canonical);
    var path_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(canonical, &path_hash, .{});
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.memorilla.reader-generation.v1\x00");
    hash.update(&path_hash);
    for ([_][]const u8{ ".", "manifest.json", "config.json", "memory.safetensors" }) |name| {
        try control.check();
        const path = try std.fs.path.join(a, &.{ directory, name });
        defer a.free(path);
        const real = try std.Io.Dir.cwd().realPathFileAlloc(io, path, a);
        defer a.free(real);
        hash.update(name);
        hash.update("\x00");
        hash.update(real);
        hash.update("\x00");
        const stat = try std.Io.Dir.cwd().statFile(io, path, .{});
        hash.update(std.mem.asBytes(&stat.inode));
        hash.update(std.mem.asBytes(&stat.size));
        const mtime: i128 = stat.mtime.toNanoseconds();
        const ctime: i128 = stat.ctime.toNanoseconds();
        hash.update(std.mem.asBytes(&mtime));
        hash.update(std.mem.asBytes(&ctime));
    }
    try control.check();
    return .{ .path = path_hash, .generation = hash.finalResult() };
}

pub const Cache = ReaderCache(reader.Owner);

fn ReaderCache(comptime Owner: type) type {
    return struct {
        const Self = @This();
        const Entry = struct { owner: Owner, lease: memory.AdmissionLease, identity: Identity, invalid: bool = false };
        mutex: std.atomic.Mutex = .unlocked,
        entry: ?Entry = null,

        pub const Handle = struct {
            cache: *Self,
            allocator: A,
            io: std.Io,
            directory: []const u8,

            pub fn owner(self: *const Handle) *Owner {
                return &self.cache.entry.?.owner;
            }

            pub fn invalidate(self: *Handle) void {
                self.cache.entry.?.invalid = true;
            }

            /// Mark drift while borrowed, then retire only after request GPU
            /// scopes have drained and Handle.deinit releases the borrow.
            pub fn verify(self: *Handle, control: Control) !void {
                const observed = identity(self.allocator, self.io, self.directory, control) catch |err| switch (err) {
                    error.FileNotFound, error.NotDir => {
                        self.cache.entry.?.invalid = true;
                        return error.ModelArtifactsChanging;
                    },
                    else => return err,
                };
                if (!std.meta.eql(self.cache.entry.?.identity, observed)) {
                    self.cache.entry.?.invalid = true;
                    return error.ModelArtifactsChanging;
                }
            }

            pub fn deinit(self: *Handle) void {
                if (self.cache.entry.?.invalid) self.cache.retire();
                self.cache.mutex.unlock();
                self.* = undefined;
            }
        };

        fn retire(self: *Self) void {
            if (self.entry) |*entry| {
                // Physical destruction must precede the admission refund.
                entry.owner.deinit();
                entry.lease.release();
                self.entry = null;
            }
        }

        /// Called only by the decoder's final owner, after all pins drain.
        pub fn deinit(self: *Self) void {
            self.retire();
            self.* = undefined;
        }

        fn inspect(self: *Self, a: A, io: std.Io, directory: []const u8, limits: memory.Limits, control: Control) !Identity {
            const observed = identity(a, io, directory, control) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => {
                    if (self.entry != null) {
                        self.retire();
                        return error.ModelArtifactsChanging;
                    }
                    return err;
                },
                else => return err,
            };
            if (self.entry) |*entry| {
                if (std.mem.eql(u8, &entry.identity.path, &observed.path)) {
                    if (!std.meta.eql(entry.identity, observed)) {
                        self.retire();
                        return error.ModelArtifactsChanging;
                    }
                    try reader.requireAdmission(&entry.lease, limits);
                } else self.retire(); // A profile switch keeps only one reader.
            }
            return observed;
        }

        /// No manager admission/eviction occurs under the cache mutex. A
        /// racing miss can reserve a temporary second lease, then reuse the
        /// winner and immediately refund its unused reservation.
        pub fn acquire(self: *Self, a: A, io: std.Io, manager: anytype, directory: []const u8, limits: memory.Limits, control: Control) !Handle {
            try reader.requireLimits(limits);
            try control.lock(&self.mutex);
            const first = self.inspect(a, io, directory, limits, control) catch |err| {
                self.mutex.unlock();
                return err;
            };
            if (self.entry != null) return .{ .cache = self, .allocator = a, .io = io, .directory = directory };
            self.mutex.unlock();
            var lease = try manager.acquireRunResourceAmounts(.gpu, limits, reader.weightPlan());
            var transferred = false;
            defer if (!transferred) lease.release();
            try control.lock(&self.mutex);
            errdefer self.mutex.unlock();
            const before = try self.inspect(a, io, directory, limits, control);
            if (!std.meta.eql(first, before)) return error.ModelArtifactsChanging;
            if (self.entry == null) {
                var owner = try Owner.open(a, io, directory, &lease, limits, control);
                errdefer owner.deinit();
                const after = try identity(a, io, directory, control);
                if (!std.meta.eql(before, after)) return error.ModelArtifactsChanging;
                self.entry = .{ .owner = owner, .lease = lease, .identity = after };
                transferred = true;
            }
            return .{ .cache = self, .allocator = a, .io = io, .directory = directory };
        }
    };
}

const TestOwner = struct {
    controller: *memory.AdmissionController,
    var opened: usize = 0;
    var closed: usize = 0;
    var fail_load: bool = false;

    pub fn open(_: A, _: std.Io, _: []const u8, lease: *memory.AdmissionLease, limits: memory.Limits, _: Control) !TestOwner {
        try reader.requireAdmission(lease, limits);
        if (fail_load) return error.TestReaderLoadFailure;
        opened += 1;
        return .{ .controller = lease.controller.? };
    }
    pub fn deinit(self: *TestOwner) void {
        std.debug.assert(self.controller.snapshot().backend_weight_bytes >= reader.parameter_bytes);
        closed += 1;
    }
};

const TestCache = ReaderCache(TestOwner);
const TestManager = struct {
    controller: memory.AdmissionController = .{},
    cache_mutex: *std.atomic.Mutex,
    admissions: usize = 0,

    pub fn acquireRunResourceAmounts(self: *@This(), class: memory.BackendClass, limits: memory.Limits, amounts: memory.AdmissionAmounts) !memory.AdmissionLease {
        // Admission may evict models: the production cache cannot hold its
        // mutex while entering the manager. Exercise that lock rule here.
        if (!self.cache_mutex.tryLock()) return error.TestAdmissionUnderCacheLock;
        self.cache_mutex.unlock();
        self.admissions += 1;
        return self.controller.tryAcquire(class, limits, amounts, false);
    }
};

test "Memorilla reader cache retains bounded admission, fences drift and refunds failed loads" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    for ([_][]const u8{ "a", "b" }) |dir| {
        try temp.dir.createDir(io, dir, .default_dir);
        for ([_][]const u8{ "manifest.json", "config.json", "memory.safetensors" }) |name| {
            const path = try std.fs.path.join(a, &.{ dir, name });
            defer a.free(path);
            try temp.dir.writeFile(io, .{ .sub_path = path, .data = "mock immutable bytes" });
        }
    }
    const first = try temp.dir.realPathFileAlloc(io, "a", a);
    defer a.free(first);
    const second = try temp.dir.realPathFileAlloc(io, "b", a);
    defer a.free(second);
    const limits = memory.Limits{ .host_limit_bytes = 4 << 30, .backend_limit_bytes = 4 << 30, .combined_limit_bytes = 6 << 30 };
    var cache = TestCache{};
    var manager = TestManager{ .cache_mutex = &cache.mutex };
    defer manager.controller.deinit();
    defer cache.deinit();
    TestOwner.opened = 0;
    TestOwner.closed = 0;
    TestOwner.fail_load = false;

    // One owner/lease survives request release; a hit performs no admission.
    var cold = try cache.acquire(a, io, &manager, first, limits, .{});
    try cold.verify(.{});
    cold.deinit();
    try std.testing.expectEqual(@as(usize, 1), TestOwner.opened);
    try std.testing.expectEqual(reader.parameter_bytes, manager.controller.snapshot().backend_weight_bytes);
    var hit = try cache.acquire(a, io, &manager, first, limits, .{});
    hit.deinit();
    try std.testing.expectEqual(@as(usize, 1), manager.admissions);
    var tight = limits;
    tight.backend_limit_bytes = 1;
    try std.testing.expectError(error.ResourceLimitExceeded, cache.acquire(a, io, &manager, first, tight, .{}));
    try std.testing.expectEqual(@as(usize, 0), TestOwner.closed);
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, cache.acquire(a, io, &manager, first, limits, .{ .check_fn = Cancel.check }));
    try std.testing.expectEqual(reader.parameter_bytes, manager.controller.snapshot().backend_weight_bytes);

    // A profile switch retires before reserving its replacement; capacity
    // never grows with the number of profiles sharing this decoder.
    var switched = try cache.acquire(a, io, &manager, second, limits, .{});
    switched.deinit();
    try std.testing.expectEqual(@as(usize, 2), TestOwner.opened);
    try std.testing.expectEqual(@as(usize, 1), TestOwner.closed);
    try std.testing.expectEqual(reader.parameter_bytes, manager.controller.snapshot().backend_weight_bytes);

    try temp.dir.writeFile(io, .{ .sub_path = "b/config.json", .data = "changed config" });
    try std.testing.expectError(error.ModelArtifactsChanging, cache.acquire(a, io, &manager, second, limits, .{}));
    try std.testing.expectEqual(@as(usize, 2), TestOwner.closed);
    try std.testing.expectEqual(@as(usize, 0), manager.controller.snapshot().backend_weight_bytes);
    TestOwner.fail_load = true;
    try std.testing.expectError(error.TestReaderLoadFailure, cache.acquire(a, io, &manager, second, limits, .{}));
    try std.testing.expectEqual(@as(usize, 0), manager.controller.snapshot().backend_weight_bytes);
    TestOwner.fail_load = false;

    // In-flight drift must not free an owner while GPU scopes still borrow it.
    var changed = try cache.acquire(a, io, &manager, second, limits, .{});
    try temp.dir.writeFile(io, .{ .sub_path = "b/extra.json", .data = "{}" });
    try std.testing.expectError(error.ModelArtifactsChanging, changed.verify(.{}));
    try std.testing.expectEqual(@as(usize, 2), TestOwner.closed);
    _ = changed.owner();
    changed.deinit();
    try std.testing.expectEqual(@as(usize, 3), TestOwner.closed);
    try std.testing.expectEqual(@as(usize, 0), manager.controller.snapshot().backend_weight_bytes);
}
