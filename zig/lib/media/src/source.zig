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

const std = @import("std");
const admission = @import("admission.zig");

/// Provider-defined cancellation/deadline check. Also passed into each read so
/// a blocking provider can check the same original deadline while doing I/O.
pub const Control = struct {
    context: ?*const anyopaque = null,
    check_fn: ?*const fn (?*const anyopaque) anyerror!void = null,
    pub fn check(self: Control) !void {
        if (self.check_fn) |callback| try callback(self.context);
    }
};
pub const ReadControl = struct {
    source: Control,
    worker: Control,
    pub fn control(self: *const ReadControl) Control {
        if (self.source.check_fn == null) return self.worker;
        if (self.worker.check_fn == null or (self.source.check_fn == self.worker.check_fn and self.source.context == self.worker.context)) return self.source;
        return .{ .context = self, .check_fn = check };
    }
    fn check(raw: ?*const anyopaque) !void {
        const self: *const ReadControl = @ptrCast(@alignCast(raw.?));
        try self.source.check();
        try self.worker.check();
    }
};
pub const Limits = struct {
    max_read_bytes: usize = 16 * 1024 * 1024,
    max_retained_bytes: usize = 32 * 1024 * 1024,
    max_total_bytes: u64 = 64 * 1024 * 1024,
    max_reads: usize = 100_000,
};
pub const Range = struct {
    /// Opt in when callbacks can run simultaneously. Callbacks that allocate
    /// from the source backing must use its rangeAllocator() for that work.
    concurrent: bool = false,
    /// Content digest or immutable object/version binding. Providers supplying
    /// this must validate that every returned range belongs to that snapshot.
    /// A display URL or unverified ETag is insufficient.
    immutable_binding: ?[32]u8 = null,
    /// Immutable local extents may lend a contiguous read without allocating.
    /// Null requests the ordinary read_at path (for example across extents).
    borrow_at: ?*const fn (*anyopaque, u64, usize, Control) anyerror!?[]const u8 = null,
    context: *anyopaque,
    /// May return a short read. Returning zero before the declared end is EOF.
    read_at: *const fn (*anyopaque, u64, []u8, Control) anyerror!usize,
    sparse_read_at: ?*const fn (*anyopaque, u64, []u8, Control) anyerror!usize = null,
    length: u64,
};
pub const Storage = union(enum) { borrowed: []const u8, range: Range };

/// Serializes work on a shared source while allowing a waiting consumer to cancel.
pub const Gate = struct {
    state: std.atomic.Value(u32) = .init(0),
    pub fn lock(self: *Gate, control: ?Control) !void {
        if (self.state.cmpxchgStrong(0, 1, .acquire, .monotonic) == null) return;
        const io = std.Io.Threaded.global_single_threaded.io();
        // A canceled waiter can consume the unlock wake before it acquires
        // the gate. Pass that wake to another parked allocator or provider.
        errdefer std.Io.futexWake(io, u32, &self.state.raw, 1);
        while (true) {
            if (control) |c| try c.check();
            if (self.state.swap(2, .acquire) == 0) return;
            if (control != null) {
                try std.Io.futexWaitTimeout(io, u32, &self.state.raw, 2, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } });
            } else std.Io.futexWaitUncancelable(io, u32, &self.state.raw, 2);
        }
    }

    pub fn unlock(self: *Gate) void {
        if (self.state.swap(0, .release) == 2)
            std.Io.futexWake(std.Io.Threaded.global_single_threaded.io(), u32, &self.state.raw, 1);
    }
};

/// A source is caller-owned and stays in place while leases or views exist.
/// Give each worker a view to keep per-consumer I/O statistics.
/// Provider context and borrowed bytes must outlive this source and its leases.
/// Identity must name immutable content (e.g. an object version or digest).
pub const Source = struct {
    allocator: std.mem.Allocator,
    storage: Storage,
    identity: []const u8,
    limits: Limits = .{},
    control: Control = .{},
    admission_pool: ?*admission.Pool = null,
    retained_bytes: usize = 0,
    total_bytes: u64 = 0,
    reads: usize = 0,

    pending_bytes: u64 = 0,
    provider_gate: Gate = .{},
    allocator_gate: Gate = .{},
    physical: ?*Source = null,
    upstream_control: ?*const Source = null,
    mutex: std.Io.Mutex = .init,

    /// A worker keeps its allocator, cancellation and local statistics. All
    /// descendants charge successful and partial I/O to the same physical owner.
    /// The owner must outlive every view and its leases.
    pub fn view(self: *Source, allocator: std.mem.Allocator, pool: ?*admission.Pool, control: Control) Source {
        const root = self.physical orelse self;
        // Preserve adapter synchronization when a worker passes the same raw
        // backing allocator. A distinct worker allocator keeps its own ownership.
        const a = RangeAllocator.forWorker(root.allocator, allocator);
        return .{ .allocator = a, .storage = self.storage, .identity = self.identity, .limits = self.limits, .control = control, .admission_pool = pool, .physical = self.physical orelse self };
    }
    /// Check cancellation even when a consumer can serve from cached data.
    pub fn checkControl(self: *const Source) !void {
        const owner = self.physical orelse self;
        const checks = ReadControl{ .source = owner.control, .worker = self.control };
        try checks.control().check();
        if (owner.upstream_control) |upstream| try upstream.checkControl();
    }

    pub fn length(self: *const Source) u64 {
        return switch (self.storage) {
            .borrowed => |b| b.len,
            .range => |r| r.length,
        };
    }
    pub fn read(self: *Source, offset: u64, size: usize) !Lease {
        return self.readControlled(offset, size, .{});
    }
    pub fn readReclaiming(self: *Source, offset: u64, size: usize, reclaimer: ?admission.Reclaimer) !Lease {
        return self.readWithControl(offset, size, .{}, reclaimer, false);
    }
    pub fn readControlled(self: *Source, offset: u64, size: usize, caller: Control) !Lease {
        return self.readWithControl(offset, size, caller, null, false);
    }
    /// Reads a small or isolated span without the transport's normal read-ahead.
    pub fn readSparse(self: *Source, offset: u64, size: usize) !Lease {
        return self.readSparseControlled(offset, size, .{});
    }
    pub fn readSparseControlled(self: *Source, offset: u64, size: usize, control: Control) !Lease {
        return self.readWithControl(offset, size, control, null, true);
    }
    fn readWithControl(self: *Source, offset: u64, size: usize, caller: Control, reclaimer: ?admission.Reclaimer, sparse: bool) !Lease {
        const owner = self.physical orelse self;
        const io = std.Io.Threaded.global_single_threaded.io();
        const controls = ReadControl{ .source = owner.control, .worker = self.control };
        const combined = ReadControl{ .source = controls.control(), .worker = caller };
        const control = combined.control();
        try control.check();
        if (owner.upstream_control) |upstream| try upstream.checkControl();
        if (offset > self.length() or size > self.length() - offset) return error.UnexpectedEndOfSource;
        if (self.storage == .borrowed) {
            var token = try self.admitBytes(size, reclaimer);
            errdefer token.deinit();
            owner.mutex.lockUncancelable(io);
            defer owner.mutex.unlock(io);
            if (!self.canRead(size) or !owner.canRead(size)) return error.ResourceLimitExceeded;
            self.chargeBorrowed(size);
            if (owner != self) owner.chargeBorrowed(size);
            return .{ .bytes = self.storage.borrowed[@intCast(offset)..][0..size], .source = self, .reservation = token };
        }
        // Mutable providers retain serial semantics. Poll only a contended gate
        // so a waiting worker can observe a provider-independent deadline.
        const serial = !self.storage.range.concurrent;
        try self.reserveRead(owner, size);
        var done: usize = 0;
        var first_call_used = false;
        defer {
            owner.mutex.lockUncancelable(io);
            defer owner.mutex.unlock(io);
            self.settleRead(size, done, first_call_used);
            if (owner != self) owner.settleRead(size, done, first_call_used);
        }
        errdefer self.releaseBytes(size);
        var reservation = try self.admitBytes(size, reclaimer);
        errdefer reservation.deinit();
        const range = self.storage.range;
        const read_at = if (sparse) range.sparse_read_at orelse range.read_at else range.read_at;
        if (range.borrow_at) |borrow| {
            if (serial) try owner.provider_gate.lock(control);
            defer if (serial) owner.provider_gate.unlock();
            const result = borrow(range.context, offset, size, control) catch |err| {
                first_call_used = true;
                return err;
            };
            if (result) |bytes| {
                first_call_used = true;
                if (bytes.len != size) return error.InvalidSourceRead;
                done = size;
                try control.check();
                return .{ .bytes = bytes, .source = self, .reservation = reservation };
            }
        }
        const a = self.rangeAllocator();
        const owned = RangeAllocator.readBuffer(a, size, control) catch |err| retry: {
            if (err != error.OutOfMemory or !admission.Reclaimer.reclaim(reclaimer)) return err;
            break :retry try RangeAllocator.readBuffer(a, size, control);
        };
        errdefer a.free(owned);
        if (serial) try owner.provider_gate.lock(control);
        defer if (serial) owner.provider_gate.unlock();
        while (done < size) {
            try control.check();
            if (first_call_used) try self.chargeCall(owner);
            first_call_used = true;
            const n = try read_at(range.context, offset + done, owned[done..], control);
            if (n == 0) return error.UnexpectedEndOfSource;
            if (n > size - done) return error.InvalidSourceRead;
            done += n;
        }
        try control.check();
        return .{ .bytes = owned, .source = self, .owned = owned, .reservation = reservation };
    }
    /// Range adapters may share the backing allocator with the original callbacks.
    /// Coordinate their buffers, scratch and lease releases with the physical source.
    /// Borrowed storage promises immutable bytes but makes no allocator guarantee.
    pub fn rangeAllocator(self: *Source) std.mem.Allocator {
        if (self.allocator.vtable == &RangeAllocator.vtable) return self.allocator;
        return .{ .ptr = self, .vtable = &RangeAllocator.vtable };
    }

    const RangeAllocator = struct {
        const vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
        fn forWorker(original: std.mem.Allocator, worker: std.mem.Allocator) std.mem.Allocator {
            if (original.vtable != &vtable) return worker;
            const source: *Source = @ptrCast(@alignCast(original.ptr));
            return if (std.meta.eql(worker, source.allocator)) original else worker;
        }
        fn readBuffer(a: std.mem.Allocator, size: usize, control: Control) ![]u8 {
            if (a.vtable != &vtable) return a.alloc(u8, size);
            const src: *Source = @ptrCast(@alignCast(a.ptr));
            const gate = src.physical orelse src;
            try gate.allocator_gate.lock(control);
            defer gate.allocator_gate.unlock();
            return src.allocator.alloc(u8, size);
        }
        fn lock(raw: *anyopaque) *Source {
            const src: *Source = @ptrCast(@alignCast(raw));
            (src.physical orelse src).allocator_gate.lock(null) catch unreachable;
            return src;
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const src = lock(raw);
            defer (src.physical orelse src).allocator_gate.unlock();
            return src.allocator.rawAlloc(len, alignment, ra);
        }
        fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
            const src = lock(raw);
            defer (src.physical orelse src).allocator_gate.unlock();
            return src.allocator.rawResize(bytes, alignment, len, ra);
        }
        fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
            const src = lock(raw);
            defer (src.physical orelse src).allocator_gate.unlock();
            return src.allocator.rawRemap(bytes, alignment, len, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const src = lock(raw);
            defer (src.physical orelse src).allocator_gate.unlock();
            src.allocator.rawFree(bytes, alignment, ra);
        }
    };
    fn admitBytes(self: *Source, size: usize, reclaimer: ?admission.Reclaimer) !admission.Token {
        return if (self.admission_pool) |pool| pool.acquire(.{ .host_bytes = size }) catch |err| retry: {
            if (!admission.Reclaimer.reclaim(reclaimer)) return err;
            break :retry try pool.acquire(.{ .host_bytes = size });
        } else admission.Token{};
    }
    fn chargeBorrowed(self: *Source, size: usize) void {
        self.reads += 1;
        self.total_bytes += size;
        self.retained_bytes += size;
    }
    fn reserveRead(self: *Source, owner: *Source, size: usize) !void {
        const io = std.Io.Threaded.global_single_threaded.io();
        owner.mutex.lockUncancelable(io);
        defer owner.mutex.unlock(io);
        if (!self.canRead(size) or !owner.canRead(size)) return error.ResourceLimitExceeded;
        self.pending_bytes += size;
        self.reads += 1;
        self.retained_bytes += size;
        if (owner != self) {
            owner.pending_bytes += size;
            owner.reads += 1;
            owner.retained_bytes += size;
        }
    }
    fn chargeCall(self: *Source, owner: *Source) !void {
        const io = std.Io.Threaded.global_single_threaded.io();
        owner.mutex.lockUncancelable(io);
        defer owner.mutex.unlock(io);
        if (self.reads >= self.limits.max_reads or owner.reads >= owner.limits.max_reads) return error.ResourceLimitExceeded;
        self.reads += 1;
        if (owner != self) owner.reads += 1;
    }
    fn settleRead(self: *Source, size: usize, transferred: usize, first_call_used: bool) void {
        if (!first_call_used) self.reads -= 1;
        self.pending_bytes -= size;
        self.total_bytes += transferred;
    }
    fn releaseBytes(self: *Source, size: usize) void {
        const owner = self.physical orelse self;
        const io = std.Io.Threaded.global_single_threaded.io();
        owner.mutex.lockUncancelable(io);
        defer owner.mutex.unlock(io);
        self.retained_bytes -= size;
        if (owner != self) owner.retained_bytes -= size;
    }
    /// Capacity available to a physical read at this instant, including other
    /// workers' leases. read() verifies the limits again under the same lock.
    pub fn remaining(self: *Source) Limits {
        const owner = self.physical orelse self;
        const io = std.Io.Threaded.global_single_threaded.io();
        owner.mutex.lockUncancelable(io);
        defer owner.mutex.unlock(io);
        return .{
            .max_read_bytes = @min(self.limits.max_read_bytes, owner.limits.max_read_bytes),
            .max_retained_bytes = @min(self.limits.max_retained_bytes -| self.retained_bytes, owner.limits.max_retained_bytes -| owner.retained_bytes),
            .max_total_bytes = @min(self.limits.max_total_bytes -| self.total_bytes -| self.pending_bytes, owner.limits.max_total_bytes -| owner.total_bytes -| owner.pending_bytes),
            .max_reads = @min(self.limits.max_reads -| self.reads, owner.limits.max_reads -| owner.reads),
        };
    }
    fn canRead(self: *const Source, size: usize) bool {
        return size <= self.limits.max_read_bytes and size <= self.limits.max_retained_bytes -| self.retained_bytes and
            size <= self.limits.max_total_bytes -| self.total_bytes -| self.pending_bytes and self.reads < self.limits.max_reads;
    }
};
/// Move-only by convention. Independent leases survive subsequent reads. Release
/// each exactly once; the source must outlive them. A failed read retains none.
pub const Lease = struct {
    bytes: []const u8,
    source: ?*Source,
    owned: ?[]u8 = null,
    reservation: admission.Token = .{},
    pub fn deinit(self: *Lease) void {
        if (self.source) |s| {
            if (self.owned) |owned| s.rangeAllocator().free(owned);
            s.releaseBytes(self.bytes.len);
        }
        self.reservation.deinit();
        self.* = .{ .bytes = &.{}, .source = null };
    }
};
/// File adapter borrows both the file handle and Io implementation.
pub const FileRange = struct {
    file: std.Io.File,
    io: std.Io,
    pub fn range(self: *FileRange, length: u64) Range {
        return .{ .concurrent = true, .context = self, .read_at = readAt, .length = length };
    }
    pub fn readAt(context: *anyopaque, offset: u64, bytes: []u8, control: Control) !usize {
        try control.check();
        const self: *FileRange = @ptrCast(@alignCast(context));
        // Source records the transfer before its post-read cancellation check.
        return self.file.readPositional(self.io, &.{bytes}, offset);
    }
};

test "source independent leases, short reads, budgets, cancellation and cleanup" {
    const Provider = struct {
        bytes: []const u8,
        fn readAt(ctx: *anyopaque, offset: u64, out: []u8, control: Control) !usize {
            try control.check();
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const n = @min(@min(out.len, 2), self.bytes.len - @as(usize, @intCast(offset)));
            @memcpy(out[0..n], self.bytes[@intCast(offset)..][0..n]);
            return n;
        }
    };
    var provider = Provider{ .bytes = "abcdefgh" };
    var src = Source{ .allocator = std.testing.allocator, .identity = "fixture-v1", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.readAt, .length = 8 } }, .limits = .{ .max_retained_bytes = 6 } };
    var first = try src.read(0, 4);
    defer first.deinit();
    var second = try src.read(4, 2);
    try std.testing.expectEqualStrings("abcd", first.bytes);
    try std.testing.expectEqualStrings("ef", second.bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(6, 1));
    second.deinit();
    try std.testing.expectEqual(@as(usize, 4), src.retained_bytes);
    try std.testing.expectError(error.UnexpectedEndOfSource, src.read(7, 2));
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    src.control = .{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, src.read(0, 1));
    first.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}

test "source provider failure and allocation failure release retention" {
    const Fail = struct {
        fn readAt(_: *anyopaque, _: u64, _: []u8, _: Control) !usize {
            return error.SourceFailure;
        }
    };
    var ctx: u8 = 0;
    var src = Source{ .allocator = std.testing.allocator, .identity = "v1", .storage = .{ .range = .{ .context = &ctx, .read_at = Fail.readAt, .length = 10 } } };
    try std.testing.expectError(error.SourceFailure, src.read(0, 3));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    src.allocator = failing.allocator();
    try std.testing.expectError(error.OutOfMemory, src.read(0, 3));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}

test "source partial EOF, read count and total budgets clean up without refunding IO" {
    const Provider = struct {
        calls: usize = 0,
        eof: bool = false,
        fn readAt(ctx: *anyopaque, _: u64, out: []u8, _: Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            if (self.eof and self.calls > 1) return 0;
            out[0] = 'x';
            return 1;
        }
    };
    var provider = Provider{ .eof = true };
    var src = Source{ .allocator = std.testing.allocator, .identity = "v1", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.readAt, .length = 10 } }, .limits = .{ .max_total_bytes = 3 } };
    try std.testing.expectError(error.UnexpectedEndOfSource, src.read(0, 3));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expectEqual(@as(u64, 1), src.total_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(0, 3));
    provider = .{};
    src.limits.max_reads = src.reads + 1;
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(0, 2));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expectEqual(@as(u64, 2), src.total_bytes);
}

test "file range adapter performs independent positional reads" {
    const io = std.testing.io;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const file = try dir.dir.createFile(io, "source", .{ .read = true });
    defer file.close(io);
    try file.writeStreamingAll(io, "abcdefgh");
    var provider = FileRange{ .file = file, .io = io };
    var src = Source{ .allocator = std.testing.allocator, .identity = "immutable-file-v1", .storage = .{ .range = provider.range(8) } };
    var tail = try src.read(4, 4);
    defer tail.deinit();
    var head = try src.read(0, 4);
    defer head.deinit();
    try std.testing.expectEqualStrings("efgh", tail.bytes);
    try std.testing.expectEqualStrings("abcd", head.bytes);
}

test "file transfer completed during cancellation consumes the shared byte allowance" {
    const io = std.testing.io;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const file = try dir.dir.createFile(io, "source", .{ .read = true });
    defer file.close(io);
    try file.writeStreamingAll(io, "abcd");
    const Provider = struct {
        file_range: FileRange,
        reading: bool = false,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: Control) !usize {
            const p: *@This() = @ptrCast(@alignCast(raw));
            return FileRange.readAt(&p.file_range, offset, out, .{ .context = p, .check_fn = begin });
        }
        fn begin(raw: ?*const anyopaque) !void {
            const p: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            try check(p);
            p.reading = true;
        }
        fn check(raw: ?*const anyopaque) !void {
            const p: *const @This() = @ptrCast(@alignCast(raw.?));
            if (p.reading) return error.Cancelled;
        }
    };
    var provider = Provider{ .file_range = .{ .file = file, .io = io } };
    var src = Source{ .allocator = std.testing.allocator, .identity = "file", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.read, .length = 4 } }, .limits = .{ .max_total_bytes = 4 } };
    var worker = src.view(std.testing.allocator, null, .{ .context = &provider, .check_fn = Provider.check });
    try std.testing.expectError(error.Cancelled, worker.read(0, 4));
    try std.testing.expectEqual(@as(u64, 4), worker.total_bytes);
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(0, 1));
}

test "source shared lease admission is retained and denied without provider reads" {
    var pool = admission.Pool{ .limits = .{ .host_bytes = 6 } };
    var src = Source{ .allocator = std.testing.allocator, .identity = "shared-v1", .storage = .{ .borrowed = "abcdefgh" }, .admission_pool = &pool };
    var first = try src.read(0, 4);
    defer first.deinit();
    const reads = src.reads;
    try std.testing.expectError(error.SharedAdmissionExceeded, src.read(4, 4));
    try std.testing.expectEqual(reads, src.reads);
    first.deinit();
    var retry = try src.read(4, 4);
    retry.deinit();
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
}

test "source immutable range borrows enforce limits and fall back across extents" {
    const Provider = struct {
        borrows: usize = 0,
        copies: usize = 0,
        fn borrow(raw: *anyopaque, offset: u64, size: usize, _: Control) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.borrows += 1;
            if (offset < 4 and size > 4 - offset) return null;
            return "abcdefgh"[@intCast(offset)..][0..size];
        }
        fn copy(raw: *anyopaque, offset: u64, target: []u8, _: Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.copies += 1;
            @memcpy(target, "abcdefgh"[@intCast(offset)..][0..target.len]);
            return target.len;
        }
    };
    var provider = Provider{};
    var src = Source{ .allocator = std.testing.failing_allocator, .identity = "immutable-extents", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.copy, .borrow_at = Provider.borrow, .length = 8 } }, .limits = .{ .max_read_bytes = 4, .max_retained_bytes = 7, .max_total_bytes = 7, .max_reads = 2 } };
    var first = try src.read(0, 3);
    defer first.deinit();
    try std.testing.expect(first.owned == null);
    try std.testing.expectEqualStrings("abc", first.bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(0, 5));
    try std.testing.expectEqual(@as(usize, 1), provider.borrows);
    // The crossing read needs an owned lease; use a reclaiming allocator.
    src.allocator = std.testing.allocator;
    var crossing = try src.read(2, 4);
    defer crossing.deinit();
    try std.testing.expect(crossing.owned != null);
    try std.testing.expectEqualStrings("cdef", crossing.bytes);
    try std.testing.expectEqual(@as(usize, 1), provider.copies);
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(7, 1));
    try std.testing.expectEqual(@as(usize, 2), provider.borrows);
    try std.testing.expectEqual(@as(u64, 7), src.total_bytes);
}

test "source views share short read charges and retained leases" {
    const Provider = struct {
        calls: usize = 0,
        fn read(raw: *anyopaque, _: u64, dest: []u8, _: Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            dest[0] = 0;
            return 1;
        }
    };
    var provider = Provider{};
    var source = Source{ .allocator = std.testing.allocator, .identity = "short", .storage = .{ .range = .{ .length = 8, .read_at = Provider.read, .context = &provider } }, .limits = .{ .max_reads = 4, .max_retained_bytes = 4 } };
    var decoder = source.view(source.allocator, null, .{});
    var sibling = decoder.view(source.allocator, null, .{});
    var held = try decoder.read(0, 2);
    defer held.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, sibling.read(2, 3));
    var header = try sibling.read(2, 1);
    header.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, decoder.read(3, 2));
    try std.testing.expectEqual(@as(usize, 4), provider.calls);
    try std.testing.expectEqual(@as(u64, 4), source.total_bytes);
    try std.testing.expectEqual(@as(u64, 3), decoder.total_bytes);
    try std.testing.expectEqual(@as(usize, 2), source.retained_bytes);
}

test "concurrent range callbacks share credits without holding the physical lock" {
    const Provider = struct {
        sibling: *Source,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (offset == 0) {
                try std.testing.expectEqual(@as(u64, 1), self.sibling.remaining().max_total_bytes);
                var nested = try self.sibling.read(1, 1);
                defer nested.deinit();
                out[0] = nested.bytes[0];
            } else out[0] = 42;
            return 1;
        }
    };
    var p: Provider = undefined;
    var owner = Source{ .allocator = std.testing.allocator, .identity = "range", .storage = .{ .range = .{ .context = &p, .length = 2, .read_at = Provider.read, .concurrent = true } }, .limits = .{ .max_reads = 2, .max_total_bytes = 2 } };
    var sibling = owner.view(owner.allocator, null, .{});
    p = .{ .sibling = &sibling };
    var result = try owner.read(0, 1);
    defer result.deinit();
    try std.testing.expectEqual(@as(u8, 42), result.bytes[0]);
    try std.testing.expectEqual(@as(u64, 2), owner.total_bytes);
    try std.testing.expectEqual(@as(u64, 1), sibling.total_bytes);
}

test "waiting for a serialized range honors cancellation without charging a call" {
    const Provider = struct {
        checks: usize = 0,
        fn check(raw: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.checks += 1;
            if (self.checks == 3) return error.Cancelled;
        }
        fn read(_: *anyopaque, _: u64, _: []u8, _: Control) !usize {
            return error.UnexpectedProviderCall;
        }
    };
    var provider = Provider{};
    var source = Source{ .allocator = std.testing.allocator, .identity = "busy", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.read, .length = 1 } } };
    // Model a sibling allocating a range buffer. Even a concurrent provider
    // must wait when allocating through an independently supplied worker arena.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    for ([_]bool{ false, true }) |concurrent| {
        provider.checks = 0;
        source.storage.range.concurrent = concurrent;
        source.allocator_gate.state.store(1, .release);
        var worker = source.view(arena.allocator(), null, .{ .context = &provider, .check_fn = Provider.check });
        try std.testing.expectError(error.Cancelled, worker.read(0, 1));
        try std.testing.expectEqual(@as(usize, 0), source.reads);
        try std.testing.expectEqual(@as(usize, 0), source.retained_bytes);
        source.allocator_gate.unlock();
    }
}

test "serial range callbacks can allocate and release scratch through the source" {
    const Provider = struct {
        source: *Source,
        fn borrow(raw: *anyopaque, _: u64, _: usize, _: Control) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const a = self.source.rangeAllocator();
            const scratch = try a.alloc(u8, 8);
            a.free(scratch);
            return null;
        }
        fn read(raw: *anyopaque, _: u64, out: []u8, control: Control) !usize {
            _ = try borrow(raw, 0, out.len, control);
            @memset(out, 42);
            return out.len;
        }
    };
    var source: Source = undefined;
    var provider = Provider{ .source = &source };
    source = .{ .allocator = std.testing.allocator, .identity = "serial-scratch", .storage = .{ .range = .{ .length = 1, .context = &provider, .borrow_at = Provider.borrow, .read_at = Provider.read } } };
    var lease = try source.read(0, 1);
    defer lease.deinit();
    try std.testing.expectEqual(@as(u8, 42), lease.bytes[0]);
}
