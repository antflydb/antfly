// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Disposable, bounded retained-frame assembly beneath the existing restore
//! source generation. Its receipt is not target progress: only the shared
//! restore page's replicated CAS can acknowledge transformed effects.
const std = @import("std");
const contract = @import("db/relational_rewrite_contract.zig");
const staging = @import("db/restore_staging_contract.zig");
const native = @import("db/native_backup.zig");
const fs = @import("../common/fs_paths.zig");
const Digest = [32]u8;
const verified = @import("verified_retained_frame.zig");
const resources = @import("resource_manager.zig");

/// One immutable frame per target owner, shared across page RPCs. All users
/// hold mutex through transformation/proposal; pressure reclamation never
/// evicts a borrowed frame. The durable spool/progress remain recovery truth.
pub const Cache = struct {
    mutex: std.Io.Mutex = .init,
    io: ?std.Io = null,
    manager: ?*resources.ResourceManager = null,
    reclaimer: u64 = 0,
    entry: ?Entry = null,
    frame_loads: u64 = 0,
    payload_bytes_read: u64 = 0,
    cache_hits: u64 = 0,

    const Entry = struct {
        alloc: std.mem.Allocator,
        scope: Digest,
        sequence: u64,
        bytes: []u8,
        frame: verified.Frame,
        reservation: ?resources.Reservation,
    };

    pub fn clear(self: *Cache) void {
        if (self.entry) |*entry| {
            entry.frame.deinit();
            entry.alloc.free(entry.bytes);
            if (entry.reservation) |*reservation| reservation.release();
            self.entry = null;
        }
    }

    pub fn deinit(self: *Cache, io: std.Io) void {
        // Unregister without our mutex: an in-flight callback may be trying
        // to acquire it. Owner teardown has already drained foreground users.
        if (self.manager) |manager| manager.unregisterReclaimer(self.reclaimer);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.clear();
        self.reclaimer = 0;
        self.manager = null;
        self.io = null;
    }

    fn reclaim(ptr: *anyopaque, _: u64) u64 {
        const self: *Cache = @ptrCast(@alignCast(ptr));
        if (!self.mutex.tryLock()) return 0;
        defer self.mutex.unlock(self.io.?);
        const size = if (self.entry) |entry| if (entry.reservation) |reservation| reservation.bytes else 0 else 0;
        self.clear();
        return size;
    }

    fn bind(self: *Cache, io: std.Io, manager: ?*resources.ResourceManager) !void {
        if (self.io != null and self.manager != manager) return error.InvalidRestoreSourceCheckpoint;
        if (self.reclaimer != 0) {
            if (self.manager != manager) return error.InvalidRestoreSourceCheckpoint;
            return;
        }
        self.io = io;
        self.manager = manager;
        if (manager) |value| self.reclaimer = try value.registerReclaimer(.relational_preparation_working_set, self, reclaim);
    }
};
const Receipt = struct {
    scope: Digest,
    sequence: u64,
    digest: Digest,
    total: u32,
    next: u32,

    fn encode(self: Receipt) [112]u8 {
        var bytes: [112]u8 = undefined;
        @memcpy(bytes[0..32], &self.scope);
        std.mem.writeInt(u64, bytes[32..40], self.sequence, .little);
        @memcpy(bytes[40..72], &self.digest);
        std.mem.writeInt(u32, bytes[72..76], self.total, .little);
        std.mem.writeInt(u32, bytes[76..80], self.next, .little);
        std.crypto.hash.Blake3.hash(bytes[0..80], bytes[80..112], .{});
        return bytes;
    }
    fn decode(bytes: []const u8) !Receipt {
        if (bytes.len != 112) return error.InvalidRestoreSourceCheckpoint;
        var digest: Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..80], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[80..112])) return error.InvalidRestoreSourceCheckpoint;
        const value: Receipt = .{ .scope = bytes[0..32].*, .sequence = std.mem.readInt(u64, bytes[32..40], .little), .digest = bytes[40..72].*, .total = std.mem.readInt(u32, bytes[72..76], .little), .next = std.mem.readInt(u32, bytes[76..80], .little) };
        if (value.sequence == 0 or value.total == 0 or value.total > 16 * 1024 * 1024 or value.next > value.total) return error.InvalidRestoreSourceCheckpoint;
        return value;
    }
};

pub const Result = struct {
    next: u32,
    /// Borrowed until cache mutex release. Never a publication receipt.
    frame: ?*const verified.Frame = null,
};

/// Return a resumable donor offset without reading frame payload. A complete
/// frame returns its final byte so a bounded duplicate chunk can resume the
/// remaining transformed effects after restart, without retransferring it.
pub fn resumeOffset(alloc: std.mem.Allocator, io: std.Io, root: []const u8, scope: staging.Scope, after: u64) !u32 {
    const path = try std.fmt.allocPrint(alloc, "{s}/rewrite-frame.receipt", .{root});
    defer alloc.free(path);
    const raw = native.readFileAlloc(alloc, io, path, 113) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer alloc.free(raw);
    const receipt = try Receipt.decode(raw);
    if (!std.mem.eql(u8, &receipt.scope, &scope.digest())) return error.RestoreStagingScopeChanged;
    if (receipt.sequence <= after) return 0;
    if (receipt.sequence != try std.math.add(u64, after, 1)) return error.RestoreStagingProgressChanged;
    return if (receipt.next == receipt.total) receipt.next - 1 else receipt.next;
}

/// Caller holds the existing source-generation transition lock, shared with
/// terminal cleanup, then cache.mutex through transformation and proposal.
/// One acknowledged chunk performs at most 64 KiB payload IO; completing a
/// frame verifies at most the fixed 16 MiB retained-frame ceiling.
pub fn receive(alloc: std.mem.Allocator, io: std.Io, root: []const u8, scope: staging.Scope, after: u64, chunk: contract.TailChunk, cache: *Cache, cache_alloc: std.mem.Allocator, manager: ?*resources.ResourceManager) !Result {
    try chunk.validate();
    const binding = scope.rewrite orelse return error.InvalidRestoreStagingCommand;
    if (!std.mem.eql(u8, &chunk.pin, &binding.retained_pin) or chunk.sequence != try std.math.add(u64, after, 1)) return error.RestoreStagingProgressChanged;
    try cache.bind(io, manager);
    if (cache.entry) |*entry| {
        if (entry.sequence == chunk.sequence and std.mem.eql(u8, &entry.scope, &scope.digest())) {
            if (entry.bytes.len != chunk.total or !std.mem.eql(u8, &entry.frame.reader.frame_digest, &chunk.frame_digest)) return error.RestoreStagingScopeChanged;
            if (!std.mem.eql(u8, entry.bytes[chunk.offset..][0..chunk.data.len], chunk.data)) return error.InvalidRestoreSourceCheckpoint;
            cache.cache_hits +|= 1;
            return .{ .next = chunk.total, .frame = &entry.frame };
        }
        cache.clear();
    }
    try fs.createDirPathPortable(io, root);
    const state_path = try std.fmt.allocPrint(alloc, "{s}/rewrite-frame.receipt", .{root});
    defer alloc.free(state_path);
    const path = try std.fmt.allocPrint(alloc, "{s}/rewrite-frame.ref3", .{root});
    defer alloc.free(path);
    const raw = native.readFileAlloc(alloc, io, state_path, 113) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (raw) |bytes| alloc.free(bytes);
    var receipt: Receipt = .{ .scope = scope.digest(), .sequence = chunk.sequence, .digest = chunk.frame_digest, .total = chunk.total, .next = 0 };
    if (raw) |bytes| {
        const previous = try Receipt.decode(bytes);
        if (!std.mem.eql(u8, &previous.scope, &receipt.scope) or previous.sequence > chunk.sequence) return error.RestoreStagingScopeChanged;
        if (previous.sequence == chunk.sequence) {
            if (previous.total != chunk.total or !std.mem.eql(u8, &previous.digest, &chunk.frame_digest)) return error.RestoreStagingScopeChanged;
            receipt = previous;
        }
    }
    const file = try fs.createFilePortable(io, path, .{ .read = true, .truncate = false });
    defer file.close(io);
    if (receipt.next != 0 and (try file.stat(io)).size < receipt.next) return error.InvalidRestoreSourceCheckpoint;
    if (chunk.offset > receipt.next) return .{ .next = receipt.next };
    if (chunk.offset < receipt.next) {
        if (chunk.data.len > receipt.next - chunk.offset) return error.RestoreStagingProgressChanged;
        const existing = try alloc.alloc(u8, chunk.data.len);
        defer alloc.free(existing);
        if (try file.readPositionalAll(io, existing, chunk.offset) != existing.len or !std.mem.eql(u8, existing, chunk.data)) return error.InvalidRestoreSourceCheckpoint;
    } else {
        // Ignore an unacknowledged torn suffix from a previous process.
        try file.setLength(io, receipt.next);
        try file.writePositionalAll(io, chunk.data, chunk.offset);
        try file.sync(io);
        receipt.next += @intCast(chunk.data.len);
        const pending = try std.fmt.allocPrint(alloc, "{s}.pending", .{state_path});
        defer alloc.free(pending);
        _ = try native.writeFileDurable(io, pending, &receipt.encode());
        try std.Io.Dir.rename(.cwd(), pending, .cwd(), state_path, io);
        try fs.syncDirPortable(io, root);
    }
    if (receipt.next != receipt.total) return .{ .next = receipt.next };
    // Charge both immutable bytes and the maximum boundary table before any
    // retained allocation. Idle frames from other owners can be reclaimed.
    var reservation: ?resources.Reservation = if (manager) |value| try value.reserve(.relational_preparation_working_set, @as(u64, receipt.total) + (@as(u64, @import("retained_effects.zig").max_keys) + 1) * @sizeOf(u32)) else null;
    errdefer if (reservation) |*value| value.release();
    const frame = try cache_alloc.alloc(u8, receipt.total);
    errdefer cache_alloc.free(frame);
    if ((try file.stat(io)).size != receipt.total or try file.readPositionalAll(io, frame, 0) != frame.len) return error.InvalidRestoreSourceCheckpoint;
    var indexed = try verified.Frame.init(cache_alloc, frame, receipt.sequence);
    errdefer indexed.deinit();
    if (!std.mem.eql(u8, &indexed.reader.frame_digest, &receipt.digest)) return error.RetainedEffectsCorrupt;
    cache.entry = .{ .alloc = cache_alloc, .scope = scope.digest(), .sequence = receipt.sequence, .bytes = frame, .frame = indexed, .reservation = reservation };
    cache.frame_loads +|= 1;
    cache.payload_bytes_read +|= frame.len;
    return .{ .next = receipt.next, .frame = &cache.entry.?.frame };
}

test "relational index system rewrite tail verifies once resumes exact boundaries and reclaims under pressure" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(root);
    const scope: staging.Scope = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .source_artifact_digest = @splat(3),
        .source_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
        .target_namespace = .{ .table_id = 3, .shard_id = 4, .range_id = 4 },
        .target_schema_digest = @splat(4),
        .rewrite = .{ .program_digest = @splat(5), .retained_pin = @splat(6), .snapshot_certificate = @splat(7), .retained_epoch = 1, .retained_start = 0, .source_applied_index = 1 },
    };
    try scope.validate();
    var bytes = std.ArrayList(u8).empty;
    defer bytes.deinit(alloc);
    const count = 4096;
    var header: [16]u8 = undefined;
    @memcpy(header[0..4], "REF3");
    std.mem.writeInt(u64, header[4..12], 1, .little);
    std.mem.writeInt(u32, header[12..16], count, .little);
    try bytes.appendSlice(alloc, &header);
    const payload = [_]u8{'x'} ** 2048;
    for (0..count) |i| {
        var name: [32]u8 = undefined;
        const key = try @import("internal_keys.zig").documentKeyAlloc(alloc, try std.fmt.bufPrint(&name, "row:{d:0>8}", .{i}));
        defer alloc.free(key);
        std.mem.writeInt(u32, header[0..4], @intCast(key.len), .little);
        std.mem.writeInt(u32, header[4..8], payload.len, .little);
        std.mem.writeInt(u64, header[8..16], 42, .little);
        try bytes.appendSlice(alloc, &header);
        try bytes.appendSlice(alloc, key);
        try bytes.appendSlice(alloc, &payload);
    }
    var checksum: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes.items, &checksum, .{});
    try bytes.appendSlice(alloc, &checksum);
    var digest: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes.items, &digest, .{});
    {
        var checked = try verified.Frame.init(alloc, bytes.items, 1);
        defer checked.deinit();
        try std.testing.expectEqual(count, checked.reader.remaining);
        try std.testing.expectEqualSlices(u8, &digest, &checked.reader.frame_digest);
    }
    var resource_options: resources.Options = .{
        .identity_allocator = alloc,
        .memory_budget = .{ .hard_limit_bytes = 20 * 1024 * 1024 },
    };
    resource_options.budgets[@intFromEnum(resources.Slice.relational_preparation_working_set)] = .{ .hard_limit_bytes = 16 * 1024 * 1024 };
    var manager = resources.ResourceManager.init(resource_options);
    defer manager.deinit(alloc);
    var cache: Cache = .{};
    defer cache.deinit(io);
    var chunk: contract.TailChunk = .{ .pin = scope.rewrite.?.retained_pin, .sequence = 1, .frame_digest = digest, .total = @intCast(bytes.items.len), .offset = 0, .data = "" };
    while (chunk.offset < chunk.total) {
        chunk.data = bytes.items[chunk.offset..@min(bytes.items.len, chunk.offset + 64 * 1024)];
        try cache.mutex.lock(io);
        const result = receive(alloc, io, root, scope, 0, chunk, &cache, alloc, &manager) catch |err| {
            cache.mutex.unlock(io);
            return err;
        };
        cache.mutex.unlock(io);
        chunk.offset = result.next;
    }
    chunk.offset = chunk.total - 1;
    chunk.data = bytes.items[chunk.offset..];
    var offset: u32 = 0;
    var remaining: u32 = 0;
    var pages: usize = 0;
    const started = std.Io.Clock.awake.now(io);
    while (pages < count / 128) : (pages += 1) {
        try cache.mutex.lock(io);
        defer cache.mutex.unlock(io);
        // Hot page retries need neither allocations nor disk payload reads.
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
        const result = try receive(failing.allocator(), io, root, scope, 0, chunk, &cache, failing.allocator(), &manager);
        var reader = try result.frame.?.readerAt(offset, remaining);
        const retry = try result.frame.?.readerAt(offset, remaining);
        try std.testing.expectEqual(reader.pos, retry.pos);
        for (0..128) |_| _ = (try reader.next()) orelse return error.TestUnexpectedResult;
        offset = @intCast(reader.pos);
        remaining = reader.remaining;
        try std.testing.expectError(error.InvalidRestoreStagingRecord, result.frame.?.readerAt(offset + 1, remaining));
    }
    const elapsed = started.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds();
    try std.testing.expectEqual(@as(u32, 0), remaining);
    try std.testing.expectEqual(@as(u64, 1), cache.frame_loads);
    try std.testing.expectEqual(bytes.items.len, cache.payload_bytes_read);
    try std.testing.expectEqual(pages, cache.cache_hits);
    std.debug.print("\nrewrite tail: {} rows / {} pages; {} payload bytes read (one verification), versus {} bytes for per-page reload; warm page traversal {}us\n", .{ count, pages, cache.payload_bytes_read, bytes.items.len * pages, @divTrunc(elapsed, 1000) });
    // A leased frame cannot be reclaimed; idle bytes are independently
    // disposable and the next page reconstructs exactly one verified cache.
    try cache.mutex.lock(io);
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaim(&cache, 1));
    cache.mutex.unlock(io);
    var competing = try manager.reserve(.relational_preparation_working_set, bytes.items.len + 262148);
    competing.release();
    try std.testing.expect(cache.entry == null);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try cache.mutex.lock(io);
    {
        defer cache.mutex.unlock(io);
        for (0..2) |failure| {
            var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = failure });
            try std.testing.expectError(error.OutOfMemory, receive(alloc, io, root, scope, 0, chunk, &cache, failing.allocator(), &manager));
            try std.testing.expect(cache.entry == null);
            try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
        }
        const restored = try receive(alloc, io, root, scope, 0, chunk, &cache, alloc, &manager);
        try std.testing.expectEqual(@as(u64, 2), cache.frame_loads);
        _ = try restored.frame.?.readerAt(offset, remaining);
        var bad = chunk;
        bad.frame_digest[0] ^= 1;
        try std.testing.expectError(error.RestoreStagingScopeChanged, receive(alloc, io, root, scope, 0, bad, &cache, alloc, &manager));
    }
    // Pressure from another slice must reclaim this cache too, not only
    // contention among relational owners sharing the local slice ceiling.
    var foreground = try manager.reserve(.dense_apply_working_set, 14 * 1024 * 1024);
    foreground.release();
    try std.testing.expect(cache.entry == null);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    // Restart/eviction may not trust a persisted completion receipt without
    // reauthenticating the spool. Disk corruption cannot enter the cache.
    const path = try std.fmt.allocPrint(alloc, "{s}/rewrite-frame.ref3", .{root});
    defer alloc.free(path);
    const file = try fs.createFilePortable(io, path, .{ .read = true, .truncate = false });
    defer file.close(io);
    try file.writePositionalAll(io, "X", 0);
    try cache.mutex.lock(io);
    defer cache.mutex.unlock(io);
    try std.testing.expectError(error.RetainedEffectsCorrupt, receive(alloc, io, root, scope, 0, chunk, &cache, alloc, &manager));
    try std.testing.expect(cache.entry == null);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}
