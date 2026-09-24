// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Optional cache-owned disk retention. No request allocator, cancellation
//! token, or executor outlives its request. One std.Io worker drains a bounded
//! queue; pressure drops retention, never authenticated query results.
const std = @import("std");
const cache_mod = @import("cache.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const max_jobs = 32;
pub const max_bytes = 16 * 1024 * 1024;

pub const Worker = struct {
    owner: *cache_mod.QueryCache,
    io_impl: std.Io.Threaded,
    group: std.Io.Group = .init,
    mu: std.Io.Mutex = .init,
    changed: std.Io.Condition = .init,
    stop: std.atomic.Value(bool) = .init(false),
    jobs: [max_jobs]Job = undefined,
    head: usize = 0,
    count: usize = 0,
    outstanding: usize = 0,
    bytes: usize = 0,
    failures: usize = 0,
    bypasses: usize = 0,

    const Job = struct {
        storage: []u8,
        artifact_id: []const u8,
        checksum: []const u8,
        byte_len: u64,
        blocks: [cache_mod.max_authenticated_publication_blocks]cache_mod.AuthenticatedBlockPublication,
        count: usize,
    };

    pub fn create(owner: *cache_mod.QueryCache) !*Worker {
        const self = try owner.alloc.create(Worker);
        errdefer owner.alloc.destroy(self);
        self.* = .{ .owner = owner, .io_impl = std.Io.Threaded.init(owner.alloc, .{ .async_limit = .nothing, .concurrent_limit = .limited(1) }) };
        errdefer self.io_impl.deinit();
        // Unlike async, concurrent may not run the infinite worker inline.
        try self.group.concurrent(self.io_impl.io(), run, .{self});
        return self;
    }

    pub fn deinit(self: *Worker) void {
        const io = self.io_impl.io();
        self.stop.store(true, .release);
        self.mu.lockUncancelable(io);
        self.changed.broadcast(io);
        self.mu.unlock(io);
        self.group.await(io) catch {};
        self.io_impl.deinit();
        self.owner.alloc.destroy(self);
    }

    /// Only already authenticated immutable bytes may enter this queue. The
    /// disk publisher independently checks digests before writing its records.
    pub fn enqueue(self: *Worker, artifact_id: []const u8, byte_len: u64, checksum: []const u8, blocks: []const cache_mod.AuthenticatedBlockPublication) !bool {
        if (blocks.len == 0) return true;
        if (blocks.len > cache_mod.max_authenticated_publication_blocks) return error.InvalidCacheBatch;
        var len = try std.math.add(usize, artifact_id.len, checksum.len);
        for (blocks) |block| {
            len = try std.math.add(usize, len, block.block_id.len);
            len = try std.math.add(usize, len, block.contents.len);
        }
        const io = self.io_impl.io();
        self.mu.lockUncancelable(io);
        defer self.mu.unlock(io);
        if (self.stop.load(.acquire) or self.outstanding == max_jobs or len > max_bytes - self.bytes) {
            self.bypasses += 1;
            return false;
        }
        const storage = try self.owner.alloc.alloc(u8, len);
        var remaining = storage;
        var job = Job{
            .storage = storage,
            .artifact_id = copy(&remaining, artifact_id),
            .checksum = copy(&remaining, checksum),
            .byte_len = byte_len,
            .blocks = undefined,
            .count = blocks.len,
        };
        for (blocks, job.blocks[0..blocks.len]) |block, *owned| {
            owned.* = block;
            owned.block_id = copy(&remaining, block.block_id);
            owned.contents = copy(&remaining, block.contents);
        }
        self.jobs[(self.head + self.count) % max_jobs] = job;
        self.count += 1;
        self.outstanding += 1;
        self.bytes += len;
        self.changed.signal(io);
        return true;
    }

    fn copy(remaining: *[]u8, bytes: []const u8) []const u8 {
        const result = remaining.*[0..bytes.len];
        @memcpy(result, bytes);
        remaining.* = remaining.*[bytes.len..];
        return result;
    }

    fn run(self: *Worker) void {
        const io = self.io_impl.io();
        while (true) {
            self.mu.lockUncancelable(io);
            while (self.count == 0 and !self.stop.load(.acquire)) self.changed.waitUncancelable(io, &self.mu);
            if (self.count == 0) {
                self.mu.unlock(io);
                return;
            }
            const job = self.jobs[self.head];
            self.head = (self.head + 1) % max_jobs;
            self.count -= 1;
            self.mu.unlock(io);
            var failed = false;
            if (!self.stop.load(.acquire)) self.owner.publishAuthenticatedBlocks(job.artifact_id, job.byte_len, job.checksum, job.blocks[0..job.count], CancellationToken.fromAtomic(&self.stop)) catch {
                failed = true;
            };
            self.owner.alloc.free(job.storage);
            self.mu.lockUncancelable(io);
            self.bytes -= job.storage.len;
            self.outstanding -= 1;
            self.failures += @intFromBool(failed);
            self.changed.broadcast(io);
            self.mu.unlock(io);
        }
    }

    /// Maintenance/test barrier, never part of a query read.
    pub fn drain(self: *Worker) void {
        const io = self.io_impl.io();
        self.mu.lockUncancelable(io);
        defer self.mu.unlock(io);
        while (self.outstanding != 0) self.changed.waitUncancelable(io, &self.mu);
    }
};
