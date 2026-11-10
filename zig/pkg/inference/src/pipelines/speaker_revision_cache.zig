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

//! Verified managed artifacts reuse digests until their deployment/file stamp
//! changes. Unmanaged or unpinned artifacts retain full content verification.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const receipts = @import("../registry/managed_receipt.zig");
const Stamp = struct {
    inode: u64,
    size: u64,
    mtime: i128,
    ctime: i128,
    fn of(stat: std.Io.File.Stat) Stamp {
        return .{ .inode = stat.inode, .size = stat.size, .mtime = stat.mtime.toNanoseconds(), .ctime = stat.ctime.toNanoseconds() };
    }
};
pub const Cache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    flights: @import("media_speaker_flights.zig").Registry = .{},
    entries: std.StringHashMapUnmanaged(Entry) = .empty,
    full_hashes: std.atomic.Value(u64) = .init(0),
    const Entry = struct { file: Stamp, receipt: Stamp, revision: [64]u8 };
    pub fn get(self: *Cache, a: std.mem.Allocator, path: []const u8, io: std.Io, control: Control) ![64]u8 {
        var identity: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(path, &identity, .{});
        const flight = try self.flights.acquire(a, identity, control);
        defer flight.deinit();
        // The map lock never covers filesystem verification or full hashing.
        try control.lockWithPoll(&self.mutex, 100 * std.time.ns_per_us);
        const cached = self.entries.get(path);
        self.mutex.unlock();
        const file = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        const initial = Stamp.of(try file.stat(io));
        const parent = std.fs.path.dirname(path) orelse ".";
        const receipt_path = try std.fs.path.join(a, &.{ parent, receipts.complete_filename });
        defer a.free(receipt_path);
        const receipt_stat = std.Io.Dir.cwd().statFile(io, receipt_path, .{}) catch null;
        if (receipt_stat != null and receipts.publicationBlocked(a, io, parent)) return error.ModelConfigurationChanged;
        if (receipt_stat) |stat| if (cached) |entry| {
            if (std.meta.eql(initial, entry.file) and std.meta.eql(Stamp.of(stat), entry.receipt)) {
                if (!std.meta.eql(initial, Stamp.of(try std.Io.Dir.cwd().statFile(io, path, .{})))) return error.ModelConfigurationChanged;
                if (!std.meta.eql(entry.receipt, Stamp.of(try std.Io.Dir.cwd().statFile(io, receipt_path, .{})))) return error.ModelConfigurationChanged;
                return entry.revision;
            }
        };
        var managed = try receipts.loadValidated(a, io, parent);
        defer if (managed) |*receipt| receipt.deinit();
        var expected: ?[]const u8 = null;
        if (managed) |*receipt| if (receipt.parsed.value.version == 2) if (receipt.parsed.value.source) |source| if (source.resolved_revision) |revision| {
            var pinned = revision.len == 40 or revision.len == 64;
            for (revision) |c| if (!std.ascii.isHex(c)) {
                pinned = false;
                break;
            };
            if (pinned) {
                if (receipt.find(std.fs.path.basename(path))) |artifact| expected = artifact.sha256;
            }
        };
        _ = self.full_hashes.fetchAdd(1, .monotonic);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        var buffer: [64 * 1024]u8 = undefined;
        while (true) {
            try control.check();
            const n = file.readStreaming(io, &.{&buffer}) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            if (n == 0) break;
            hash.update(buffer[0..n]);
        }
        const revision = std.fmt.bytesToHex(hash.finalResult(), .lower);
        if (!std.meta.eql(initial, Stamp.of(try file.stat(io))) or !std.meta.eql(initial, Stamp.of(try std.Io.Dir.cwd().statFile(io, path, .{})))) return error.ModelConfigurationChanged;
        if (expected) |digest| {
            if (!std.mem.eql(u8, &revision, digest)) return error.ChecksumMismatch;
            if (receipt_stat) |stat| {
                const stamp = Stamp.of(stat);
                if (!std.meta.eql(stamp, Stamp.of(try std.Io.Dir.cwd().statFile(io, receipt_path, .{}))) or receipts.publicationBlocked(a, io, parent)) return error.ModelConfigurationChanged;
                try control.lockWithPoll(&self.mutex, 100 * std.time.ns_per_us);
                defer self.mutex.unlock();
                if (self.entries.getPtr(path)) |entry| entry.* = .{ .file = initial, .receipt = stamp, .revision = revision } else {
                    if (self.entries.count() >= 128) {
                        var it = self.entries.iterator();
                        const oldest = it.next().?.key_ptr.*;
                        const removed = self.entries.fetchRemove(oldest).?;
                        a.free(removed.key);
                    }
                    const name = try a.dupe(u8, path);
                    errdefer a.free(name);
                    try self.entries.put(a, name, .{ .file = initial, .receipt = stamp, .revision = revision });
                }
            }
        }
        return revision;
    }
    pub fn deinit(self: *Cache, a: std.mem.Allocator) void {
        self.flights.deinit(a);
        var it = self.entries.iterator();
        while (it.next()) |entry| a.free(entry.key_ptr.*);
        self.entries.deinit(a);
    }
};

test "model aware media managed speaker digests reuse verified receipts and invalidate changed artifacts" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const parent = try temp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(parent);
    const path = try std.fs.path.join(a, &.{ parent, "speaker.onnx" });
    defer a.free(path);
    var cache: Cache = .{};
    defer cache.deinit(a);
    try temp.dir.writeFile(io, .{ .sub_path = "speaker.onnx", .data = "model one" });
    const original = try cache.get(a, path, io, .{});
    _ = try cache.get(a, path, io, .{});
    try std.testing.expectEqual(@as(u64, 2), cache.full_hashes.load(.monotonic));
    const receipt = try std.json.Stringify.valueAlloc(a, receipts.DownloadReceipt{
        .version = 2,
        .source = .{ .owner = "test", .name = "speaker", .variant = "speaker.onnx", .resolved_revision = "0123456789012345678901234567890123456789" },
        .artifacts = &.{.{ .path = "speaker.onnx", .size = 9, .sha256 = &original }},
    }, .{});
    defer a.free(receipt);
    try temp.dir.writeFile(io, .{ .sub_path = receipts.complete_filename, .data = receipt });
    _ = try cache.get(a, path, io, .{});
    _ = try cache.get(a, path, io, .{});
    try std.testing.expectEqual(@as(u64, 3), cache.full_hashes.load(.monotonic));
    try temp.dir.writeFile(io, .{ .sub_path = receipts.in_progress_filename, .data = "" });
    try std.testing.expectError(error.ModelConfigurationChanged, cache.get(a, path, io, .{}));
    try temp.dir.deleteFile(io, receipts.in_progress_filename);
    // Atomic same-size replacement must invalidate the previously verified inode.
    try temp.dir.writeFile(io, .{ .sub_path = "replacement", .data = "model two" });
    try temp.dir.rename("replacement", temp.dir, "speaker.onnx", io);
    try std.testing.expectError(error.ChecksumMismatch, cache.get(a, path, io, .{}));
    try std.testing.expectEqual(@as(u64, 4), cache.full_hashes.load(.monotonic));
    try temp.dir.deleteFile(io, receipts.complete_filename);
    const changed = try cache.get(a, path, io, .{});
    try std.testing.expect(!std.mem.eql(u8, &original, &changed));
    // A replacement receipt is separately stamped and must be revalidated.
    try temp.dir.writeFile(io, .{ .sub_path = "speaker.onnx", .data = "model one" });
    try temp.dir.writeFile(io, .{ .sub_path = receipts.complete_filename, .data = receipt });
    _ = try cache.get(a, path, io, .{});
    const before = cache.full_hashes.load(.monotonic);
    _ = try cache.get(a, path, io, .{});
    try std.testing.expectEqual(before, cache.full_hashes.load(.monotonic));
    // In-place same-size edits invalidate ctime even when mtime is restored.
    const file = try temp.dir.openFile(io, "speaker.onnx", .{ .mode = .read_write });
    defer file.close(io);
    const stat = try file.stat(io);
    try file.writePositionalAll(io, "model two", 0);
    try file.setTimestamps(io, .{ .modify_timestamp = .{ .new = stat.mtime } });
    try std.testing.expectError(error.ChecksumMismatch, cache.get(a, path, io, .{}));
}

test "model aware media a paused speaker hash does not block a different model digest" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    try temp.dir.writeFile(io, .{ .sub_path = "a.onnx", .data = "first model" });
    try temp.dir.writeFile(io, .{ .sub_path = "b.onnx", .data = "second model" });
    const first_path = try temp.dir.realPathFileAlloc(io, "a.onnx", a);
    defer a.free(first_path);
    const second_path = try temp.dir.realPathFileAlloc(io, "b.onnx", a);
    defer a.free(second_path);
    var cache: Cache = .{};
    defer cache.deinit(a);
    const Worker = struct {
        cache: *Cache,
        path: []const u8,
        paused: std.atomic.Value(bool) = .init(false),
        proceed: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cache.full_hashes.load(.monotonic) == 0) return;
            self.paused.store(true, .release);
            while (!self.proceed.load(.acquire)) std.atomic.spinLoopHint();
        }
        fn run(self: *@This()) void {
            _ = self.cache.get(std.testing.allocator, self.path, std.testing.io, .{ .ptr = self, .check_fn = check }) catch |err| {
                self.failure = err;
                self.paused.store(true, .release);
                return;
            };
        }
    };
    var worker = Worker{ .cache = &cache, .path = first_path };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    defer {
        worker.proceed.store(true, .release);
        thread.join();
    }
    while (!worker.paused.load(.acquire)) std.atomic.spinLoopHint();
    if (worker.failure) |err| return err;
    const now = @import("antfly_platform").time.monotonicNs();
    _ = try cache.get(a, second_path, io, .{ .deadline_ns = now + std.time.ns_per_s });
    try std.testing.expectEqual(@as(u64, 2), cache.full_hashes.load(.monotonic));
}
