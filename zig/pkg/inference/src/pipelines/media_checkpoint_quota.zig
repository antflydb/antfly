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

//! Cross-process quota for optional checkpoint files. The small ledger is
//! reserved before writes. Dirty/torn ledgers and new processes reconcile
//! against disk, so crashes cannot create permanently unaccounted files.
const std = @import("std");
pub const Policy = struct {
    max_bytes: u64 = 256 * 1024 * 1024,
    max_files: u64 = 4096,
    max_age_seconds: u64 = 7 * 24 * 60 * 60,
    sweep_seconds: u64 = 60,
};
const Stats = struct { bytes: u64 = 0, files: u64 = 0, swept: i64 = 0, dirty: bool = false };
const Row = struct { name: [68]u8, bytes: u64, modified: i64 };
const batch = 256;
const Scan = struct { stats: Stats = .{}, rows: [batch]Row = undefined, count: usize = 0 };
pub const Manager = struct {
    policy: Policy = .{},
    initialized: std.atomic.Value(bool) = .init(false),
    scans: std.atomic.Value(u64) = .init(0),
    pub fn open(self: *Manager, io: std.Io, root: []const u8, identity: [32]u8) !Gate {
        var path: [std.fs.max_path_bytes]u8 = undefined;
        const directory = try std.fmt.bufPrint(&path, "{s}/.antfly-speaker-plans", .{root});
        try std.Io.Dir.cwd().createDirPath(io, directory);
        const dir = try std.Io.Dir.cwd().openDir(io, directory, .{ .iterate = true });
        errdefer dir.close(io);
        const lock = try dir.createFile(io, ".quota.lock", .{ .truncate = false, .lock = .exclusive, .lock_nonblocking = true });
        errdefer lock.close(io);
        const counter = try dir.createFile(io, ".quota.bin", .{ .truncate = false, .read = true });
        errdefer counter.close(io);
        var gate = Gate{ .manager = self, .io = io, .dir = dir, .lock = lock, .counter = counter, .protect = undefined };
        _ = try std.fmt.bufPrint(&gate.protect, "{s}.bin", .{std.fmt.bytesToHex(identity, .lower)});
        const loaded = gate.readLedger() catch null;
        const now = Gate.now(io);
        if (loaded) |value| gate.stats = value;
        if (!self.initialized.load(.acquire) or loaded == null or gate.stats.dirty or @as(u64, @intCast(@max(0, now -| gate.stats.swept))) >= self.policy.sweep_seconds or gate.stats.swept > now) {
            const scanned = try gate.scan(null);
            gate.stats = scanned.stats;
            try gate.writeLedger();
        }
        try gate.reclaim(0, 0, null);
        self.initialized.store(true, .release);
        return gate;
    }
};
pub const Gate = struct {
    manager: *Manager,
    io: std.Io,
    dir: std.Io.Dir,
    lock: std.Io.File,
    counter: std.Io.File,
    stats: Stats = .{},
    protect: [68]u8,
    reserved_bytes: u64 = 0,
    reserved_files: u64 = 0,
    fn now(io: std.Io) i64 {
        return @intCast(@divFloor(std.Io.Clock.real.now(io).toNanoseconds(), std.time.ns_per_s));
    }
    pub fn deinit(self: *Gate) void {
        self.counter.close(self.io);
        self.lock.close(self.io);
        self.dir.close(self.io);
    }
    fn readLedger(self: *Gate) !?Stats {
        if ((try self.counter.stat(self.io)).size != 72) return null;
        var bytes: [72]u8 = undefined;
        if (try self.counter.readPositionalAll(self.io, &bytes, 0) != bytes.len or !std.mem.eql(u8, bytes[0..8], "AFSQ0001") or bytes[32] > 1) return null;
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[0..40], &hash, .{});
        if (!std.mem.eql(u8, &hash, bytes[40..])) return null;
        return .{ .bytes = std.mem.readInt(u64, bytes[8..16], .little), .files = std.mem.readInt(u64, bytes[16..24], .little), .swept = std.mem.readInt(i64, bytes[24..32], .little), .dirty = bytes[32] != 0 };
    }
    fn writeLedger(self: *Gate) !void {
        var bytes: [72]u8 = @splat(0);
        @memcpy(bytes[0..8], "AFSQ0001");
        std.mem.writeInt(u64, bytes[8..16], self.stats.bytes, .little);
        std.mem.writeInt(u64, bytes[16..24], self.stats.files, .little);
        std.mem.writeInt(i64, bytes[24..32], self.stats.swept, .little);
        bytes[32] = @intFromBool(self.stats.dirty);
        std.crypto.hash.sha2.Sha256.hash(bytes[0..40], bytes[40..72], .{});
        try self.counter.writePositionalAll(self.io, &bytes, 0);
        try self.counter.setLength(self.io, bytes.len);
        // Process crashes preserve kernel-visible writes; torn writes fail the
        // checksum. After a machine restart every Manager scans on first use.
    }
    fn owned(name: []const u8, suffix: []const u8) bool {
        if (name.len != 68 or !std.mem.endsWith(u8, name, suffix)) return false;
        for (name[0..64]) |char| if (!std.ascii.isHex(char)) return false;
        return true;
    }
    fn candidate(scan_: *Scan, row: Row) void {
        var index: usize = undefined;
        if (scan_.count < batch) {
            index = scan_.count;
            scan_.rows[index] = row;
            scan_.count += 1;
        } else {
            if (row.modified >= scan_.rows[0].modified) return;
            scan_.rows[0] = row;
            var at: usize = 0;
            while (at * 2 + 1 < scan_.count) {
                var child = at * 2 + 1;
                if (child + 1 < scan_.count and scan_.rows[child + 1].modified > scan_.rows[child].modified) child += 1;
                if (scan_.rows[at].modified >= scan_.rows[child].modified) break;
                std.mem.swap(Row, &scan_.rows[at], &scan_.rows[child]);
                at = child;
            }
            return;
        }
        while (index > 0) {
            const parent = (index - 1) / 2;
            if (scan_.rows[parent].modified >= scan_.rows[index].modified) break;
            std.mem.swap(Row, &scan_.rows[parent], &scan_.rows[index]);
            index = parent;
        }
    }
    fn scan(self: *Gate, protect: ?[]const u8) !Scan {
        _ = self.manager.scans.fetchAdd(1, .monotonic);
        var result = Scan{};
        result.stats.swept = now(self.io);
        var iter = self.dir.iterate();
        while (try iter.next(self.io)) |entry| {
            if (entry.kind != .file and entry.kind != .unknown) continue;
            if (owned(entry.name, ".tmp")) {
                self.dir.deleteFile(self.io, entry.name) catch {};
                continue;
            }
            if (!owned(entry.name, ".bin")) continue;
            const stat = self.dir.statFile(self.io, entry.name, .{ .follow_symlinks = false }) catch continue;
            if (stat.kind != .file) continue;
            const modified: i64 = @intCast(@divFloor(stat.mtime.toNanoseconds(), std.time.ns_per_s));
            const protected = if (protect) |name| std.mem.eql(u8, name, entry.name) else false;
            if (!protected and self.manager.policy.max_age_seconds > 0 and @as(u64, @intCast(@max(0, result.stats.swept -| modified))) > self.manager.policy.max_age_seconds) {
                if (self.dir.deleteFile(self.io, entry.name)) |_| continue else |_| {}
            }
            result.stats.bytes = try std.math.add(u64, result.stats.bytes, stat.size);
            result.stats.files += 1;
            if (!protected) {
                var row = Row{ .name = undefined, .bytes = stat.size, .modified = modified };
                @memcpy(&row.name, entry.name);
                candidate(&result, row);
            }
        }
        return result;
    }
    fn fits(self: *Gate, bytes: u64, files: u64) bool {
        return bytes <= self.manager.policy.max_bytes -| self.stats.bytes and files <= self.manager.policy.max_files -| self.stats.files and self.stats.bytes <= self.manager.policy.max_bytes and self.stats.files <= self.manager.policy.max_files;
    }
    fn headroom(self: *Gate, bytes: u64, files: u64) bool {
        const target_bytes = @max(bytes, self.manager.policy.max_bytes - self.manager.policy.max_bytes / 16);
        const target_files = @max(files, self.manager.policy.max_files - self.manager.policy.max_files / 16);
        return self.stats.bytes <= target_bytes and bytes <= target_bytes - self.stats.bytes and self.stats.files <= target_files and files <= target_files - self.stats.files;
    }
    fn reclaim(self: *Gate, bytes: u64, files: u64, protect: ?[]const u8) !void {
        if (bytes > self.manager.policy.max_bytes or files > self.manager.policy.max_files) return error.ResourceLimitExceeded;
        if (self.fits(bytes, files)) return;
        while (true) {
            var scanned = try self.scan(protect);
            self.stats = scanned.stats;
            if (self.fits(bytes, files)) {
                try self.writeLedger();
                return;
            }
            const Order = struct {
                fn less(_: void, left: Row, right: Row) bool {
                    return left.modified < right.modified;
                }
            };
            std.mem.sort(Row, scanned.rows[0..scanned.count], {}, Order.less);
            var removed: usize = 0;
            for (scanned.rows[0..scanned.count]) |row| {
                if (self.dir.deleteFile(self.io, &row.name)) |_| {
                    self.stats.bytes -= row.bytes;
                    self.stats.files -= 1;
                    removed += 1;
                } else |_| {}
                if (self.headroom(bytes, files)) {
                    try self.writeLedger();
                    return;
                }
            }
            // Reclaim in batches under pressure, amortizing directory scans
            // across subsequent new sources while bounding sweep memory.
            if (self.fits(bytes, files)) {
                try self.writeLedger();
                return;
            }
            if (removed == 0) return error.ResourceLimitExceeded;
        }
    }
    pub fn reserve(self: *Gate, bytes: u64, files: u64) !void {
        try self.reclaim(bytes, files, &self.protect);
        self.reserved_bytes = bytes;
        self.reserved_files = files;
        self.stats.bytes += bytes;
        self.stats.files += files;
        self.stats.dirty = true;
        try self.writeLedger();
    }
    pub fn commit(self: *Gate, old_bytes: u64, new_bytes: u64, created: bool) !void {
        self.stats.bytes = self.stats.bytes - self.reserved_bytes - old_bytes + new_bytes;
        self.stats.files = self.stats.files - self.reserved_files + @intFromBool(created);
        self.stats.dirty = false;
        try self.writeLedger();
    }
};

fn publish(manager: *Manager, io: std.Io, root: []const u8, identity: [32]u8, data: []const u8) !void {
    var gate = try manager.open(io, root, identity);
    defer gate.deinit();
    const old = if (gate.dir.statFile(io, &gate.protect, .{})) |stat| @as(?u64, stat.size) else |_| null;
    try gate.reserve(data.len, @intFromBool(old == null));
    try gate.dir.writeFile(io, .{ .sub_path = &gate.protect, .data = data });
    try gate.commit(old orelse 0, data.len, old == null);
}

test "model aware media checkpoint quota bounds files and bytes without rescanning warm writes" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var manager = Manager{ .policy = .{ .max_bytes = 50, .max_files = 2, .max_age_seconds = 0, .sweep_seconds = 3600 } };
    try publish(&manager, io, root, @splat(1), "12345678901234567890");
    const scans = manager.scans.load(.monotonic);
    try publish(&manager, io, root, @splat(2), "12345678901234567890");
    try std.testing.expectEqual(scans, manager.scans.load(.monotonic));
    try publish(&manager, io, root, @splat(3), "12345678901234567890");
    {
        var gate = try manager.open(io, root, @splat(3));
        defer gate.deinit();
        const actual = try gate.scan(null);
        try std.testing.expectEqual(@as(u64, 2), actual.stats.files);
        try std.testing.expectEqual(@as(u64, 40), actual.stats.bytes);
        try std.testing.expectEqual(gate.stats.bytes, actual.stats.bytes);
        try gate.dir.writeFile(io, .{ .sub_path = "unrelated.bin", .data = "leave me" });
    }
    try std.testing.expectError(error.ResourceLimitExceeded, publish(&manager, io, root, @splat(4), "this new record is deliberately too large for this quota"));
    // A new process reconciles the ledger before relying on its totals.
    var restarted = Manager{ .policy = manager.policy };
    var gate = try restarted.open(io, root, @splat(5));
    defer gate.deinit();
    try std.testing.expectEqual(@as(u64, 40), gate.stats.bytes);
    try gate.dir.access(io, "unrelated.bin", .{});
}

test "model aware media checkpoint retention expires files and repairs interrupted reservations" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var manager = Manager{ .policy = .{ .max_bytes = 100, .max_files = 3, .max_age_seconds = 1, .sweep_seconds = 0 } };
    try publish(&manager, io, root, @splat(1), "old checkpoint");
    var old_name: [68]u8 = undefined;
    {
        var gate = try manager.open(io, root, @splat(1));
        defer gate.deinit();
        old_name = gate.protect;
        const file = try gate.dir.openFile(io, &old_name, .{ .mode = .read_write });
        defer file.close(io);
        try file.setTimestamps(io, .{ .modify_timestamp = .{ .new = .fromNanoseconds(1) } });
        var abandoned: [68]u8 = undefined;
        _ = try std.fmt.bufPrint(&abandoned, "{s}.tmp", .{std.fmt.bytesToHex(@as([32]u8, @splat(9)), .lower)});
        try gate.dir.writeFile(io, .{ .sub_path = &abandoned, .data = "abandoned temporary" });
        try gate.reserve(20, 1); // Crash before publishing or committing.
    }
    {
        var gate = try manager.open(io, root, @splat(2));
        defer gate.deinit();
        try std.testing.expectEqual(@as(u64, 0), gate.stats.bytes);
        try std.testing.expectEqual(@as(u64, 0), gate.stats.files);
        try std.testing.expectError(error.FileNotFound, gate.dir.access(io, &old_name, .{}));
        try gate.counter.writePositionalAll(io, "torn ledger", 0);
    }
    try publish(&manager, io, root, @splat(2), "new checkpoint");
    var gate = try manager.open(io, root, @splat(2));
    defer gate.deinit();
    try std.testing.expectEqual(@as(u64, 14), gate.stats.bytes);
    try std.testing.expectEqual(@as(u64, 1), gate.stats.files);
}

test "model aware media checkpoint quota coordinates independent writers and amortizes eviction scans" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var manager = Manager{ .policy = .{ .max_bytes = 1024 * 1024, .max_files = 32, .max_age_seconds = 0, .sweep_seconds = 3600 } };
    var peer = Manager{ .policy = manager.policy };
    {
        var gate = try manager.open(io, root, @splat(1));
        defer gate.deinit();
        try std.testing.expectError(error.WouldBlock, peer.open(io, root, @splat(2)));
    }
    for (0..33) |i| {
        var id: [32]u8 = @splat(0);
        id[0] = @intCast(i);
        try publish(&manager, io, root, id, "checkpoint");
    }
    const after_eviction = manager.scans.load(.monotonic);
    for (33..35) |i| {
        var id: [32]u8 = @splat(0);
        id[0] = @intCast(i);
        try publish(&manager, io, root, id, "checkpoint");
    }
    try std.testing.expectEqual(after_eviction, manager.scans.load(.monotonic));
    var gate = try peer.open(io, root, @splat(9));
    defer gate.deinit();
    try std.testing.expectEqual(@as(u64, 32), gate.stats.files);
}
