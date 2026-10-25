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

//! Verified pause-aware planning checkpoints. Only locally derived boundaries
//! enter this directory; caller continuations still contain only plan/ordinal.
const std = @import("std");
const media = @import("antfly_media");
pub const max_points = 4096;
pub const max_encoded_bytes = 76 + 16 * max_points;
pub const Point = struct { ordinal: u64, offset_ns: i64 };
pub const Snapshot = struct {
    points: []const Point,
    pub fn nearest(self: Snapshot, ordinal: u64) ?Point {
        var lo: usize = 0;
        var hi = self.points.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.points[mid].ordinal <= ordinal) lo = mid + 1 else hi = mid;
        }
        return if (lo == 0) null else self.points[lo - 1];
    }
    pub fn validate(self: Snapshot) !void {
        if (self.points.len == 0 or self.points.len > max_points) return error.InvalidAudioPlan;
        for (self.points, 0..) |point, i| {
            if (point.offset_ns < 0) return error.InvalidAudioPlan;
            if (i != 0 and (point.ordinal <= self.points[i - 1].ordinal or point.offset_ns <= self.points[i - 1].offset_ns)) return error.InvalidAudioPlan;
        }
    }
};
pub const Builder = struct {
    points: std.ArrayList(Point) = .empty,
    pub fn deinit(self: *Builder, a: std.mem.Allocator) void {
        self.points.deinit(a);
    }
    pub fn append(self: *Builder, a: std.mem.Allocator, point: Point) !void {
        if (self.points.items.len != 0 and point.ordinal <= self.points.items[self.points.items.len - 1].ordinal) return;
        if (self.points.items.len == max_points) {
            // Keep old anchors as well as the advancing tail. Compaction
            // bounds metadata independently of source duration.
            var count: usize = 0;
            for (self.points.items, 0..) |old, i| if (i % 2 == 0) {
                self.points.items[count] = old;
                count += 1;
            };
            self.points.items.len = count;
        }
        try self.points.append(a, point);
    }
    pub fn snapshot(self: Builder) Snapshot {
        return .{ .points = self.points.items };
    }
};

pub fn key(source: *const media.source.Source, plan: [32]u8, rate: u32, samples: usize) ?[32]u8 {
    const binding = switch (source.storage) {
        .range => |range| range.immutable_binding orelse return null,
        .borrowed => return null,
    };
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-pause-plan-v1");
    hash.update(@import("build_info").version());
    hash.update(&binding);
    hash.update(&plan);
    hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u32, rate)));
    hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, samples)));
    return hash.finalResult();
}

pub fn encode(a: std.mem.Allocator, identity: [32]u8, value: Snapshot) ![]u8 {
    try value.validate();
    const bytes = try a.alloc(u8, 76 + value.points.len * 16);
    @memcpy(bytes[0..8], "AFAP0001");
    @memcpy(bytes[8..40], &identity);
    std.mem.writeInt(u32, bytes[40..44], @intCast(value.points.len), .little);
    for (value.points, 0..) |point, i| {
        std.mem.writeInt(u64, bytes[44 + i * 16 ..][0..8], point.ordinal, .little);
        std.mem.writeInt(i64, bytes[52 + i * 16 ..][0..8], point.offset_ns, .little);
    }
    std.crypto.hash.sha2.Sha256.hash(bytes[0 .. bytes.len - 32], bytes[bytes.len - 32 ..][0..32], .{});
    return bytes;
}
pub fn decode(a: std.mem.Allocator, identity: [32]u8, bytes: []const u8) !Snapshot {
    if (bytes.len < 92 or bytes.len > max_encoded_bytes or !std.mem.eql(u8, bytes[0..8], "AFAP0001") or !std.mem.eql(u8, bytes[8..40], &identity)) return error.InvalidAudioPlan;
    const count = std.mem.readInt(u32, bytes[40..44], .little);
    if (count == 0 or count > max_points or bytes.len != 76 + @as(usize, count) * 16) return error.InvalidAudioPlan;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes[0 .. bytes.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidAudioPlan;
    const points = try a.alloc(Point, count);
    errdefer a.free(points);
    for (points, 0..) |*point, i| point.* = .{ .ordinal = std.mem.readInt(u64, bytes[44 + i * 16 ..][0..8], .little), .offset_ns = std.mem.readInt(i64, bytes[52 + i * 16 ..][0..8], .little) };
    const value = Snapshot{ .points = points };
    try value.validate();
    return value;
}

/// Fixed disk slots bound both bytes and file count (256 snapshots, at most
/// 16.1 MiB plus 256 same-sized temporary files and 256 empty lock files). Hash collisions evict an
/// accelerator; the full key and checksum always fence reads. Read-only model
/// directories simply disable persistence. No media or request context is saved.
pub const Store = struct {
    mutexes: [256]std.Io.Mutex = @splat(.init),
    pub const Context = struct { store: *Store, io: std.Io, models_dir: []const u8 };
    fn path(buffer: []u8, root: []const u8, identity: [32]u8, temporary: bool) ![]const u8 {
        return std.fmt.bufPrint(buffer, "{s}/.antfly-audio-plans/{x:0>2}{s}", .{ root, identity[0], if (temporary) ".pending" else ".plan" });
    }
    pub fn load(context: Context, a: std.mem.Allocator, identity: [32]u8) !Snapshot {
        if (comptime @import("builtin").os.tag == .freestanding) return error.UnsupportedPlatform;
        var buffer: [std.fs.max_path_bytes]u8 = undefined;
        const name = try path(&buffer, context.models_dir, identity, false);
        const bytes = try @import("../runtime/file_snapshot.zig").read(a, context.io, .cwd(), name, max_encoded_bytes, null);
        defer a.free(bytes);
        return decode(a, identity, bytes);
    }
    pub fn save(context: Context, a: std.mem.Allocator, identity: [32]u8, value: Snapshot) !void {
        if (comptime @import("builtin").os.tag == .freestanding) return error.UnsupportedPlatform;
        try value.validate();
        const mutex = &context.store.mutexes[identity[0]];
        try mutex.lock(context.io);
        defer mutex.unlock(context.io);
        var directory: [std.fs.max_path_bytes]u8 = undefined;
        try std.Io.Dir.cwd().createDirPath(context.io, try std.fmt.bufPrint(&directory, "{s}/.antfly-audio-plans", .{context.models_dir}));
        // Multiple inference owners may share a model directory. A permanent
        // slot lock protects its replaceable temporary/target names. Contention
        // skips this optional write; a crashed owner releases the OS lock.
        var lock_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const lock_path = try std.fmt.bufPrint(&lock_buffer, "{s}/.antfly-audio-plans/{x:0>2}.lock", .{ context.models_dir, identity[0] });
        const lock_file = try std.Io.Dir.cwd().createFile(context.io, lock_path, .{ .truncate = false, .lock = .exclusive, .lock_nonblocking = true });
        defer lock_file.close(context.io);
        if (load(context, a, identity)) |previous| {
            defer a.free(previous.points);
            if (previous.points[previous.points.len - 1].ordinal >= value.points[value.points.len - 1].ordinal) return;
        } else |_| {}
        var target_buffer: [std.fs.max_path_bytes]u8 = undefined;
        var pending_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const target = try path(&target_buffer, context.models_dir, identity, false);
        const pending = try path(&pending_buffer, context.models_dir, identity, true);
        const bytes = try encode(a, identity, value);
        defer a.free(bytes);
        defer std.Io.Dir.cwd().deleteFile(context.io, pending) catch {};
        {
            const file = try std.Io.Dir.cwd().createFile(context.io, pending, .{});
            defer file.close(context.io);
            try file.writeStreamingAll(context.io, bytes);
            try file.sync(context.io);
        }
        try std.Io.Dir.cwd().rename(pending, .cwd(), target, context.io);
    }
};

test "model aware media audio plan compaction bounds metadata and keeps ordered restart anchors" {
    const a = std.testing.allocator;
    var builder = Builder{};
    defer builder.deinit(a);
    for (0..max_points * 5) |i| try builder.append(a, .{ .ordinal = i, .offset_ns = @intCast(i * 1000) });
    try builder.snapshot().validate();
    try std.testing.expect(builder.points.items.len <= max_points);
    try std.testing.expectEqual(@as(u64, max_points * 5 - 1), builder.snapshot().nearest(max_points * 5).?.ordinal);
    try std.testing.expectEqual(@as(u64, 0), builder.snapshot().nearest(0).?.ordinal);
}

test "model aware media audio plan disk checkpoints survive owner restart and reject changed bindings or corruption" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    var root_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&root_buffer, ".zig-cache/tmp/{s}", .{temp.sub_path});
    var first = Store{};
    const context = Store.Context{ .store = &first, .io = std.testing.io, .models_dir = root };
    const identity: [32]u8 = @splat(7);
    const points = [_]Point{ .{ .ordinal = 0, .offset_ns = 0 }, .{ .ordinal = 32, .offset_ns = 32000 } };
    try Store.save(context, a, identity, .{ .points = &points });
    var restarted = Store{};
    const loaded = try Store.load(.{ .store = &restarted, .io = std.testing.io, .models_dir = root }, a, identity);
    defer a.free(loaded.points);
    try std.testing.expectEqualDeep(&points, loaded.points);
    var collision = identity;
    collision[1] = 8; // Same disk slot, different verified key.
    try std.testing.expectError(error.InvalidAudioPlan, Store.load(context, a, collision));
    const bytes = try encode(a, identity, loaded);
    defer a.free(bytes);
    bytes[52] ^= 1;
    try std.testing.expectError(error.InvalidAudioPlan, decode(a, identity, bytes));
}

test "model aware media audio plan keys bind source plan rate and sample count" {
    const Provider = struct {
        fn read(_: *anyopaque, _: u64, _: []u8, _: media.source.Control) !usize {
            return 0;
        }
    };
    var context: u8 = 0;
    var source = media.source.Source{ .allocator = std.testing.allocator, .identity = "source", .storage = .{ .range = .{ .context = &context, .read_at = Provider.read, .length = 100, .immutable_binding = @splat(1) } } };
    const original = key(&source, @splat(2), 48000, 100).?;
    try std.testing.expect(!std.mem.eql(u8, &original, &key(&source, @splat(3), 48000, 100).?));
    try std.testing.expect(!std.mem.eql(u8, &original, &key(&source, @splat(2), 44100, 100).?));
    try std.testing.expect(!std.mem.eql(u8, &original, &key(&source, @splat(2), 48000, 101).?));
    source.storage.range.immutable_binding = @splat(4);
    try std.testing.expect(!std.mem.eql(u8, &original, &key(&source, @splat(2), 48000, 100).?));
    source.storage.range.immutable_binding = null;
    try std.testing.expectEqual(@as(?[32]u8, null), key(&source, @splat(2), 48000, 100));
}

fn allocationProbe(a: std.mem.Allocator) !void {
    var builder = Builder{};
    defer builder.deinit(a);
    for (0..19) |i| try builder.append(a, .{ .ordinal = i, .offset_ns = @intCast(i * 100) });
    const bytes = try encode(a, @splat(1), builder.snapshot());
    defer a.free(bytes);
    const decoded = try decode(a, @splat(1), bytes);
    defer a.free(decoded.points);
    try std.testing.expectEqualDeep(builder.points.items, decoded.points);
}
test "model aware media audio plan allocation failures release builders encodings and decoded checkpoints" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationProbe, .{});
}
