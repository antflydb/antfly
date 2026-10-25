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
pub const max_hot_points = 64;
const grid_points = max_points - max_hot_points - 1;
pub const max_encoded_bytes = 88 + 16 * max_points + 8 * max_hot_points;
pub const Point = struct { ordinal: u64, offset_ns: i64 };

pub fn strideFor(tail: u64) u64 {
    var stride: u64 = 1;
    while (tail / stride >= grid_points) stride *= 2;
    return stride;
}
pub const Snapshot = struct {
    points: []const Point,
    /// Zero denotes a legacy directory with no spacing policy.
    stride: u64 = 0,
    /// Recent inserted repair anchors, oldest first. Reads do not rewrite LRU.
    hot_ordinals: []const u64 = &.{},
    pub fn nearest(self: Snapshot, ordinal: u64) ?Point {
        const at = lowerBound(self.points, ordinal);
        if (at < self.points.len and self.points[at].ordinal == ordinal) return self.points[at];
        return if (at == 0) null else self.points[at - 1];
    }
    pub fn validate(self: Snapshot) !void {
        if (self.points.len == 0 or self.points.len > max_points or self.hot_ordinals.len > max_hot_points) return error.InvalidAudioPlan;
        if (self.stride != 0 and self.stride != strideFor(self.points[self.points.len - 1].ordinal)) return error.InvalidAudioPlan;
        for (self.points, 0..) |point, i| {
            if (point.offset_ns < 0) return error.InvalidAudioPlan;
            if (i != 0 and (point.ordinal <= self.points[i - 1].ordinal or point.offset_ns <= self.points[i - 1].offset_ns)) return error.InvalidAudioPlan;
            if (self.stride != 0 and point.ordinal % self.stride != 0 and i + 1 != self.points.len and !contains(self.hot_ordinals, point.ordinal)) return error.InvalidAudioPlan;
        }
        for (self.hot_ordinals, 0..) |ordinal, i| {
            const point = self.nearest(ordinal) orelse return error.InvalidAudioPlan;
            if (point.ordinal != ordinal or contains(self.hot_ordinals[0..i], ordinal)) return error.InvalidAudioPlan;
        }
    }
    /// Only decoded snapshots own these slices; cache and builder views borrow.
    pub fn deinit(self: Snapshot, a: std.mem.Allocator) void {
        a.free(self.points);
        a.free(self.hot_ordinals);
    }
    pub fn digest(self: Snapshot) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("audio-plan-directory-v2");
        for ([_]u64{ self.stride, self.points.len, self.hot_ordinals.len }) |n| hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, n)));
        for (self.points) |point| {
            hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, point.ordinal)));
            hash.update(&std.mem.toBytes(std.mem.nativeToLittle(i64, point.offset_ns)));
        }
        for (self.hot_ordinals) |n| hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, n)));
        return hash.finalResult();
    }
};
fn contains(values: []const u64, value: u64) bool {
    for (values) |n| if (n == value) return true;
    return false;
}
fn lowerBound(points: []const Point, ordinal: u64) usize {
    var lo: usize = 0;
    var hi = points.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (points[mid].ordinal < ordinal) lo = mid + 1 else hi = mid;
    }
    return lo;
}
const Policy = struct {
    tail: u64 = 0,
    stride: u64 = 1,
    hot: [max_hot_points]u64 = undefined,
    hot_count: usize = 0,
    fn hints(self: *const Policy) []const u64 {
        return self.hot[0..self.hot_count];
    }
    fn touch(self: *Policy, ordinal: u64) bool {
        if (contains(self.hints(), ordinal)) return false;
        if (self.hot_count == max_hot_points) {
            std.mem.copyForwards(u64, self.hot[0 .. max_hot_points - 1], self.hot[1..]);
            self.hot_count -= 1;
        }
        self.hot[self.hot_count] = ordinal;
        self.hot_count += 1;
        return true;
    }
    fn allows(self: *const Policy, ordinal: u64) bool {
        return ordinal == self.tail or ordinal % self.stride == 0 or contains(self.hints(), ordinal);
    }
};
fn filter(storage: []Point, count: *usize, policy: *const Policy) void {
    var out: usize = 0;
    for (storage[0..count.*]) |point| if (policy.allows(point.ordinal)) {
        storage[out] = point;
        out += 1;
    };
    count.* = out;
}
fn advance(storage: []Point, count: *usize, policy: *Policy, tail: u64) void {
    if (tail <= policy.tail) return;
    const old_stride = policy.stride;
    policy.tail = tail;
    policy.stride = strideFor(tail);
    if (old_stride != policy.stride) {
        filter(storage, count, policy);
    } else if (count.* != 0 and !policy.allows(storage[count.* - 1].ordinal)) count.* -= 1;
}
fn offer(storage: []Point, count: *usize, policy: *Policy, point: Point, hot: bool) !bool {
    if (point.offset_ns < 0) return error.InvalidAudioPlan;
    advance(storage, count, policy, point.ordinal);
    if (hot and policy.touch(point.ordinal)) filter(storage, count, policy);
    if (!policy.allows(point.ordinal)) return false;
    const at = lowerBound(storage[0..count.*], point.ordinal);
    if (at < count.* and storage[at].ordinal == point.ordinal) {
        if (storage[at].offset_ns != point.offset_ns) return error.InvalidAudioPlan;
        return false;
    }
    if ((at != 0 and storage[at - 1].offset_ns >= point.offset_ns) or (at < count.* and storage[at].offset_ns <= point.offset_ns) or count.* == max_points) return error.InvalidAudioPlan;
    std.mem.copyBackwards(Point, storage[at + 1 .. count.* + 1], storage[at..count.*]);
    storage[at] = point;
    count.* += 1;
    return true;
}
fn mergeInto(storage: []Point, count: *usize, policy: *Policy, snapshot: Snapshot) !void {
    try snapshot.validate();
    if (count.* == 0 and snapshot.stride != 0) {
        @memcpy(storage[0..snapshot.points.len], snapshot.points);
        count.* = snapshot.points.len;
        policy.tail = snapshot.points[snapshot.points.len - 1].ordinal;
        policy.stride = snapshot.stride;
        policy.hot_count = snapshot.hot_ordinals.len;
        @memcpy(policy.hot[0..policy.hot_count], snapshot.hot_ordinals);
        return;
    }
    advance(storage, count, policy, snapshot.points[snapshot.points.len - 1].ordinal);
    // Legacy files remain useful. Preserve their recent fine-grained tail
    // during the first migration, then fill older grid holes from real repairs.
    if (snapshot.stride == 0 and count.* == 0) {
        var first = snapshot.points.len;
        var selected: usize = 0;
        while (first != 0 and selected < max_hot_points) {
            first -= 1;
            const n = snapshot.points[first].ordinal;
            if (n != policy.tail and n % policy.stride != 0) selected += 1;
        }
        for (snapshot.points[first..]) |point| if (point.ordinal != policy.tail and point.ordinal % policy.stride != 0) {
            _ = policy.touch(point.ordinal);
        };
    }
    for (snapshot.hot_ordinals) |n| _ = policy.touch(n);
    filter(storage, count, policy);
    for (snapshot.points) |point| _ = try offer(storage, count, policy, point, false);
}

pub const Buffer = struct {
    points: [max_points]Point = undefined,
    count: usize = 0,
    policy: Policy = .{},
    pub fn append(self: *Buffer, point: Point, hot: bool) !void {
        _ = try offer(&self.points, &self.count, &self.policy, point, hot);
    }
    pub fn merge(self: *Buffer, value: Snapshot) !void {
        try mergeInto(&self.points, &self.count, &self.policy, value);
    }
    pub fn snapshot(self: *const Buffer) Snapshot {
        return .{ .points = self.points[0..self.count], .stride = self.policy.stride, .hot_ordinals = self.policy.hints() };
    }
};
pub const Builder = struct {
    points: std.ArrayList(Point) = .empty,
    policy: Policy = .{},
    pub fn deinit(self: *Builder, a: std.mem.Allocator) void {
        self.points.deinit(a);
    }
    pub fn append(self: *Builder, a: std.mem.Allocator, point: Point) !void {
        try self.points.ensureTotalCapacityPrecise(a, max_points);
        var count = self.points.items.len;
        defer self.points.items.len = count;
        _ = try offer(self.points.allocatedSlice(), &count, &self.policy, point, false);
    }
    pub fn merge(self: *Builder, a: std.mem.Allocator, value: Snapshot) !void {
        try self.points.ensureTotalCapacityPrecise(a, max_points);
        var count = self.points.items.len;
        defer self.points.items.len = count;
        try mergeInto(self.points.allocatedSlice(), &count, &self.policy, value);
    }
    pub fn snapshot(self: *const Builder) Snapshot {
        return .{ .points = self.points.items, .stride = self.policy.stride, .hot_ordinals = self.policy.hints() };
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
    if (value.stride != 0) return encodeCanonical(a, identity, value);
    var canonical = Buffer{};
    try canonical.merge(value);
    return encodeCanonical(a, identity, canonical.snapshot());
}
fn encodeCanonical(a: std.mem.Allocator, identity: [32]u8, snapshot: Snapshot) ![]u8 {
    const bytes = try a.alloc(u8, 88 + snapshot.points.len * 16 + snapshot.hot_ordinals.len * 8);
    @memcpy(bytes[0..8], "AFAP0002");
    @memcpy(bytes[8..40], &identity);
    std.mem.writeInt(u32, bytes[40..44], @intCast(snapshot.points.len), .little);
    std.mem.writeInt(u64, bytes[44..52], snapshot.stride, .little);
    std.mem.writeInt(u32, bytes[52..56], @intCast(snapshot.hot_ordinals.len), .little);
    for (snapshot.points, 0..) |point, i| {
        std.mem.writeInt(u64, bytes[56 + i * 16 ..][0..8], point.ordinal, .little);
        std.mem.writeInt(i64, bytes[64 + i * 16 ..][0..8], point.offset_ns, .little);
    }
    const hot_start = 56 + snapshot.points.len * 16;
    for (snapshot.hot_ordinals, 0..) |n, i| std.mem.writeInt(u64, bytes[hot_start + i * 8 ..][0..8], n, .little);
    std.crypto.hash.sha2.Sha256.hash(bytes[0 .. bytes.len - 32], bytes[bytes.len - 32 ..][0..32], .{});
    return bytes;
}
pub fn decode(a: std.mem.Allocator, identity: [32]u8, bytes: []const u8) !Snapshot {
    if (bytes.len < 92 or bytes.len > max_encoded_bytes or !std.mem.eql(u8, bytes[8..40], &identity)) return error.InvalidAudioPlan;
    const legacy = std.mem.eql(u8, bytes[0..8], "AFAP0001");
    if (!legacy and !std.mem.eql(u8, bytes[0..8], "AFAP0002")) return error.InvalidAudioPlan;
    const header: usize = if (legacy) 44 else 56;
    if (bytes.len < header + 48) return error.InvalidAudioPlan;
    const count = std.mem.readInt(u32, bytes[40..44], .little);
    const hot_count = if (legacy) 0 else std.mem.readInt(u32, bytes[52..56], .little);
    if (count == 0 or count > max_points or hot_count > max_hot_points or bytes.len != header + @as(usize, count) * 16 + @as(usize, hot_count) * 8 + 32) return error.InvalidAudioPlan;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes[0 .. bytes.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidAudioPlan;
    const points = try a.alloc(Point, count);
    errdefer a.free(points);
    const hot = try a.alloc(u64, hot_count);
    errdefer a.free(hot);
    for (points, 0..) |*point, i| point.* = .{ .ordinal = std.mem.readInt(u64, bytes[header + i * 16 ..][0..8], .little), .offset_ns = std.mem.readInt(i64, bytes[header + 8 + i * 16 ..][0..8], .little) };
    const hot_start = header + @as(usize, count) * 16;
    for (hot, 0..) |*n, i| n.* = std.mem.readInt(u64, bytes[hot_start + i * 8 ..][0..8], .little);
    const value = Snapshot{ .points = points, .stride = if (legacy) 0 else std.mem.readInt(u64, bytes[44..52], .little), .hot_ordinals = hot };
    try value.validate();
    return value;
}

/// Hash slots bound journals and temporary snapshots independently of duration.
/// Append records are synced before success; torn suffixes recover the last
/// verified state. Old snapshot-only files remain readable.
pub const Store = struct {
    mutexes: [256]std.Io.Mutex = @splat(.init),
    pub const max_records = 65; // one base plus at most 64 delta records
    const trailer_bytes = 112;
    const record_overhead = 4 + trailer_bytes;
    pub const max_file_bytes = 40 + 2 * (max_encoded_bytes + record_overhead);
    /// Internally derived canonical delta; value is its merge with base_digest.
    pub const Update = struct { base_digest: [32]u8, delta: Snapshot };
    pub const Context = struct { store: *Store, io: std.Io, models_dir: []const u8 };
    fn path(buffer: []u8, root: []const u8, identity: [32]u8, temporary: bool) ![]const u8 {
        return std.fmt.bufPrint(buffer, "{s}/.antfly-audio-plans/{x:0>2}{s}", .{ root, identity[0], if (temporary) ".pending" else ".plan" });
    }
    pub fn load(context: Context, a: std.mem.Allocator, identity: [32]u8) !Snapshot {
        if (comptime @import("builtin").os.tag == .freestanding) return error.UnsupportedPlatform;
        var buffer: [std.fs.max_path_bytes]u8 = undefined;
        const bytes = try @import("../runtime/file_snapshot.zig").read(a, context.io, .cwd(), try path(&buffer, context.models_dir, identity, false), max_file_bytes, null);
        defer a.free(bytes);
        if (bytes.len < 40 or !std.mem.eql(u8, bytes[0..8], "AFAP0003")) return decode(a, identity, bytes);
        if (!std.mem.eql(u8, bytes[8..40], &identity)) return error.InvalidAudioPlan;
        var state = Buffer{};
        var digest: [32]u8 = @splat(0);
        var count: u32 = 0;
        var at: usize = 40;
        while (at + record_overhead <= bytes.len and count < max_records) {
            const length = std.mem.readInt(u32, bytes[at..][0..4], .little);
            if (length > max_encoded_bytes or length + record_overhead > bytes.len - at) break;
            const end = at + 4 + length;
            const trailer = bytes[end..][0..trailer_bytes];
            if (!std.mem.eql(u8, trailer[104..112], "AFAC0001") or std.mem.readInt(u32, trailer[96..100], .little) != length + record_overhead or std.mem.readInt(u32, trailer[100..104], .little) != count + 1 or !std.mem.eql(u8, trailer[0..32], &digest)) break;
            var checksum: [32]u8 = undefined;
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update(bytes[at .. end + 64]);
            hash.update(trailer[96..112]);
            hash.final(&checksum);
            if (!std.mem.eql(u8, &checksum, trailer[64..96])) break;
            const delta = decode(a, identity, bytes[at + 4 .. end]) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => break,
            };
            defer delta.deinit(a);
            // A failed merge may mutate its destination. Publish only a fully
            // verified next state, retaining the preceding valid prefix.
            var next = state;
            next.merge(delta) catch break;
            const next_digest = next.snapshot().digest();
            if (!std.mem.eql(u8, &next_digest, trailer[32..64])) break;
            state = next;
            digest = next_digest;
            count += 1;
            at = end + trailer_bytes;
        }
        if (count == 0) return error.InvalidAudioPlan;
        const points = try a.dupe(Point, state.snapshot().points);
        errdefer a.free(points);
        const hot = try a.dupe(u64, state.snapshot().hot_ordinals);
        return .{ .points = points, .stride = state.policy.stride, .hot_ordinals = hot };
    }
    fn frame(a: std.mem.Allocator, identity: [32]u8, delta: Snapshot, prior: [32]u8, result: [32]u8, count: u32) ![]u8 {
        const payload = try encode(a, identity, delta);
        defer a.free(payload);
        const bytes = try a.alloc(u8, payload.len + record_overhead);
        std.mem.writeInt(u32, bytes[0..4], @intCast(payload.len), .little);
        @memcpy(bytes[4..][0..payload.len], payload);
        const trailer = bytes[4 + payload.len ..][0..trailer_bytes];
        @memcpy(trailer[0..32], &prior);
        @memcpy(trailer[32..64], &result);
        std.mem.writeInt(u32, trailer[96..100], @intCast(bytes.len), .little);
        std.mem.writeInt(u32, trailer[100..104], count, .little);
        @memcpy(trailer[104..112], "AFAC0001");
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(bytes[0 .. 4 + payload.len + 64]);
        hash.update(trailer[96..112]);
        hash.final(trailer[64..96]);
        return bytes;
    }
    /// Caller-derived updates can append after checking only the committed
    /// trailer. A changed owner, collision or old encoding takes the merge path.
    fn append(context: Context, a: std.mem.Allocator, identity: [32]u8, value: Snapshot, update: Update) !bool {
        if (value.stride == 0 or update.delta.stride == 0) return false;
        var buffer: [std.fs.max_path_bytes]u8 = undefined;
        if (comptime @import("builtin").os.tag != .linux and @import("builtin").os.tag != .macos) return false;
        const name = try path(&buffer, context.models_dir, identity, false);
        if (std.mem.indexOfScalar(u8, name, 0) != null) return false;
        var terminated_buffer: [std.fs.max_path_bytes + 1]u8 = undefined;
        const terminated = try std.fmt.bufPrintSentinel(&terminated_buffer, "{s}", .{name}, 0);
        const fd = std.posix.openatZ(std.Io.Dir.cwd().handle, terminated, .{ .ACCMODE = .RDWR, .CLOEXEC = true, .NONBLOCK = true, .NOCTTY = true }, 0) catch return false;
        const file = std.Io.File{ .handle = fd, .flags = .{ .nonblocking = true } };
        defer file.close(context.io);
        const stat = try file.stat(context.io);
        if (stat.kind != .file or stat.size < 40 + record_overhead or stat.size > max_file_bytes) return false;
        var header: [40]u8 = undefined;
        if (try file.readPositionalAll(context.io, &header, 0) != header.len or !std.mem.eql(u8, header[0..8], "AFAP0003") or !std.mem.eql(u8, header[8..40], &identity)) return false;
        var trailer: [trailer_bytes]u8 = undefined;
        if (try file.readPositionalAll(context.io, &trailer, stat.size - trailer_bytes) != trailer_bytes or !std.mem.eql(u8, trailer[104..112], "AFAC0001") or !std.mem.eql(u8, trailer[32..64], &update.base_digest)) return false;
        const count = std.mem.readInt(u32, trailer[100..104], .little);
        if (count == 0 or count >= max_records) return false;
        const bytes = try frame(a, identity, update.delta, update.base_digest, value.digest(), count + 1);
        defer a.free(bytes);
        if (bytes.len > max_file_bytes - stat.size) return false;
        try file.writePositionalAll(context.io, bytes, stat.size);
        try file.sync(context.io);
        return true;
    }
    pub fn save(context: Context, a: std.mem.Allocator, identity: [32]u8, value: Snapshot) !void {
        return saveUpdate(context, a, identity, value, null);
    }
    pub fn saveUpdate(context: Context, a: std.mem.Allocator, identity: [32]u8, value: Snapshot, update: ?Update) !void {
        if (comptime @import("builtin").os.tag == .freestanding) return error.UnsupportedPlatform;
        try value.validate();
        const mutex = &context.store.mutexes[identity[0]];
        try mutex.lock(context.io);
        defer mutex.unlock(context.io);
        var directory: [std.fs.max_path_bytes]u8 = undefined;
        try std.Io.Dir.cwd().createDirPath(context.io, try std.fmt.bufPrint(&directory, "{s}/.antfly-audio-plans", .{context.models_dir}));
        var lock_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const lock_file = try std.Io.Dir.cwd().createFile(context.io, try std.fmt.bufPrint(&lock_buffer, "{s}/.antfly-audio-plans/{x:0>2}.lock", .{ context.models_dir, identity[0] }), .{ .truncate = false, .lock = .exclusive, .lock_nonblocking = true });
        defer lock_file.close(context.io);
        if (update) |change| if (try append(context, a, identity, value, change)) return;
        var merged = Buffer{};
        var previous_digest: ?[32]u8 = null;
        if (load(context, a, identity)) |previous| {
            defer previous.deinit(a);
            previous_digest = previous.digest();
            try merged.merge(previous);
        } else |_| {}
        try merged.merge(value);
        const digest = merged.snapshot().digest();
        if (previous_digest) |prior| if (std.mem.eql(u8, &prior, &digest)) return;
        var target_buffer: [std.fs.max_path_bytes]u8 = undefined;
        var pending_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const target = try path(&target_buffer, context.models_dir, identity, false);
        const pending = try path(&pending_buffer, context.models_dir, identity, true);
        const bytes = try frame(a, identity, merged.snapshot(), @splat(0), digest, 1);
        defer a.free(bytes);
        defer std.Io.Dir.cwd().deleteFile(context.io, pending) catch {};
        {
            const file = try std.Io.Dir.cwd().createFile(context.io, pending, .{});
            defer file.close(context.io);
            var header: [40]u8 = undefined;
            @memcpy(header[0..8], "AFAP0003");
            @memcpy(header[8..40], &identity);
            try file.writeStreamingAll(context.io, &header);
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
    defer loaded.deinit(a);
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
    try builder.merge(a, .{ .points = &.{.{ .ordinal = 100000, .offset_ns = 10000000 }}, .hot_ordinals = &.{100000} });
    const bytes = try encode(a, @splat(1), builder.snapshot());
    defer a.free(bytes);
    const decoded = try decode(a, @splat(1), bytes);
    defer decoded.deinit(a);
    try std.testing.expectEqualDeep(builder.points.items, decoded.points);
    try std.testing.expectEqualDeep(builder.snapshot().hot_ordinals, decoded.hot_ordinals);
}
test "model aware media audio plan allocation failures release builders encodings and decoded checkpoints" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationProbe, .{});
}

test "model aware media checkpoint spacing bounds every historical gap and preserves exact coordinates" {
    const a = std.testing.allocator;
    var builder = Builder{};
    defer builder.deinit(a);
    for (0..100001) |i| {
        try builder.append(a, .{ .ordinal = i, .offset_ns = @intCast(i * 100000 + i % 101 * 3) });
        if (i == 4095 or i == 8193 or i == 100000) {
            const value = builder.snapshot();
            try value.validate();
            try std.testing.expect(value.points.len <= max_points);
            try std.testing.expectEqual(@as(u64, 0), value.points[0].ordinal);
            try std.testing.expectEqual(@as(u64, i), value.points[value.points.len - 1].ordinal);
            for (value.points, 0..) |point, n| {
                try std.testing.expectEqual(@as(i64, @intCast(point.ordinal * 100000 + point.ordinal % 101 * 3)), point.offset_ns);
                if (n != 0) try std.testing.expect(point.ordinal - value.points[n - 1].ordinal <= value.stride);
            }
        }
    }
    try std.testing.expectEqual(@as(u64, 37888), builder.snapshot().nearest(37889).?.ordinal);
}

test "model aware media checkpoint codec reads legacy files and persists same-tail backfills" {
    const a = std.testing.allocator;
    const identity: [32]u8 = @splat(3);
    const legacy_points = [_]Point{ .{ .ordinal = 0, .offset_ns = 0 }, .{ .ordinal = 100000, .offset_ns = 100000000 } };
    var legacy: [108]u8 = undefined;
    @memcpy(legacy[0..8], "AFAP0001");
    @memcpy(legacy[8..40], &identity);
    std.mem.writeInt(u32, legacy[40..44], 2, .little);
    for (legacy_points, 0..) |point, i| {
        std.mem.writeInt(u64, legacy[44 + i * 16 ..][0..8], point.ordinal, .little);
        std.mem.writeInt(i64, legacy[52 + i * 16 ..][0..8], point.offset_ns, .little);
    }
    std.crypto.hash.sha2.Sha256.hash(legacy[0..76], legacy[76..108], .{});
    const decoded = try decode(a, identity, &legacy);
    defer decoded.deinit(a);
    try std.testing.expectEqual(@as(u64, 0), decoded.stride);
    try std.testing.expectEqualDeep(&legacy_points, decoded.points);
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    var root_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&root_buffer, ".zig-cache/tmp/{s}", .{temp.sub_path});
    var store = Store{};
    const context = Store.Context{ .store = &store, .io = std.testing.io, .models_dir = root };
    try Store.save(context, a, identity, decoded);
    for ([_]u64{ 37889, 40001 }) |ordinal| {
        const point = Point{ .ordinal = ordinal, .offset_ns = @intCast(ordinal * 1000) };
        try Store.save(context, a, identity, .{ .points = &.{point}, .hot_ordinals = &.{ordinal} });
    }
    const saved = try Store.load(context, a, identity);
    defer saved.deinit(a);
    try saved.validate();
    try std.testing.expectEqual(@as(u64, 100000), saved.points[saved.points.len - 1].ordinal);
    try std.testing.expectEqual(@as(u64, 37889), saved.nearest(37889).?.ordinal);
    try std.testing.expectEqual(@as(u64, 40001), saved.nearest(40001).?.ordinal);
    var bounded = Buffer{};
    try bounded.merge(saved);
    for (0..max_hot_points * 3) |i| try bounded.append(.{ .ordinal = 50001 + i * 2, .offset_ns = @intCast((50001 + i * 2) * 1000) }, true);
    try bounded.snapshot().validate();
    try std.testing.expectEqual(@as(usize, max_hot_points), bounded.snapshot().hot_ordinals.len);
    try std.testing.expect(bounded.count <= max_points);
    try std.testing.expectEqual(@as(u64, 100000), bounded.snapshot().points[bounded.count - 1].ordinal);
}

test "model aware media journal recovers every torn suffix and merges stale repair writers" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    var root_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&root_buffer, ".zig-cache/tmp/{s}", .{temp.sub_path});
    var store = Store{};
    const context = Store.Context{ .store = &store, .io = std.testing.io, .models_dir = root };
    const identity: [32]u8 = @splat(7);
    var base = Buffer{};
    try base.append(.{ .ordinal = 0, .offset_ns = 0 }, false);
    try base.append(.{ .ordinal = 100000, .offset_ns = 100000000 }, false);
    try Store.save(context, a, identity, base.snapshot());
    var file_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const file = try Store.path(&file_buffer, root, identity, false);
    const original = try @import("../runtime/file_snapshot.zig").read(a, std.testing.io, .cwd(), file, Store.max_file_bytes, null);
    defer a.free(original);
    var delta = Buffer{};
    try delta.append(.{ .ordinal = 50001, .offset_ns = 50001000 }, true);
    var next = base;
    try next.merge(delta.snapshot());
    try Store.saveUpdate(context, a, identity, next.snapshot(), .{ .base_digest = base.snapshot().digest(), .delta = delta.snapshot() });
    const complete = try @import("../runtime/file_snapshot.zig").read(a, std.testing.io, .cwd(), file, Store.max_file_bytes, null);
    defer a.free(complete);
    try std.testing.expect(complete.len > original.len);
    for (original.len..complete.len) |cut| {
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = file, .data = complete[0..cut] });
        const saved = try Store.load(context, a, identity);
        defer saved.deinit(a);
        try std.testing.expectEqualDeep(base.snapshot().points, saved.points);
    }
    // A complete checksum-invalid suffix is ignored, too.
    const corrupt = try a.dupe(u8, complete);
    defer a.free(corrupt);
    corrupt[original.len + 48] ^= 1;
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = file, .data = corrupt });
    {
        const saved = try Store.load(context, a, identity);
        defer saved.deinit(a);
        try std.testing.expectEqualDeep(base.snapshot().points, saved.points);
    }
    // Saving after a torn trailer replaces it with a verified compact base.
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = file, .data = complete[0 .. complete.len - 1] });
    try Store.saveUpdate(context, a, identity, next.snapshot(), .{ .base_digest = base.snapshot().digest(), .delta = delta.snapshot() });
    var stale_delta = Buffer{};
    try stale_delta.append(.{ .ordinal = 60001, .offset_ns = 60001000 }, true);
    var stale = base;
    try stale.merge(stale_delta.snapshot());
    var second_owner = Store{};
    try Store.saveUpdate(.{ .store = &second_owner, .io = std.testing.io, .models_dir = root }, a, identity, stale.snapshot(), .{ .base_digest = base.snapshot().digest(), .delta = stale_delta.snapshot() });
    const saved = try Store.load(context, a, identity);
    defer saved.deinit(a);
    try std.testing.expectEqual(@as(u64, 50001), saved.nearest(50001).?.ordinal);
    try std.testing.expectEqual(@as(u64, 60001), saved.nearest(60001).?.ordinal);
    try std.testing.expectEqual(@as(u64, 100000), saved.points[saved.points.len - 1].ordinal);
    var collision = identity;
    collision[31] ^= 1;
    try std.testing.expectError(error.InvalidAudioPlan, Store.load(context, a, collision));
}

fn journalAllocationProbe(a: std.mem.Allocator) !void {
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    var root_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&root_buffer, ".zig-cache/tmp/{s}", .{temp.sub_path});
    var store = Store{};
    const context = Store.Context{ .store = &store, .io = std.testing.io, .models_dir = root };
    var base = Buffer{};
    try base.append(.{ .ordinal = 0, .offset_ns = 0 }, false);
    try base.append(.{ .ordinal = 100000, .offset_ns = 100000000 }, false);
    const identity: [32]u8 = @splat(6);
    try Store.save(context, a, identity, base.snapshot());
    var delta = Buffer{};
    try delta.append(.{ .ordinal = 50001, .offset_ns = 50001000 }, true);
    var next = base;
    try next.merge(delta.snapshot());
    try Store.saveUpdate(context, a, identity, next.snapshot(), .{ .base_digest = base.snapshot().digest(), .delta = delta.snapshot() });
    const saved = try Store.load(context, a, identity);
    defer saved.deinit(a);
    try std.testing.expectEqualDeep(next.snapshot().points, saved.points);
    try std.testing.expectEqualDeep(next.snapshot().hot_ordinals, saved.hot_ordinals);
}
test "model aware media journal releases allocations during append and recovery failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, journalAllocationProbe, .{});
}

test "model aware media journal byte ceiling compacts large deltas before the record ceiling" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    var root_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&root_buffer, ".zig-cache/tmp/{s}", .{temp.sub_path});
    var store = Store{};
    const context = Store.Context{ .store = &store, .io = std.testing.io, .models_dir = root };
    const identity: [32]u8 = @splat(12);
    var builder = Builder{};
    defer builder.deinit(a);
    for (0..4001) |ordinal| try builder.append(a, .{ .ordinal = ordinal, .offset_ns = @intCast(ordinal * 1000) });
    try Store.save(context, a, identity, builder.snapshot());
    var previous_length: usize = 0;
    var compacted = false;
    for (4001..4005) |ordinal| {
        const prior = builder.snapshot().digest();
        try builder.append(a, .{ .ordinal = ordinal, .offset_ns = @intCast(ordinal * 1000) });
        // Full snapshots are legitimate large repair deltas. Their byte limit
        // must trigger compaction well before 64 records accumulate.
        try Store.saveUpdate(context, a, identity, builder.snapshot(), .{ .base_digest = prior, .delta = builder.snapshot() });
        var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const bytes = try @import("../runtime/file_snapshot.zig").read(a, std.testing.io, .cwd(), try Store.path(&path_buffer, root, identity, false), Store.max_file_bytes, null);
        defer a.free(bytes);
        if (bytes.len < previous_length) compacted = true;
        previous_length = bytes.len;
        const saved = try Store.load(context, a, identity);
        defer saved.deinit(a);
        try std.testing.expectEqualDeep(builder.snapshot().points, saved.points);
    }
    try std.testing.expect(compacted);
}
