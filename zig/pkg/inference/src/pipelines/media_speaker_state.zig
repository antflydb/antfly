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

//! Immutable, source-bound speaker checkpoints. Decoder restart positions are
//! absolute sample-grid ordinals; codec providers supply their exact seek/preroll.
const std = @import("std");
pub const timeline = @import("media_speaker_timeline.zig");
const speakers = @import("inference_chunker").diarization;
pub const max_encoded_bytes = 74 + 64 * 512 * 4 + speakers.max_spans * 20 + 32;
pub const Snapshot = struct {
    registry: *const speakers.Registry,
    spans: []const speakers.Span = &.{},
    shared: ?timeline.View = null,
    sample_rate: u32,
    next_window: u64 = 0,
    covered_samples: u64 = 0,
    total_samples: ?u64 = null,
    complete: bool = false,
    pub fn spanCount(self: Snapshot) usize {
        return if (self.shared) |view| view.count else self.spans.len;
    }
    pub fn spanAt(self: Snapshot, i: usize) speakers.Span {
        return if (self.shared) |view| view.at(i) else self.spans[i];
    }
    pub fn prefixDigest(self: Snapshot, count: usize) [32]u8 {
        return if (self.shared) |view| view.prefixDigest(count) else spansHash(self.spans[0..count]);
    }
    pub fn validate(self: Snapshot) !void {
        if (self.sample_rate == 0 or self.registry.count > 64 or self.spanCount() > speakers.max_spans) return error.InvalidSpeakerCheckpoint;
        const expected: u128 = @as(u128, self.next_window) * self.sample_rate * 3 / 2;
        if (self.complete) {
            if (self.total_samples == null or self.covered_samples != self.total_samples.?) return error.InvalidSpeakerCheckpoint;
            if (self.covered_samples > expected or (self.next_window != 0 and self.covered_samples <= @as(u128, self.next_window - 1) * self.sample_rate * 3 / 2)) return error.InvalidSpeakerCheckpoint;
        } else if (expected != self.covered_samples) return error.InvalidSpeakerCheckpoint;
        if (self.total_samples) |total| if (self.covered_samples > total) return error.InvalidSpeakerCheckpoint;
        var end: i64 = 0;
        if (self.shared) |view| {
            try view.validate(self.registry.count);
            if (view.count > 0) end = view.at(view.count - 1).end_ns;
        } else {
            for (self.spans) |span| {
                if (span.start_ns != end or span.end_ns <= span.start_ns) return error.InvalidSpeakerCheckpoint;
                if (span.speaker_index) |index| if (index >= self.registry.count) return error.InvalidSpeakerCheckpoint;
                end = span.end_ns;
            }
        }
        const ns = (@as(u128, self.covered_samples) * std.time.ns_per_s + (if (self.complete) @as(u128, self.sample_rate - 1) else 0)) / self.sample_rate;
        if (ns != @as(u128, @intCast(end))) return error.InvalidSpeakerCheckpoint;
        for (self.registry.sums[0..self.registry.count]) |sum| {
            var norm: f64 = 0;
            for (sum) |v| {
                if (!std.math.isFinite(v)) return error.InvalidSpeakerCheckpoint;
                norm += @as(f64, v) * v;
            }
            if (norm < 1e-12) return error.InvalidSpeakerCheckpoint;
        }
    }
    pub fn copy(self: Snapshot, a: std.mem.Allocator) !Snapshot {
        const registry = try a.create(speakers.Registry);
        errdefer a.destroy(registry);
        registry.* = self.registry.*;
        var value = self;
        value.registry = registry;
        if (self.shared) |view| value.shared = try view.copy(a) else value.spans = try a.dupe(speakers.Span, self.spans);
        return value;
    }
    pub fn deinit(self: Snapshot, a: std.mem.Allocator) void {
        a.destroy(self.registry);
        if (self.shared) |view| view.deinit(a) else a.free(self.spans);
    }
    /// Concurrent writers may extend the final open turn, but cannot relabel
    /// an accepted prefix or replace a newer/complete checkpoint with an older one.
    pub fn extends(self: Snapshot, old: Snapshot) bool {
        if (self.sample_rate != old.sample_rate or self.next_window < old.next_window or self.covered_samples < old.covered_samples or self.registry.count < old.registry.count) return false;
        if (old.total_samples) |total| if (self.total_samples == null or self.total_samples.? != total) return false;
        if (old.complete and !self.complete) return false;
        if (self.spanCount() < old.spanCount()) return false;
        if (old.spanCount() > 0) {
            const prefix = old.spanCount() - 1;
            if (!std.mem.eql(u8, &self.prefixDigest(prefix), &old.prefixDigest(prefix))) return false;
            const span = old.spanAt(prefix);
            const current = self.spanAt(prefix);
            if (span.start_ns != current.start_ns or span.speaker_index != current.speaker_index or current.end_ns < span.end_ns) return false;
        }
        if (self.next_window == old.next_window) {
            if (self.spanCount() != old.spanCount() or self.registry.count != old.registry.count) return false;
            for (self.registry.sums[0..self.registry.count], old.registry.sums[0..old.registry.count]) |left, right| if (!std.mem.eql(f32, &left, &right)) return false;
        }
        return true;
    }
};
pub fn key(a: std.mem.Allocator, source: [32]u8, version: []const u8, revision: []const u8, config: speakers.Config, sample_rate: u32) ![32]u8 {
    var semantic = config;
    semantic.model = null;
    semantic.model_identity = null;
    const options = try std.json.Stringify.valueAlloc(a, semantic, .{});
    defer a.free(options);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("speaker-checkpoint-v1");
    hash.update(speakers.algorithm);
    hash.update(&source);
    hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, version.len)));
    hash.update(version);
    hash.update(revision);
    hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u32, sample_rate)));
    hash.update(options);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
pub fn encode(a: std.mem.Allocator, identity: [32]u8, value: Snapshot) ![]u8 {
    return encodeParts(a, identity, value, 0);
}
fn encodeParts(a: std.mem.Allocator, identity: [32]u8, value: Snapshot, first: usize) ![]u8 {
    try value.validate();
    return encodeRaw(a, identity, value, first);
}
fn encodeRaw(a: std.mem.Allocator, identity: [32]u8, value: Snapshot, first: usize) ![]u8 {
    const count = value.spanCount() - first;
    const bytes = try a.alloc(u8, 74 + value.registry.count * 512 * 4 + count * 20 + 32);
    @memcpy(bytes[0..8], "AFSP0001");
    @memcpy(bytes[8..40], &identity);
    std.mem.writeInt(u32, bytes[40..44], value.sample_rate, .little);
    std.mem.writeInt(u64, bytes[44..52], value.next_window, .little);
    std.mem.writeInt(u64, bytes[52..60], value.covered_samples, .little);
    std.mem.writeInt(u64, bytes[60..68], value.total_samples orelse std.math.maxInt(u64), .little);
    bytes[68] = @intFromBool(value.complete);
    bytes[69] = @intCast(value.registry.count);
    std.mem.writeInt(u32, bytes[70..74], @intCast(count), .little);
    var at: usize = 74;
    for (value.registry.sums[0..value.registry.count]) |sum| for (sum) |v| {
        std.mem.writeInt(u32, bytes[at..][0..4], @bitCast(v), .little);
        at += 4;
    };
    for (first..value.spanCount()) |i| {
        const span = value.spanAt(i);
        std.mem.writeInt(i64, bytes[at..][0..8], span.start_ns, .little);
        std.mem.writeInt(i64, bytes[at + 8 ..][0..8], span.end_ns, .little);
        std.mem.writeInt(u32, bytes[at + 16 ..][0..4], span.speaker_index orelse std.math.maxInt(u32), .little);
        at += 20;
    }
    std.crypto.hash.sha2.Sha256.hash(bytes[0..at], bytes[at..][0..32], .{});
    return bytes;
}
pub fn decode(a: std.mem.Allocator, identity: [32]u8, bytes: []const u8) !Snapshot {
    return decodeParts(a, identity, bytes, true);
}
fn decodeParts(a: std.mem.Allocator, identity: [32]u8, bytes: []const u8, full: bool) !Snapshot {
    if (bytes.len < 106 or bytes.len > max_encoded_bytes or !std.mem.eql(u8, bytes[0..8], "AFSP0001") or !std.mem.eql(u8, bytes[8..40], &identity) or bytes[68] > 1 or bytes[69] > 64) return error.InvalidSpeakerCheckpoint;
    const count = bytes[69];
    const spans_count = std.mem.readInt(u32, bytes[70..74], .little);
    if (spans_count > speakers.max_spans or bytes.len != 74 + @as(usize, count) * 512 * 4 + @as(usize, spans_count) * 20 + 32) return error.InvalidSpeakerCheckpoint;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes[0 .. bytes.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidSpeakerCheckpoint;
    const registry = try a.create(speakers.Registry);
    errdefer a.destroy(registry);
    registry.* = .{ .count = count };
    const spans = try a.alloc(speakers.Span, spans_count);
    errdefer a.free(spans);
    var at: usize = 74;
    for (registry.sums[0..count]) |*sum| for (sum) |*v| {
        v.* = @bitCast(std.mem.readInt(u32, bytes[at..][0..4], .little));
        at += 4;
    };
    for (spans) |*span| {
        const speaker = std.mem.readInt(u32, bytes[at + 16 ..][0..4], .little);
        span.* = .{ .start_ns = std.mem.readInt(i64, bytes[at..][0..8], .little), .end_ns = std.mem.readInt(i64, bytes[at + 8 ..][0..8], .little), .speaker_index = if (speaker == std.math.maxInt(u32)) null else speaker };
        at += 20;
    }
    const total = std.mem.readInt(u64, bytes[60..68], .little);
    const value = Snapshot{ .registry = registry, .spans = spans, .sample_rate = std.mem.readInt(u32, bytes[40..44], .little), .next_window = std.mem.readInt(u64, bytes[44..52], .little), .covered_samples = std.mem.readInt(u64, bytes[52..60], .little), .total_samples = if (total == std.math.maxInt(u64)) null else total, .complete = bytes[68] == 1 };
    if (full) try value.validate();
    return value;
}
pub const DiskCursor = struct {
    bytes: u64,
    records: u32,
    checksum: [32]u8,
    window: u64,
    span_count: usize,
    tail: ?speakers.Span,
    prefix_hash: [32]u8,
    rate: u32,
    registry_count: usize,
    total: ?u64,
    complete: bool,
};
fn spansHash(spans: []const speakers.Span) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var raw: [20]u8 = undefined;
    for (spans) |span| {
        std.mem.writeInt(i64, raw[0..8], span.start_ns, .little);
        std.mem.writeInt(i64, raw[8..16], span.end_ns, .little);
        std.mem.writeInt(u32, raw[16..20], span.speaker_index orelse std.math.maxInt(u32), .little);
        hash.update(&raw);
    }
    return hash.finalResult();
}
fn cursorFor(value: Snapshot, bytes: u64, records: u32, checksum: [32]u8) DiskCursor {
    return .{ .bytes = bytes, .records = records, .checksum = checksum, .window = value.next_window, .span_count = value.spanCount(), .tail = if (value.spanCount() == 0) null else value.spanAt(value.spanCount() - 1), .prefix_hash = value.prefixDigest(value.spanCount() -| 1), .rate = value.sample_rate, .registry_count = value.registry.count, .total = value.total_samples, .complete = value.complete };
}
pub const Store = struct {
    pub const max_records = 64;
    pub const max_file_bytes = max_encoded_bytes * 8;
    full_reads: std.atomic.Value(u64) = .init(0),
    mutexes: [256]std.Io.Mutex = @splat(.init),
    pub const Context = struct { store: *Store, io: std.Io, models_dir: []const u8 };
    pub const Loaded = struct { snapshot: Snapshot, cursor: DiskCursor };
    pub fn path(buf: []u8, root: []const u8, identity: [32]u8, pending: bool) ![]u8 {
        return std.fmt.bufPrint(buf, "{s}/.antfly-speaker-plans/{s}.{s}", .{ root, std.fmt.bytesToHex(identity, .lower), if (pending) "tmp" else "bin" });
    }
    pub fn load(context: Context, a: std.mem.Allocator, identity: [32]u8) !Snapshot {
        return (try loadWithCursor(context, a, identity)).snapshot;
    }
    pub fn loadWithCursor(context: Context, a: std.mem.Allocator, identity: [32]u8) !Loaded {
        _ = context.store.full_reads.fetchAdd(1, .monotonic);
        var name: [std.fs.max_path_bytes]u8 = undefined;
        const file = try @import("../runtime/file_snapshot.zig").openRegular(context.io, .cwd(), try path(&name, context.models_dir, identity, false), null);
        defer file.close(context.io);
        const stat = try file.stat(context.io);
        if (stat.size > max_file_bytes) return error.InvalidSpeakerCheckpoint;
        const bytes = try a.alloc(u8, @intCast(stat.size));
        defer a.free(bytes);
        if (try file.readPositionalAll(context.io, bytes, 0) != bytes.len) return error.InvalidSpeakerCheckpoint;
        if (bytes.len >= 8 and std.mem.eql(u8, bytes[0..8], "AFSP0001")) {
            const value = try decode(a, identity, bytes);
            return .{ .snapshot = value, .cursor = cursorFor(value, bytes.len, 0, @splat(0)) };
        }
        if (bytes.len < 40 or !std.mem.eql(u8, bytes[0..8], "AFSJ0001") or !std.mem.eql(u8, bytes[8..40], &identity)) return error.InvalidSpeakerCheckpoint;
        var current: ?Snapshot = null;
        errdefer if (current) |value| value.deinit(a);
        var at: usize = 40;
        var records: u32 = 0;
        var checksum: [32]u8 = @splat(0);
        while (at + 88 <= bytes.len and records < max_records) {
            const size = std.mem.readInt(u32, bytes[at..][0..4], .little);
            if (size > max_encoded_bytes or size > bytes.len - at - 88) break;
            const finish = at + 48 + size;
            if (!std.mem.eql(u8, bytes[finish + 32 ..][0..8], "AFSC0001") or !std.mem.eql(u8, bytes[at + 16 ..][0..32], &checksum)) break;
            var actual: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes[at..finish], &actual, .{});
            if (!std.mem.eql(u8, &actual, bytes[finish..][0..32])) break;
            const base = std.mem.readInt(u64, bytes[at + 4 ..][0..8], .little);
            const first = std.mem.readInt(u32, bytes[at + 12 ..][0..4], .little);
            const part = decodeParts(a, identity, bytes[at + 48 .. finish], false) catch |err| {
                if (err == error.OutOfMemory) return err;
                break;
            };
            var consumed = false;
            defer if (!consumed) part.deinit(a);
            var value = part;
            if (current) |old| {
                if (base != old.next_window or first != old.spans.len -| 1 or first + part.spans.len > speakers.max_spans) break;
                const spans = try a.alloc(speakers.Span, first + part.spans.len);
                @memcpy(spans[0..first], old.spans[0..first]);
                @memcpy(spans[first..], part.spans);
                value.spans = spans;
                a.free(part.spans);
                consumed = true;
                value.validate() catch {
                    value.deinit(a);
                    break;
                };
                if (!value.extends(old)) {
                    value.deinit(a);
                    break;
                }
                old.deinit(a);
            } else {
                if (base != 0 or first != 0) break;
                value.validate() catch break;
                consumed = true;
            }
            current = value;
            checksum = actual;
            records += 1;
            at = finish + 40;
        }
        const value = current orelse return error.InvalidSpeakerCheckpoint;
        return .{ .snapshot = value, .cursor = cursorFor(value, at, records, checksum) };
    }
    fn frame(a: std.mem.Allocator, identity: [32]u8, value: Snapshot, prior: ?DiskCursor) ![]u8 {
        const first = if (prior) |cursor| cursor.span_count -| 1 else 0;
        const payload = try encodeParts(a, identity, value, first);
        defer a.free(payload);
        const bytes = try a.alloc(u8, 88 + payload.len);
        std.mem.writeInt(u32, bytes[0..4], @intCast(payload.len), .little);
        std.mem.writeInt(u64, bytes[4..12], if (prior) |cursor| cursor.window else 0, .little);
        std.mem.writeInt(u32, bytes[12..16], @intCast(first), .little);
        @memcpy(bytes[16..48], if (prior) |cursor| &cursor.checksum else &@as([32]u8, @splat(0)));
        @memcpy(bytes[48..][0..payload.len], payload);
        std.crypto.hash.sha2.Sha256.hash(bytes[0 .. 48 + payload.len], bytes[48 + payload.len ..][0..32], .{});
        @memcpy(bytes[bytes.len - 8 ..], "AFSC0001");
        return bytes;
    }
    fn append(context: Context, a: std.mem.Allocator, identity: [32]u8, value: Snapshot, cursor: DiskCursor) !?DiskCursor {
        if (cursor.records == 0 or cursor.records >= max_records or value.next_window <= cursor.window or cursor.complete or value.sample_rate != cursor.rate or value.registry.count < cursor.registry_count or value.spanCount() < cursor.span_count) return null;
        if (cursor.total) |total| if (value.total_samples == null or value.total_samples.? != total) return null;
        if (!std.mem.eql(u8, &cursor.prefix_hash, &value.prefixDigest(cursor.span_count -| 1))) return null;
        if (cursor.tail) |tail| {
            const current = value.spanAt(cursor.span_count - 1);
            if (current.start_ns != tail.start_ns or current.speaker_index != tail.speaker_index or current.end_ns < tail.end_ns) return null;
        }
        var name: [std.fs.max_path_bytes]u8 = undefined;
        const file = std.Io.Dir.cwd().openFile(context.io, try path(&name, context.models_dir, identity, false), .{ .mode = .read_write }) catch return null;
        defer file.close(context.io);
        const stat = try file.stat(context.io);
        if (stat.kind != .file or stat.size != cursor.bytes or stat.size > max_file_bytes or stat.size < 80) return null;
        var header: [40]u8 = undefined;
        var trailer: [40]u8 = undefined;
        if (try file.readPositionalAll(context.io, &header, 0) != 40 or !std.mem.eql(u8, header[0..8], "AFSJ0001") or !std.mem.eql(u8, header[8..40], &identity)) return null;
        if (try file.readPositionalAll(context.io, &trailer, stat.size - 40) != 40 or !std.mem.eql(u8, trailer[0..32], &cursor.checksum) or !std.mem.eql(u8, trailer[32..40], "AFSC0001")) return null;
        const bytes = try frame(a, identity, value, cursor);
        defer a.free(bytes);
        if (bytes.len > max_file_bytes - stat.size) return null;
        try file.writePositionalAll(context.io, bytes, stat.size);
        try file.sync(context.io);
        var checksum: [32]u8 = undefined;
        @memcpy(&checksum, bytes[bytes.len - 40 ..][0..32]);
        return cursorFor(value, stat.size + bytes.len, cursor.records + 1, checksum);
    }
    pub fn save(context: Context, a: std.mem.Allocator, identity: [32]u8, value: Snapshot) !void {
        _ = try saveWithCursor(context, a, identity, value, null);
    }
    pub fn saveWithCursor(context: Context, a: std.mem.Allocator, identity: [32]u8, value: Snapshot, hint: ?DiskCursor) !DiskCursor {
        if (comptime @import("builtin").os.tag == .freestanding) return error.UnsupportedPlatform;
        try value.validate();
        const mutex = &context.store.mutexes[identity[0]];
        try mutex.lock(context.io);
        defer mutex.unlock(context.io);
        var dir: [std.fs.max_path_bytes]u8 = undefined;
        try std.Io.Dir.cwd().createDirPath(context.io, try std.fmt.bufPrint(&dir, "{s}/.antfly-speaker-plans", .{context.models_dir}));
        var lock_buf: [std.fs.max_path_bytes]u8 = undefined;
        const lock = try std.Io.Dir.cwd().createFile(context.io, try std.fmt.bufPrint(&lock_buf, "{s}/.antfly-speaker-plans/{x:0>2}.lock", .{ context.models_dir, identity[0] }), .{ .truncate = false, .lock = .exclusive, .lock_nonblocking = true });
        defer lock.close(context.io);
        if (hint) |cursor| if (try append(context, a, identity, value, cursor)) |saved| return saved;
        if (loadWithCursor(context, a, identity)) |old| {
            defer old.snapshot.deinit(a);
            if (old.snapshot.extends(value)) return old.cursor;
            if (!value.extends(old.snapshot)) return error.InvalidSpeakerCheckpoint;
            if (try append(context, a, identity, value, old.cursor)) |saved| return saved;
        } else |err| {
            if (err == error.OutOfMemory) return err;
        }
        const bytes = try frame(a, identity, value, null);
        defer a.free(bytes);
        var target_buf: [std.fs.max_path_bytes]u8 = undefined;
        var pending_buf: [std.fs.max_path_bytes]u8 = undefined;
        const target = try path(&target_buf, context.models_dir, identity, false);
        const pending = try path(&pending_buf, context.models_dir, identity, true);
        defer std.Io.Dir.cwd().deleteFile(context.io, pending) catch {};
        {
            const file = try std.Io.Dir.cwd().createFile(context.io, pending, .{});
            defer file.close(context.io);
            var header: [40]u8 = undefined;
            @memcpy(header[0..8], "AFSJ0001");
            @memcpy(header[8..40], &identity);
            try file.writeStreamingAll(context.io, &header);
            try file.writeStreamingAll(context.io, bytes);
            try file.sync(context.io);
        }
        try std.Io.Dir.cwd().rename(pending, .cwd(), target, context.io);
        var checksum: [32]u8 = undefined;
        @memcpy(&checksum, bytes[bytes.len - 40 ..][0..32]);
        return cursorFor(value, 40 + bytes.len, 1, checksum);
    }
};

test "model aware media speaker checkpoints own centroids fence semantic recipes and reject corrupt snapshots" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            const registry = try a.create(speakers.Registry);
            defer a.destroy(registry);
            registry.* = .{ .count = 1 };
            registry.sums[0][0] = 1;
            const value = Snapshot{ .registry = registry, .spans = &.{.{ .start_ns = 0, .end_ns = 3 * std.time.ns_per_s, .speaker_index = 0 }}, .sample_rate = 16000, .next_window = 2, .covered_samples = 48000, .total_samples = 80000 };
            const identity = try key(a, @splat(1), "v1", "speaker-r1", .{}, 16000);
            const bytes = try encode(a, identity, value);
            defer a.free(bytes);
            const decoded = try decode(a, identity, bytes);
            defer decoded.deinit(a);
            try std.testing.expectEqualDeep(value.registry, decoded.registry);
            try std.testing.expectEqualDeep(value.spans, decoded.spans);
            const copy = try value.copy(a);
            defer copy.deinit(a);
            registry.sums[0][0] = 0.5;
            try std.testing.expectEqual(@as(f32, 1), copy.registry.sums[0][0]);
            bytes[75] ^= 1;
            try std.testing.expectError(error.InvalidSpeakerCheckpoint, decode(a, identity, bytes));
            bytes[75] ^= 1;
            try std.testing.expectError(error.InvalidSpeakerCheckpoint, decode(a, @splat(2), bytes));
            const alias = try key(a, @splat(1), "v1", "speaker-r1", .{ .model = "alias/model:weights.onnx", .model_identity = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, 16000);
            try std.testing.expectEqualSlices(u8, &identity, &alias);
            const changed = [_][32]u8{ try key(a, @splat(2), "v1", "speaker-r1", .{}, 16000), try key(a, @splat(1), "v2", "speaker-r1", .{}, 16000), try key(a, @splat(1), "v1", "speaker-r2", .{}, 16000), try key(a, @splat(1), "v1", "speaker-r1", .{ .similarity_threshold = 0.7 }, 16000), try key(a, @splat(1), "v1", "speaker-r1", .{}, 11025) };
            for (changed) |other| try std.testing.expect(!std.mem.eql(u8, &identity, &other));
        }
    };
    try Fixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "model aware media speaker checkpoint persistence resumes after restart without regressing accepted turns" {
    const a = std.testing.allocator;
    const registry = try a.create(speakers.Registry);
    defer a.destroy(registry);
    registry.* = .{ .count = 1 };
    registry.sums[0][0] = 1;
    const initial = Snapshot{ .registry = registry, .spans = &.{.{ .start_ns = 0, .end_ns = 1500 * std.time.ns_per_ms, .speaker_index = 0 }}, .sample_rate = 16000, .next_window = 1, .covered_samples = 24000, .total_samples = 48000 };
    const advanced = Snapshot{ .registry = registry, .spans = &.{.{ .start_ns = 0, .end_ns = 3 * std.time.ns_per_s, .speaker_index = 0 }}, .sample_rate = 16000, .next_window = 2, .covered_samples = 48000, .total_samples = 48000, .complete = true };
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const directory = try temp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(directory);
    const identity = try key(a, @splat(1), "v1", "revision", .{}, 16000);
    var store = Store{};
    const context = Store.Context{ .store = &store, .io = std.testing.io, .models_dir = directory };
    try Store.save(context, a, identity, initial);
    try Store.save(context, a, identity, advanced);
    try Store.save(context, a, identity, initial);
    var restarted = Store{};
    const saved = try Store.load(.{ .store = &restarted, .io = std.testing.io, .models_dir = directory }, a, identity);
    defer saved.deinit(a);
    try std.testing.expect(saved.complete);
    try std.testing.expectEqual(@as(u64, 2), saved.next_window);
    try std.testing.expectEqualDeep(registry.*, saved.registry.*);
    try std.testing.expect(!initial.extends(advanced));
    var conflict_registry = registry.*;
    conflict_registry.sums[0][0] = 0.1;
    var conflict = advanced;
    conflict.registry = &conflict_registry;
    try std.testing.expect(!conflict.extends(advanced));
    var file_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const name = try Store.path(&file_buffer, directory, identity, false);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = name, .data = "incomplete crash write" });
    try std.testing.expectError(error.InvalidSpeakerCheckpoint, Store.load(context, a, identity));
    // An invalid optional checkpoint is replaceable by deterministic replay.
    try Store.save(context, a, identity, initial);
    const replayed = try Store.load(context, a, identity);
    defer replayed.deinit(a);
    try std.testing.expectEqual(@as(u64, 1), replayed.next_window);
}

test "model aware media speaker journal appends only changed turns and recovers torn commits" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const directory = try temp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(directory);
    var store: Store = .{};
    const context = Store.Context{ .store = &store, .io = io, .models_dir = directory };
    const identity: [32]u8 = @splat(42);
    const registry = try a.create(speakers.Registry);
    defer a.destroy(registry);
    registry.* = .{ .count = 1 };
    registry.sums[0][0] = 1;
    var spans: [102]speakers.Span = undefined;
    for (&spans, 0..) |*span, i| span.* = .{ .start_ns = @intCast(i * 1500 * std.time.ns_per_ms), .end_ns = @intCast((i + 1) * 1500 * std.time.ns_per_ms), .speaker_index = 0 };
    var value = Snapshot{ .registry = registry, .spans = spans[0..100], .sample_rate = 16000, .next_window = 100, .covered_samples = 100 * 24000 };
    const first = try Store.saveWithCursor(context, a, identity, value, null);
    var file_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const name = try Store.path(&file_buffer, directory, identity, false);
    const prefix = try std.Io.Dir.cwd().readFileAlloc(io, name, a, .limited(Store.max_file_bytes));
    defer a.free(prefix);
    value.spans = spans[0..101];
    value.next_window = 101;
    value.covered_samples = 101 * 24000;
    const second = try Store.saveWithCursor(context, a, identity, value, first);
    try std.testing.expectEqual(@as(u32, 2), second.records);
    try std.testing.expectEqual(@as(u64, 88 + 106 + 512 * 4 + 2 * 20), second.bytes - first.bytes);
    const appended = try std.Io.Dir.cwd().readFileAlloc(io, name, a, .limited(Store.max_file_bytes));
    defer a.free(appended);
    try std.testing.expectEqualSlices(u8, prefix, appended[0..prefix.len]);
    // Truncate each portion of the last commit; every recovery keeps the
    // previously committed registry/turns and never accepts a partial suffix.
    for ([_]usize{ 1, 32, 41, 100, @intCast(second.bytes - first.bytes - 1) }) |missing| {
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = name, .data = appended[0 .. appended.len - missing] });
        const loaded = try Store.loadWithCursor(context, a, identity);
        defer loaded.snapshot.deinit(a);
        try std.testing.expectEqual(first.window, loaded.cursor.window);
        try std.testing.expectEqual(first.bytes, loaded.cursor.bytes);
    }
    // A stale hint sees the trailing bytes and falls back to verified recovery
    // plus atomic compaction, instead of appending behind a torn frame.
    const repaired = try Store.saveWithCursor(context, a, identity, value, first);
    try std.testing.expectEqual(@as(u32, 1), repaired.records);
    const loaded = try Store.load(context, a, identity);
    defer loaded.deinit(a);
    try std.testing.expectEqual(@as(u64, 101), loaded.next_window);
    try std.testing.expectEqualSlices(speakers.Span, value.spans, loaded.spans);
    const stale = try Store.saveWithCursor(context, a, identity, value, second);
    try std.testing.expectEqual(repaired.window, stale.window);
    // Bound recovery work by compacting long append chains.
    var cursor = repaired;
    for (102..172) |window| {
        spans[101].end_ns = @intCast(window * 1500 * std.time.ns_per_ms);
        value.spans = &spans;
        value.next_window = window;
        value.covered_samples = window * 24000;
        cursor = try Store.saveWithCursor(context, a, identity, value, cursor);
        try std.testing.expect(cursor.records <= Store.max_records);
    }
    const compacted = try Store.loadWithCursor(context, a, identity);
    defer compacted.snapshot.deinit(a);
    try std.testing.expectEqual(@as(u64, 171), compacted.snapshot.next_window);
    try std.testing.expectEqual(cursor.records, compacted.cursor.records);
}

test "model aware media shared speaker snapshots preserve checkpoint bytes and journal prefixes" {
    const a = std.testing.allocator;
    const registry = try a.create(speakers.Registry);
    defer a.destroy(registry);
    registry.* = .{ .count = 2 };
    registry.sums[0][0] = 1;
    registry.sums[1][1] = 1;
    var turns = timeline.Timeline{ .backing = a };
    defer turns.deinit(a);
    for (0..512) |i| try turns.append(a, .{ .start_ns = @intCast(i * 1500 * std.time.ns_per_ms), .end_ns = @intCast((i + 1) * 1500 * std.time.ns_per_ms), .speaker_index = @intCast(i % 2) });
    const value = Snapshot{ .registry = registry, .shared = turns.view(), .sample_rate = 16000, .next_window = 512, .covered_samples = 512 * 24000 };
    const saved = try value.copy(a);
    defer saved.deinit(a);
    const identity: [32]u8 = @splat(71);
    const bytes = try encode(a, identity, value);
    defer a.free(bytes);
    const decoded = try decode(a, identity, bytes);
    defer decoded.deinit(a);
    const flat_bytes = try encode(a, identity, decoded);
    defer a.free(flat_bytes);
    try std.testing.expectEqualSlices(u8, bytes, flat_bytes);
    try std.testing.expect(value.extends(decoded) and decoded.extends(value));
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const directory = try temp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(directory);
    var store: Store = .{};
    const context = Store.Context{ .store = &store, .io = std.testing.io, .models_dir = directory };
    var cursor = try Store.saveWithCursor(context, a, identity, value, null);
    try turns.append(a, .{ .start_ns = 512 * 1500 * std.time.ns_per_ms, .end_ns = 513 * 1500 * std.time.ns_per_ms, .speaker_index = 1 });
    var advanced = value;
    advanced.shared = turns.view();
    advanced.next_window = 513;
    advanced.covered_samples = 513 * 24000;
    try std.testing.expect(advanced.extends(saved));
    try std.testing.expectEqual(@as(i64, 512 * 1500 * std.time.ns_per_ms), saved.spanAt(511).end_ns);
    cursor = try Store.saveWithCursor(context, a, identity, advanced, cursor);
    try std.testing.expectEqual(@as(u32, 2), cursor.records);
    try turns.append(a, .{ .start_ns = 513 * 1500 * std.time.ns_per_ms, .end_ns = 514 * 1500 * std.time.ns_per_ms, .speaker_index = 0 });
    advanced.shared = turns.view();
    advanced.next_window = 514;
    advanced.covered_samples = 514 * 24000;
    cursor = try Store.saveWithCursor(context, a, identity, advanced, cursor);
    const recovered = try Store.load(context, a, identity);
    defer recovered.deinit(a);
    try std.testing.expect(recovered.extends(advanced) and advanced.extends(recovered));
    try std.testing.expectEqual(@as(u32, 3), cursor.records);
}
