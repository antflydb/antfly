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

//! Durable checkpoint/segment namespace. A sealed segment's NEXT link is
//! published before its successor can acknowledge an edit. Both the old and
//! new checkpoint therefore reach the same advancing suffix during rollover.
const std = @import("std");
const storage_io = @import("storage_io.zig");
const manifest = @import("../lsm/manifest.zig");
const Crc32 = @import("antfly_hash").Crc32;
const Allocator = std.mem.Allocator;
const Storage = storage_io.Storage;

pub const magic = "ALSMSET1";
const segment_magic = "ALSMSEG1";
const link_magic = "ALSMNXT1";
pub const header_len = 28;
pub const max_segments = 64;

pub const Descriptor = struct {
    checkpoint: u64,
    first_segment: u64,
    sequence: u64,

    pub fn encode(self: @This()) [36]u8 {
        var bytes: [36]u8 = undefined;
        @memcpy(bytes[0..8], magic);
        std.mem.writeInt(u64, bytes[8..16], self.checkpoint, .little);
        std.mem.writeInt(u64, bytes[16..24], self.first_segment, .little);
        std.mem.writeInt(u64, bytes[24..32], self.sequence, .little);
        std.mem.writeInt(u32, bytes[32..36], Crc32.hash(bytes[0..32]), .little);
        return bytes;
    }

    pub fn decode(raw: []const u8) !@This() {
        if (raw.len != 36 or !std.mem.eql(u8, raw[0..8], magic) or Crc32.hash(raw[0..32]) != std.mem.readInt(u32, raw[32..36], .little)) return error.InvalidManifest;
        const out = Descriptor{
            .checkpoint = std.mem.readInt(u64, raw[8..16], .little),
            .first_segment = std.mem.readInt(u64, raw[16..24], .little),
            .sequence = std.mem.readInt(u64, raw[24..32], .little),
        };
        if (out.checkpoint == 0 or out.first_segment == 0) return error.InvalidManifest;
        return out;
    }
};

pub const Kind = enum { checkpoint, journal, next };

pub fn identify(path: []const u8) ?struct { id: u64, kind: Kind } {
    const name = std.fs.path.basename(path);
    if (!std.mem.startsWith(u8, name, "manifest-")) return null;
    const dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return null;
    if (dot <= 9) return null;
    const id = std.fmt.parseInt(u64, name[9..dot], 10) catch return null;
    const kind = std.meta.stringToEnum(Kind, name[dot + 1 ..]) orelse return null;
    return .{ .id = id, .kind = kind };
}

pub fn pathAlloc(allocator: Allocator, root: []const u8, id: u64, kind: Kind) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}/manifest-{d}.{s}", .{ root, id, @tagName(kind) });
}

/// Only atomic-writer siblings of owned manifest names are inventory garbage.
/// Callers must hold the native writer lease with no publication in flight.
pub fn isTemporaryFile(name: []const u8) bool {
    const marker = std.mem.lastIndexOf(u8, name, ".tmp-") orelse return false;
    const nonce = name[marker + 5 ..];
    if (nonce.len == 0) return false;
    for (nonce) |byte| if (!std.ascii.isDigit(byte)) return false;
    _ = std.fmt.parseUnsigned(u64, nonce, 10) catch return false;
    const target = name[0..marker];
    return std.mem.eql(u8, target, "manifest.bin") or identify(target) != null;
}

fn header(tag: *const [8]u8, id: u64, value: u64) [header_len]u8 {
    var out: [header_len]u8 = undefined;
    @memcpy(out[0..8], tag);
    std.mem.writeInt(u64, out[8..16], id, .little);
    std.mem.writeInt(u64, out[16..24], value, .little);
    std.mem.writeInt(u32, out[24..28], Crc32.hash(out[0..24]), .little);
    return out;
}

fn parseHeader(raw: []const u8, tag: *const [8]u8, id: u64) !u64 {
    if (raw.len < header_len or !std.mem.eql(u8, raw[0..8], tag) or std.mem.readInt(u64, raw[8..16], .little) != id or Crc32.hash(raw[0..24]) != std.mem.readInt(u32, raw[24..28], .little)) return error.InvalidManifest;
    return std.mem.readInt(u64, raw[16..24], .little);
}

pub fn nextSegment(allocator: Allocator, storage: Storage, root: []const u8, id: u64) !?u64 {
    const path = try pathAlloc(allocator, root, id, .next);
    defer allocator.free(path);
    const raw = storage.readFileAlloc(allocator, path, header_len + 1) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer allocator.free(raw);
    if (raw.len != header_len) return error.InvalidManifest;
    const next = try parseHeader(raw, link_magic, id);
    if (next <= id) return error.InvalidManifest;
    return next;
}

pub fn replace(allocator: Allocator, storage: Storage, path: []const u8, bytes: []const u8) !void {
    var sink = try storage.beginAtomicWrite(allocator, path);
    var owned = true;
    errdefer if (owned) sink.abort();
    try sink.appendSlice(bytes);
    owned = false;
    try sink.finish();
}

pub fn createSegment(allocator: Allocator, storage: Storage, root: []const u8, id: u64, first_sequence: u64) !void {
    const path = try pathAlloc(allocator, root, id, .journal);
    defer allocator.free(path);
    try replace(allocator, storage, path, &header(segment_magic, id, first_sequence));
}

/// The caller serializes this handoff with appends. A failed or ambiguous
/// handoff must fence further appends until recovery or a new set publication.
pub fn rotate(allocator: Allocator, storage: Storage, root: []const u8, previous: u64, next: u64, first_sequence: u64) !void {
    if (next <= previous) return error.InvalidManifest;
    try createSegment(allocator, storage, root, next, first_sequence);
    const path = try pathAlloc(allocator, root, previous, .next);
    defer allocator.free(path);
    try replace(allocator, storage, path, &header(link_magic, previous, next));
}

pub fn publish(allocator: Allocator, storage: Storage, root: []const u8, descriptor: Descriptor) !void {
    const path = try std.fs.path.join(allocator, &.{ root, "manifest.bin" });
    defer allocator.free(path);
    try replace(allocator, storage, path, &descriptor.encode());
}

/// One pass over pinned metadata, with fixed scratch independent of SST count
/// or key length. The framing header is patched before the atomic sink finishes.
pub fn writeCheckpoint(allocator: Allocator, storage: Storage, root: []const u8, id: u64, sequence: u64, next_run_id: u64, source: anytype, limit: usize) !usize {
    const path = try pathAlloc(allocator, root, id, .checkpoint);
    defer allocator.free(path);
    var sink = try storage.beginAtomicWrite(allocator, path);
    var owned = true;
    errdefer if (owned) sink.abort();
    const buffer = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(buffer);
    const Writer = struct {
        sink: *storage_io.AtomicWriteSink,
        buffer: []u8,
        used: usize = 0,
        total: usize = 0,
        limit: usize,
        inner: Crc32 = Crc32.init(),
        outer: Crc32 = Crc32.init(),

        fn append(self: *@This(), bytes: []const u8) !void {
            if (bytes.len > self.limit -| self.total) return error.FileTooBig;
            self.total += bytes.len;
            var remaining = bytes;
            while (remaining.len != 0) {
                const len = @min(remaining.len, self.buffer.len - self.used);
                @memcpy(self.buffer[self.used..][0..len], remaining[0..len]);
                self.used += len;
                remaining = remaining[len..];
                if (self.used == self.buffer.len) try self.flush();
            }
        }
        fn flush(self: *@This()) !void {
            try self.sink.appendSlice(self.buffer[0..self.used]);
            self.used = 0;
        }
        fn body(self: *@This(), bytes: []const u8) !void {
            self.outer.update(bytes);
            try self.append(bytes);
        }
        fn logical(self: *@This(), bytes: []const u8) !void {
            self.inner.update(bytes);
            try self.body(bytes);
        }
    };
    var writer = Writer{ .sink = &sink, .buffer = buffer, .limit = limit };
    try writer.append(&(@as([32]u8, @splat(0))));
    try writer.body(&(@as([8]u8, @splat(0))));
    var inner_header: [28]u8 = undefined;
    @memcpy(inner_header[0..8], manifest.magic);
    std.mem.writeInt(u32, inner_header[8..12], manifest.version, .little);
    std.mem.writeInt(u64, inner_header[12..20], next_run_id, .little);
    std.mem.writeInt(u32, inner_header[20..24], @intCast(source.runCount()), .little);
    std.mem.writeInt(u32, inner_header[24..28], @intCast(source.obsoleteCount()), .little);
    try writer.logical(&inner_header);
    var runs: usize = 0;
    while (source.nextRun()) |run| {
        runs += 1;
        try writer.logical(&manifest.runHeader(run));
        try writer.logical(run.path);
        if (run.smallest_namespace_name) |name| try writer.logical(name);
        try writer.logical(run.smallest_key);
        if (run.largest_namespace_name) |name| try writer.logical(name);
        try writer.logical(run.largest_key);
    }
    var paths: usize = 0;
    while (source.nextObsolete()) |obsolete| {
        paths += 1;
        var path_header: [12]u8 = undefined;
        std.mem.writeInt(u64, path_header[0..8], obsolete.delete_after_ns, .little);
        std.mem.writeInt(u32, path_header[8..12], @intCast(obsolete.path.len), .little);
        try writer.logical(&path_header);
        try writer.logical(obsolete.path);
    }
    if (runs != source.runCount() or paths != source.obsoleteCount()) return error.InvalidManifest;
    var checksum: [4]u8 = undefined;
    std.mem.writeInt(u32, &checksum, writer.inner.final(), .little);
    try writer.body(&checksum);
    const body_len = writer.total - 32;
    std.mem.writeInt(u32, &checksum, writer.outer.final(), .little);
    try writer.append(&checksum);
    try writer.flush();
    var frame_header: [32]u8 = undefined;
    @memcpy(frame_header[0..8], manifest.journal_magic);
    std.mem.writeInt(u64, frame_header[8..16], body_len, .little);
    std.mem.writeInt(u64, frame_header[16..24], sequence, .little);
    std.mem.writeInt(u32, frame_header[24..28], 1, .little);
    std.mem.writeInt(u32, frame_header[28..32], Crc32.hash(frame_header[8..28]), .little);
    try sink.writeAt(0, &frame_header);
    owned = false;
    try sink.finish();
    return writer.total;
}

pub const Loaded = struct {
    bytes: []u8,
    descriptor: Descriptor,
    active_segment: u64,
    segments: usize,
};

/// Reconstructs the codec stream before mounting any SST. An incomplete frame
/// is permitted only in the last, unsealed segment. All links and sequence
/// boundaries are validated; orphan files are never adopted by listing.
pub fn load(allocator: Allocator, storage: Storage, root: []const u8, descriptor: Descriptor, limit: usize) !Loaded {
    const checkpoint_path = try pathAlloc(allocator, root, descriptor.checkpoint, .checkpoint);
    defer allocator.free(checkpoint_path);
    const checkpoint = try storage.readFileAlloc(allocator, checkpoint_path, limit +| 1);
    defer allocator.free(checkpoint);
    if (checkpoint.len > limit) return error.FileTooBig;
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    errdefer bytes.deinit(allocator);
    try bytes.appendSlice(allocator, checkpoint);
    if (checkpoint.len < 36 or !std.mem.startsWith(u8, checkpoint, manifest.journal_magic) or std.mem.readInt(u64, checkpoint[16..24], .little) != descriptor.sequence or std.mem.readInt(u32, checkpoint[24..28], .little) != 1 or Crc32.hash(checkpoint[8..28]) != std.mem.readInt(u32, checkpoint[28..32], .little)) return error.InvalidManifest;
    if (std.mem.readInt(u64, checkpoint[8..16], .little) != checkpoint.len - 36 or Crc32.hash(checkpoint[32 .. checkpoint.len - 4]) != std.mem.readInt(u32, checkpoint[checkpoint.len - 4 ..][0..4], .little)) return error.InvalidManifest;
    var expected = std.math.add(u64, descriptor.sequence, 1) catch return error.InvalidManifest;
    var id = descriptor.first_segment;
    var count: usize = 0;
    while (count < max_segments) {
        count += 1;
        const path = try pathAlloc(allocator, root, id, .journal);
        defer allocator.free(path);
        const link_path = try pathAlloc(allocator, root, id, .next);
        defer allocator.free(link_path);
        const link = storage.readFileAlloc(allocator, link_path, header_len + 1) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        defer if (link) |owned| allocator.free(owned);
        // Read NEXT first: once it exists this segment is immutable. Reading
        // the bytes first could pair an in-progress append with a later seal.
        const raw = try storage.readFileAlloc(allocator, path, (limit -| bytes.items.len) +| (header_len + 1));
        defer allocator.free(raw);
        if (try parseHeader(raw, segment_magic, id) != expected) return error.InvalidManifest;
        var offset: usize = header_len;
        while (offset < raw.len) {
            if (raw.len - offset < 24) break;
            const frame = raw[offset..];
            if (Crc32.hash(frame[0..20]) != std.mem.readInt(u32, frame[20..24], .little) or std.mem.readInt(u64, frame[8..16], .little) != expected or std.mem.readInt(u32, frame[16..20], .little) != 0) return error.InvalidManifest;
            const len = std.mem.readInt(u64, frame[0..8], .little);
            if (len > std.math.maxInt(u32)) return error.InvalidManifest;
            if (len > frame.len - 24 or frame.len - 24 - @as(usize, @intCast(len)) < 4) break;
            offset += 28 + @as(usize, @intCast(len));
            expected = std.math.add(u64, expected, 1) catch return error.InvalidManifest;
        }
        if (link != null and offset != raw.len) return error.InvalidManifest;
        if (raw.len - header_len > limit -| bytes.items.len) return error.FileTooBig;
        try bytes.appendSlice(allocator, raw[header_len..]);
        if (link) |next_link| {
            if (next_link.len != header_len) return error.InvalidManifest;
            const next = try parseHeader(next_link, link_magic, id);
            if (next <= id) return error.InvalidManifest;
            id = next;
        } else return .{ .bytes = try bytes.toOwnedSlice(allocator), .descriptor = descriptor, .active_segment = id, .segments = count };
    }
    return error.ManifestJournalBacklogExceeded;
}

test "manifest set checkpoint handoff preserves the advancing suffix from either descriptor" {
    const allocator = std.testing.allocator;
    var memory = storage_io.MemoryStorage.init(allocator);
    defer memory.deinit();
    const storage = memory.storage();
    const root = "/manifest-set";
    try storage.createDirPath(root);
    const old = Descriptor{ .checkpoint = 1, .first_segment = 1, .sequence = 0 };
    const base = try manifest.encodeJournalFrameAlloc(allocator, 0, true, &.{}, &.{}, .{ .next_run_id = 2, .runs = &.{} });
    defer allocator.free(base);
    const path = try pathAlloc(allocator, root, 1, .checkpoint);
    defer allocator.free(path);
    try replace(allocator, storage, path, base);
    try createSegment(allocator, storage, root, 1, 1);
    try publish(allocator, storage, root, old);
    const first_edit = try manifest.encodeJournalFrameAlloc(allocator, 1, false, &.{}, &.{}, .{ .next_run_id = 3, .runs = &.{} });
    defer allocator.free(first_edit);
    const segment = try pathAlloc(allocator, root, 1, .journal);
    defer allocator.free(segment);
    try storage.appendFileAbsolute(allocator, segment, first_edit, true);
    try rotate(allocator, storage, root, 1, 2, 2);
    const checkpoint = try manifest.encodeJournalFrameAlloc(allocator, 1, true, &.{}, &.{}, .{ .next_run_id = 3, .runs = &.{} });
    defer allocator.free(checkpoint);
    const new_path = try pathAlloc(allocator, root, 2, .checkpoint);
    defer allocator.free(new_path);
    const Source = struct {
        pub fn runCount(_: *@This()) usize {
            return 0;
        }
        pub fn obsoleteCount(_: *@This()) usize {
            return 0;
        }
        pub fn nextRun(_: *@This()) ?manifest.RunMeta {
            return null;
        }
        pub fn nextObsolete(_: *@This()) ?manifest.ObsoletePathMeta {
            return null;
        }
    };
    var source: Source = .{};
    try std.testing.expectEqual(checkpoint.len, try writeCheckpoint(allocator, storage, root, 2, 1, 3, &source, 4096));
    const streamed = try storage.readFileAlloc(allocator, new_path, 4096);
    defer allocator.free(streamed);
    try std.testing.expectEqualSlices(u8, checkpoint, streamed);
    const tail = try manifest.encodeJournalFrameAlloc(allocator, 2, false, &.{}, &.{}, .{ .next_run_id = 4, .runs = &.{} });
    defer allocator.free(tail);
    const active_path = try pathAlloc(allocator, root, 2, .journal);
    defer allocator.free(active_path);
    try storage.appendFileAbsolute(allocator, active_path, tail, true);
    const current = Descriptor{ .checkpoint = 2, .first_segment = 2, .sequence = 1 };
    for ([_]Descriptor{ old, current }) |descriptor| {
        const loaded = try load(allocator, storage, root, descriptor, 4096);
        defer allocator.free(loaded.bytes);
        var decoded = try manifest.decodeAlloc(allocator, loaded.bytes);
        defer decoded.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 4), decoded.next_run_id);
        try std.testing.expectEqual(@as(u64, 2), loaded.active_segment);
        var replayed = try @import("manifest_replay.zig").load(allocator, storage, root, descriptor, 4096);
        defer replayed.deinit(allocator);
        try std.testing.expectEqual(decoded.next_run_id, replayed.next_run_id);
        try std.testing.expectEqual(loaded.active_segment, replayed.active_segment);
    }
    try publish(allocator, storage, root, current);
    try std.testing.expectEqualDeep(current, try Descriptor.decode(&current.encode()));
    // A torn tail is permitted only while its segment is active. Once sealed,
    // recovery must not skip it and adopt edits from a successor.
    try storage.appendFileAbsolute(allocator, active_path, tail[0..7], true);
    const torn = try load(allocator, storage, root, current, 4096);
    defer allocator.free(torn.bytes);
    var decoded_torn = try manifest.decodeAlloc(allocator, torn.bytes);
    defer decoded_torn.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 4), decoded_torn.next_run_id);
    var replayed_torn = try @import("manifest_replay.zig").load(allocator, storage, root, current, 4096);
    defer replayed_torn.deinit(allocator);
    try std.testing.expectEqual(decoded_torn.next_run_id, replayed_torn.next_run_id);
    try rotate(allocator, storage, root, 2, 3, 3);
    try std.testing.expectError(error.InvalidManifest, load(allocator, storage, root, current, 4096));
    try std.testing.expectError(error.InvalidManifest, @import("manifest_replay.zig").load(allocator, storage, root, current, 4096));
}
