// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! HA-only materialized participant PREPARE. Row programs/providers never enter
//! this frame: it owns exact private intent/outbox/owner-custody postimages.
//! Receipt issuance still requires the real native publisher to flush its
//! existing durable outbox. This codec grants no user mutation authority.
const std = @import("std");
const A = std.mem.Allocator;
const wire = @import("../../system_catalog/logical_replication_publisher.zig");
const changes = @import("../publication_prepare_sink.zig");
pub const magic = "ALPP\x01\x00\x00\x00";
pub const max_metadata_bytes: usize = 32 * 1024;
const prefix_bytes: usize = 16;
const checksum_bytes: usize = 32;
const source_apply = @import("../../api/replication_source_apply.zig");
const External = @import("../transaction_decision_owner.zig").External;
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const Position = @import("receipt_position.zig").Position;
pub const Subject = union(enum) {
    capture: wire.CaptureRef,
    source: source_apply.ApplyReceipt,
    both: struct { capture: wire.CaptureRef, source: source_apply.ApplyReceipt },
    pub fn captureReference(self: @This()) ?wire.CaptureRef {
        return switch (self) {
            .capture => |value| value,
            .both => |value| value.capture,
            .source => null,
        };
    }
    pub fn sourceReceipt(self: @This()) ?source_apply.ApplyReceipt {
        return switch (self) {
            .source => |value| value,
            .both => |value| value.source,
            .capture => null,
        };
    }
    pub fn position(self: @This()) Position {
        return if (self.captureReference()) |value| value.position else self.sourceReceipt().?.position;
    }
    pub fn namespace(self: @This()) Namespace {
        return if (self.captureReference()) |value| value.namespace else self.sourceReceipt().?.permit.namespace;
    }
    pub fn physicalRoot(self: @This()) u128 {
        return if (self.captureReference()) |value| value.physical_root_incarnation else self.sourceReceipt().?.image.physical_root_incarnation;
    }
    pub fn owner(self: @This()) External {
        return if (self.captureReference()) |value| value.owner else self.sourceReceipt().?.permit.decision_owner;
    }
    pub fn transactionId(self: @This()) [16]u8 {
        return if (self.captureReference()) |value| value.transaction_id else self.sourceReceipt().?.permit.semantic.transaction_id;
    }
    pub fn member(self: @This()) u16 {
        return if (self.captureReference()) |value| value.participant else self.sourceReceipt().?.permit.member;
    }
    pub fn validate(self: @This()) !void {
        if (self.captureReference()) |value| try value.validate();
        if (self.sourceReceipt()) |value| {
            try value.validate();
            if (value.stage != .prepared) return error.InvalidLogicalOutbox;
        }
        if (self == .both) {
            const left = self.both.capture;
            const right = self.both.source;
            if (!left.owner.eql(right.permit.decision_owner) or !std.mem.eql(u8, &left.transaction_id, &right.permit.semantic.transaction_id) or
                left.participant != right.permit.member or left.group_id != right.permit.owner_group_id or !left.namespace.eql(right.permit.namespace) or
                left.physical_root_incarnation != right.image.physical_root_incarnation or !std.meta.eql(left.position, right.position) or
                !std.mem.eql(u8, &left.pregrant_body_digest, &right.permit.pregrant_body_digest)) return error.InvalidLogicalOutbox;
        }
    }
};
pub const Metadata = struct {
    subject: Subject,
    seed: ?@import("native_standby_seed_contract.zig").Active = null,
    previous_sequence: u64,
    pub fn validate(self: @This()) !void {
        try self.subject.validate();
        if (self.seed) |seed| {
            try seed.validate();
            if (seed.pending.physical_root_incarnation != self.subject.physicalRoot() or !seed.pending.namespace.eql(self.subject.namespace()) or seed.pending.source_sequence > self.previous_sequence) return error.InvalidNativeSourceSeed;
        }
        const actual = self.subject.position();
        if (actual != .native or actual.native.sequence == 0 or self.previous_sequence == std.math.maxInt(u64) or self.previous_sequence + 1 != actual.native.sequence) return error.InvalidLogicalOutbox;
    }
};

pub fn isFrame(bytes: []const u8) bool {
    return bytes.len >= magic.len and std.mem.eql(u8, bytes[0..magic.len], magic);
}
pub const Frame = struct {
    metadata: Metadata,
    writes: []const changes.Change,
    fn validateEntries(entries: []const changes.Change) !void {
        if (entries.len == 0 or entries.len > std.math.maxInt(u32)) return error.InvalidLogicalOutbox;
        var previous: ?[]const u8 = null;
        for (entries) |entry| {
            if (!std.mem.startsWith(u8, entry.key, "\x00\x00") or entry.key.len > std.math.maxInt(u32)) return error.InvalidLogicalOutbox;
            if (previous) |old| if (std.mem.order(u8, old, entry.key) != .lt) return error.InvalidLogicalOutbox;
            previous = entry.key;
        }
    }
    pub fn encode(self: @This(), a: A, maximum: usize) ![]u8 {
        try self.metadata.validate();
        try validateEntries(self.writes);
        const metadata = try std.json.Stringify.valueAlloc(a, self.metadata, .{});
        defer a.free(metadata);
        if (metadata.len > max_metadata_bytes) return error.InvalidLogicalOutbox;
        var total = try std.math.add(usize, prefix_bytes + checksum_bytes, metadata.len);
        for (self.writes) |entry| {
            total = try std.math.add(usize, total, 12 + entry.key.len);
            total = try std.math.add(usize, total, if (entry.value) |value| value.len else 0);
        }
        if (total > maximum) return error.TransactionTooLarge;
        const bytes = try a.alloc(u8, total);
        errdefer a.free(bytes);
        @memcpy(bytes[0..8], magic);
        std.mem.writeInt(u32, bytes[8..12], @intCast(metadata.len), .little);
        std.mem.writeInt(u32, bytes[12..16], @intCast(self.writes.len), .little);
        @memcpy(bytes[16..][0..metadata.len], metadata);
        var offset: usize = 16 + metadata.len;
        for (self.writes) |entry| {
            std.mem.writeInt(u32, bytes[offset..][0..4], @intCast(entry.key.len), .little);
            std.mem.writeInt(u64, bytes[offset + 4 ..][0..8], if (entry.value) |value| @intCast(value.len) else std.math.maxInt(u64), .little);
            offset += 12;
            @memcpy(bytes[offset..][0..entry.key.len], entry.key);
            offset += entry.key.len;
            if (entry.value) |value| {
                @memcpy(bytes[offset..][0..value.len], value);
                offset += value.len;
            }
        }
        std.debug.assert(offset == bytes.len - checksum_bytes);
        std.crypto.hash.Blake3.hash(bytes[0..offset], bytes[offset..][0..checksum_bytes], .{});
        return bytes;
    }
    /// Only metadata and the descriptor array allocate. Key/value bodies borrow
    /// the immutable input record until ordered replay returns.
    pub fn decode(a: A, bytes: []const u8, maximum: usize) !Frame {
        if (!isFrame(bytes) or bytes.len < prefix_bytes + checksum_bytes or bytes.len > maximum) return error.InvalidLogicalOutbox;
        const end = bytes.len - checksum_bytes;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..end], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[end..])) return error.InvalidLogicalOutbox;
        const metadata_len: usize = std.mem.readInt(u32, bytes[8..12], .little);
        const count: usize = std.mem.readInt(u32, bytes[12..16], .little);
        if (metadata_len > max_metadata_bytes or metadata_len > end - 16 or count == 0 or count > (end - 16 - metadata_len) / 14) return error.InvalidLogicalOutbox;
        const metadata = try std.json.parseFromSliceLeaky(Metadata, a, bytes[16..][0..metadata_len], .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        try metadata.validate();
        const entries = try a.alloc(changes.Change, count);
        var offset: usize = 16 + metadata_len;
        for (entries) |*entry| {
            if (offset > end or end - offset < 12) return error.InvalidLogicalOutbox;
            const key_len: usize = std.mem.readInt(u32, bytes[offset..][0..4], .little);
            const value_len = std.mem.readInt(u64, bytes[offset + 4 ..][0..8], .little);
            offset += 12;
            if (key_len > end - offset) return error.InvalidLogicalOutbox;
            const key = bytes[offset..][0..key_len];
            offset += key_len;
            const value: ?[]const u8 = if (value_len == std.math.maxInt(u64)) null else blk: {
                if (value_len > end - offset) return error.InvalidLogicalOutbox;
                const selected = bytes[offset..][0..@intCast(value_len)];
                offset += @intCast(value_len);
                break :blk selected;
            };
            entry.* = .{ .key = key, .value = value };
        }
        if (offset != end) return error.InvalidLogicalOutbox;
        try validateEntries(entries);
        return .{ .metadata = metadata, .writes = entries };
    }
};

test "publication materialized prepare frame borrows binary postimages and rejects tampering and wrong source cut" {
    const a = std.testing.allocator;
    const ns: Namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 };
    const capture: wire.CaptureRef = .{
        .owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 },
        .transaction_id = @splat(4),
        .participant = 0,
        .group_id = 5,
        .namespace = ns,
        .physical_root_incarnation = 6,
        .position = .{ .native = .{ .namespace = ns, .sequence = 11 } },
        .roster_digest = @splat(12),
        .guard_digest = @splat(13),
        .pregrant_body_digest = @splat(14),
        .changes_digest = @splat(15),
        .capacity_digest = @splat(16),
        .before_digest = @splat(17),
        .change_count = 0,
        .encoded_bytes = 100,
    };
    const frame: Frame = .{
        .metadata = .{ .subject = .{ .capture = capture }, .previous_sequence = 10 },
        .writes = &.{ .{ .key = "\x00\x00a\xff", .value = "\x00\xff\xc0" }, .{ .key = "\x00\x00z", .value = null } },
    };
    const encoded = try frame.encode(a, 64 * 1024);
    defer a.free(encoded);
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const decoded = try Frame.decode(arena.allocator(), encoded, encoded.len);
    try std.testing.expectEqualDeep(frame.metadata, decoded.metadata);
    try std.testing.expectEqualDeep(frame.writes, decoded.writes);
    // Descriptor/metadata allocate; every opaque value remains borrowed from
    // the one immutable journal record through ordered application return.
    const body = decoded.writes[0].value.?;
    try std.testing.expect(@intFromPtr(body.ptr) >= @intFromPtr(encoded.ptr));
    try std.testing.expect(@intFromPtr(body.ptr) + body.len <= @intFromPtr(encoded.ptr) + encoded.len);
    try std.testing.expectError(error.TransactionTooLarge, frame.encode(a, encoded.len - 1));
    try std.testing.expectError(error.InvalidLogicalOutbox, Frame.decode(arena.allocator(), encoded, encoded.len - 1));
    var wrong = frame;
    wrong.metadata.previous_sequence = 9;
    try std.testing.expectError(error.InvalidLogicalOutbox, wrong.encode(a, 64 * 1024));
    wrong = frame;
    wrong.writes = &.{ frame.writes[1], frame.writes[0] };
    try std.testing.expectError(error.InvalidLogicalOutbox, wrong.encode(a, 64 * 1024));
    wrong.writes = &.{ frame.writes[0], frame.writes[0] };
    try std.testing.expectError(error.InvalidLogicalOutbox, wrong.encode(a, 64 * 1024));
    const corrupt = try a.dupe(u8, encoded);
    defer a.free(corrupt);
    corrupt[corrupt.len - 33] ^= 1;
    try std.testing.expectError(error.InvalidLogicalOutbox, Frame.decode(arena.allocator(), corrupt, corrupt.len));
    const Check = struct {
        fn run(alloc: A, bytes: []const u8) !void {
            var owned: std.heap.ArenaAllocator = .init(alloc);
            defer owned.deinit();
            const value = try Frame.decode(owned.allocator(), bytes, bytes.len);
            const again = try value.encode(alloc, bytes.len);
            defer alloc.free(again);
            try std.testing.expectEqualSlices(u8, bytes, again);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{encoded});
}
