// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Versioned completion-slot descriptor only. Resource quantities are claims
//! to restore and validate, not reservations or a completion certificate. This
//! module cannot apply templates. Dynamic counters/sequences must be bound to
//! authoritative state by the serialized completion path, never replayed from
//! an earlier observation. The checksum detects corruption, not forgery.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const magic = "AFCSLOT\x00";
pub const version: u16 = 1;
pub const storage_key = "\x00\x00__metadata__:completion_slot_v1";
const header_len = 48;

/// Restart input bounds include both outcome templates and the full wire record.
pub const Limits = struct {
    memory_bytes: u64,
    wal_bytes: u64,
    flush_bytes: u64,
    fd_count: u32,
    max_operations: u32,
    max_encoded_bytes: u32,
};
pub const Bounds = struct {
    max_wire_bytes: usize = 1024 * 1024,
    max_operations: usize = 4096,
    max_bindings: usize = 4096,
    max_bindings_per_operation: usize = 128,
    max_namespace_bytes: usize = 1024,
    max_fence_bytes: usize = 4096,
    max_memory_bytes: u64 = 1024 * 1024 * 1024,
    max_wal_bytes: u64 = 1024 * 1024 * 1024,
    max_flush_bytes: u64 = 1024 * 1024 * 1024,
    max_fd_count: u32 = 64,
};
pub const Binding = struct {
    kind: enum(u8) { commit_timestamp, replay_sequence, shared_ledger_count, shared_ledger_bytes, replay_next_sequence },
    target: enum(u8) { key, value },
    byte_order: enum(u8) { little, big },
    offset: u32,
};
pub const Operation = struct {
    kind: enum(u8) { put, delete },
    key: []const u8,
    value: []const u8 = "",
    bindings: []const Binding = &.{},
};
pub const Descriptor = struct {
    txn_id: [16]u8,
    intent_revision: u64,
    namespace: ?[]const u8 = null,
    limits: Limits,
    profile_fence: []const u8,
    commit: []const Operation,
    abort: []const Operation,
};
const alignment = std.mem.Alignment.of(Operation);
pub const OwnedDescriptor = struct {
    allocator: Allocator,
    storage: []align(@alignOf(Operation)) u8,
    descriptor: Descriptor,
    pub fn deinit(self: *OwnedDescriptor) void {
        self.allocator.free(self.storage);
        self.* = undefined;
    }
};

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.CompletionSlotTooLarge;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.CompletionSlotTooLarge;
}
fn boundedLen(n: usize) !u32 {
    return std.math.cast(u32, n) orelse error.CompletionSlotTooLarge;
}
fn validateLimits(l: Limits, b: Bounds) !void {
    if (l.memory_bytes == 0 or l.wal_bytes == 0 or l.flush_bytes == 0 or l.fd_count == 0 or
        l.max_operations == 0 or l.max_encoded_bytes < header_len) return error.InvalidCompletionSlot;
    if (l.memory_bytes > b.max_memory_bytes or l.wal_bytes > b.max_wal_bytes or l.flush_bytes > b.max_flush_bytes or
        l.fd_count > b.max_fd_count or l.max_operations > b.max_operations or l.max_encoded_bytes > b.max_wire_bytes)
        return error.CompletionSlotTooLarge;
}
fn validateBinding(op: Operation, binding: Binding, previous: []const Binding) !void {
    const bytes = switch (binding.target) {
        .key => op.key,
        .value => op.value,
    };
    if (op.kind == .delete and binding.target == .value) return error.InvalidCompletionSlot;
    const end = try add(binding.offset, 8);
    if (end > bytes.len) return error.InvalidCompletionSlot;
    for (previous) |prior| if (prior.target == binding.target) {
        const prior_end = try add(prior.offset, 8);
        if (binding.offset < prior_end and prior.offset < end) return error.InvalidCompletionSlot;
    };
}
fn measure(d: Descriptor, b: Bounds) !usize {
    try validateLimits(d.limits, b);
    if (d.profile_fence.len > b.max_fence_bytes or (if (d.namespace) |ns| ns.len else 0) > b.max_namespace_bytes)
        return error.CompletionSlotTooLarge;
    var len: usize = header_len + 16 + 8 + 36 + 1 + 4 + 8;
    len = try add(len, d.profile_fence.len);
    if (d.namespace) |ns| len = try add(try add(len, 4), ns.len);
    const count = try add(d.commit.len, d.abort.len);
    if (count > d.limits.max_operations) return error.CompletionSlotTooLarge;
    var bindings: usize = 0;
    for ([_][]const Operation{ d.commit, d.abort }) |ops| for (ops) |op| {
        if (op.key.len == 0 or (op.kind == .delete and op.value.len != 0)) return error.InvalidCompletionSlot;
        if (op.bindings.len > b.max_bindings_per_operation) return error.CompletionSlotTooLarge;
        bindings = try add(bindings, op.bindings.len);
        if (bindings > b.max_bindings) return error.CompletionSlotTooLarge;
        for (op.bindings, 0..) |binding, i| try validateBinding(op, binding, op.bindings[0..i]);
        len = try add(len, try add(13, try add(op.key.len, op.value.len)));
        len = try add(len, try mul(op.bindings.len, 8));
    };
    if (len > d.limits.max_encoded_bytes or len > b.max_wire_bytes) return error.CompletionSlotTooLarge;
    _ = try boundedLen(len);
    return len;
}
const Writer = struct {
    bytes: []u8,
    pos: usize = 0,
    fn raw(self: *Writer, value: []const u8) void {
        @memcpy(self.bytes[self.pos..][0..value.len], value);
        self.pos += value.len;
    }
    fn int(self: *Writer, comptime T: type, value: T) void {
        std.mem.writeInt(T, self.bytes[self.pos..][0..@sizeOf(T)], value, .little);
        self.pos += @sizeOf(T);
    }
    fn slice(self: *Writer, value: []const u8) void {
        self.int(u32, @intCast(value.len));
        self.raw(value);
    }
};
fn digest(bytes: []const u8) [32]u8 {
    var hash = Sha256.init(.{});
    hash.update(bytes[0..16]);
    hash.update(bytes[header_len..]);
    return hash.finalResult();
}
pub fn encode(alloc: Allocator, d: Descriptor, bounds: Bounds) ![]u8 {
    const len = try measure(d, bounds);
    const bytes = try alloc.alloc(u8, len);
    var w = Writer{ .bytes = bytes };
    w.raw(magic);
    w.int(u16, version);
    w.int(u16, 0);
    w.int(u32, @intCast(len));
    w.raw(&@as([32]u8, @splat(0)));
    w.raw(&d.txn_id);
    w.int(u64, d.intent_revision);
    inline for (.{ "memory_bytes", "wal_bytes", "flush_bytes" }) |field| w.int(u64, @field(d.limits, field));
    inline for (.{ "fd_count", "max_operations", "max_encoded_bytes" }) |field| w.int(u32, @field(d.limits, field));
    w.int(u8, @intFromBool(d.namespace != null));
    if (d.namespace) |ns| w.slice(ns);
    w.slice(d.profile_fence);
    for ([_][]const Operation{ d.commit, d.abort }) |ops| {
        w.int(u32, @intCast(ops.len));
        for (ops) |op| {
            w.int(u8, @intFromEnum(op.kind));
            w.slice(op.key);
            w.slice(op.value);
            w.int(u32, @intCast(op.bindings.len));
            for (op.bindings) |binding| {
                w.int(u8, @intFromEnum(binding.kind));
                w.int(u8, @intFromEnum(binding.target));
                w.int(u8, @intFromEnum(binding.byte_order));
                w.int(u8, 0);
                w.int(u32, binding.offset);
            }
        }
    }
    std.debug.assert(w.pos == len);
    @memcpy(bytes[16..48], &digest(bytes));
    return bytes;
}
const Reader = struct {
    bytes: []const u8,
    pos: usize = header_len,
    fn raw(self: *Reader, len: usize) ![]const u8 {
        if (len > self.bytes.len - self.pos) return error.InvalidCompletionSlot;
        const result = self.bytes[self.pos..][0..len];
        self.pos += len;
        return result;
    }
    fn int(self: *Reader, comptime T: type) !T {
        return std.mem.readInt(T, (try self.raw(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
    fn slice(self: *Reader) ![]const u8 {
        return self.raw(try self.int(u32));
    }
    fn binding(self: *Reader) !Binding {
        const kind = std.enums.fromInt(@FieldType(Binding, "kind"), try self.int(u8)) orelse return error.InvalidCompletionSlot;
        const target = std.enums.fromInt(@FieldType(Binding, "target"), try self.int(u8)) orelse return error.InvalidCompletionSlot;
        const byte_order = std.enums.fromInt(@FieldType(Binding, "byte_order"), try self.int(u8)) orelse return error.InvalidCompletionSlot;
        if (try self.int(u8) != 0) return error.InvalidCompletionSlot;
        return .{ .kind = kind, .target = target, .byte_order = byte_order, .offset = try self.int(u32) };
    }
};
const Counts = struct { operations: usize = 0, bindings: usize = 0 };
fn readPayload(r: *Reader, bounds: Bounds, counts: *Counts, operations: ?[]Operation, bindings: ?[]Binding) !Descriptor {
    var d: Descriptor = undefined;
    d.txn_id = (try r.raw(16))[0..16].*;
    d.intent_revision = try r.int(u64);
    inline for (.{ "memory_bytes", "wal_bytes", "flush_bytes" }) |field| @field(d.limits, field) = try r.int(u64);
    inline for (.{ "fd_count", "max_operations", "max_encoded_bytes" }) |field| @field(d.limits, field) = try r.int(u32);
    try validateLimits(d.limits, bounds);
    if (r.bytes.len > d.limits.max_encoded_bytes) return error.CompletionSlotTooLarge;
    d.namespace = switch (try r.int(u8)) {
        0 => null,
        1 => try r.slice(),
        else => return error.InvalidCompletionSlot,
    };
    if ((if (d.namespace) |ns| ns.len else 0) > bounds.max_namespace_bytes) return error.CompletionSlotTooLarge;
    d.profile_fence = try r.slice();
    if (d.profile_fence.len > bounds.max_fence_bytes) return error.CompletionSlotTooLarge;
    inline for (.{ "commit", "abort" }) |field| {
        const count = try r.int(u32);
        const start = counts.operations;
        counts.operations = try add(start, count);
        if (counts.operations > d.limits.max_operations) return error.CompletionSlotTooLarge;
        for (0..count) |i| {
            var op = Operation{ .kind = std.enums.fromInt(@FieldType(Operation, "kind"), try r.int(u8)) orelse return error.InvalidCompletionSlot, .key = try r.slice(), .value = try r.slice() };
            if (op.key.len == 0 or (op.kind == .delete and op.value.len != 0)) return error.InvalidCompletionSlot;
            const binding_count = try r.int(u32);
            if (binding_count > bounds.max_bindings_per_operation) return error.CompletionSlotTooLarge;
            const binding_start = counts.bindings;
            counts.bindings = try add(binding_start, binding_count);
            if (counts.bindings > bounds.max_bindings) return error.CompletionSlotTooLarge;
            const prior_start = r.pos;
            for (0..binding_count) |j| {
                const current = try r.binding();
                try validateBinding(op, current, &.{});
                var prior = Reader{ .bytes = r.bytes, .pos = prior_start };
                for (0..j) |_| {
                    const previous = try prior.binding();
                    try validateBinding(op, current, &.{previous});
                }
                if (bindings) |out| out[binding_start + j] = current;
            }
            if (bindings) |out| op.bindings = out[binding_start..counts.bindings];
            if (operations) |out| out[start + i] = op;
        }
        @field(d, field) = if (operations) |out| out[start..counts.operations] else &.{};
    }
    if (r.pos != r.bytes.len) return error.InvalidCompletionSlot;
    return d;
}
pub fn decode(alloc: Allocator, encoded: []const u8, bounds: Bounds) !OwnedDescriptor {
    if (encoded.len > bounds.max_wire_bytes) return error.CompletionSlotTooLarge;
    if (encoded.len < header_len or !std.mem.eql(u8, encoded[0..8], magic)) return error.InvalidCompletionSlot;
    if (std.mem.readInt(u16, encoded[8..10], .little) != version) return error.UnsupportedCompletionSlotVersion;
    if (std.mem.readInt(u16, encoded[10..12], .little) != 0 or std.mem.readInt(u32, encoded[12..16], .little) != encoded.len)
        return error.InvalidCompletionSlot;
    if (!std.mem.eql(u8, encoded[16..48], &digest(encoded))) return error.CompletionSlotChecksumMismatch;
    var reader = Reader{ .bytes = encoded };
    var counts = Counts{};
    _ = try readPayload(&reader, bounds, &counts, null, null);
    // No allocation until every length, enum, bound, binding and checksum has
    // been checked. The result uses one allocation and owns all returned bytes.
    const operation_bytes = try mul(counts.operations, @sizeOf(Operation));
    const binding_bytes = try mul(counts.bindings, @sizeOf(Binding));
    comptime std.debug.assert(@alignOf(Operation) >= @alignOf(Binding));
    const wire_offset = try add(operation_bytes, binding_bytes);
    const storage = try alloc.alignedAlloc(u8, alignment, try add(wire_offset, encoded.len));
    errdefer alloc.free(storage);
    const ops: []Operation = @as([*]Operation, @ptrCast(storage.ptr))[0..counts.operations];
    const bs: []Binding = @as([*]Binding, @ptrCast(@alignCast(storage.ptr + operation_bytes)))[0..counts.bindings];
    @memcpy(storage[wire_offset..], encoded);
    reader = .{ .bytes = storage[wire_offset..] };
    counts = .{};
    const d = try readPayload(&reader, bounds, &counts, ops, bs);
    return .{ .allocator = alloc, .storage = storage, .descriptor = d };
}

fn fixture() Descriptor {
    return .{
        .txn_id = @splat(17),
        .intent_revision = 9,
        .namespace = "",
        .limits = .{ .memory_bytes = 65536, .wal_bytes = 8192, .flush_bytes = 32768, .fd_count = 2, .max_operations = 8, .max_encoded_bytes = 4096 },
        .profile_fence = "opaque\x00fence",
        .commit = &.{ .{ .kind = .put, .key = "row", .value = "00000000payload", .bindings = &.{.{ .kind = .commit_timestamp, .target = .value, .byte_order = .little, .offset = 0 }} }, .{ .kind = .put, .key = "seq:00000000", .value = "00000000", .bindings = &.{ .{ .kind = .replay_sequence, .target = .key, .byte_order = .big, .offset = 4 }, .{ .kind = .shared_ledger_bytes, .target = .value, .byte_order = .little, .offset = 0 } } } },
        .abort = &.{.{ .kind = .delete, .key = "row" }},
    };
}
test "workload admission completion slot roundtrip owns bytes and preserves ordered templates" {
    var input = fixture();
    for ([_]?[]const u8{ null, "", "named\x00namespace" }) |ns| {
        input.namespace = ns;
        const wire = try encode(std.testing.allocator, input, .{});
        defer std.testing.allocator.free(wire);
        var owned = try decode(std.testing.allocator, wire, .{});
        defer owned.deinit();
        const reencoded = try encode(std.testing.allocator, owned.descriptor, .{});
        defer std.testing.allocator.free(reencoded);
        try std.testing.expectEqualSlices(u8, wire, reencoded);
        @memset(wire, 0);
        try std.testing.expectEqualStrings("row", owned.descriptor.commit[0].key);
        try std.testing.expectEqualStrings("opaque\x00fence", owned.descriptor.profile_fence);
        try std.testing.expectEqual(.delete, owned.descriptor.abort[0].kind);
        try std.testing.expectEqual(ns == null, owned.descriptor.namespace == null);
        try std.testing.expectEqual(.big, owned.descriptor.commit[1].bindings[0].byte_order);
    }
}
test "workload admission completion slot rejects corrupt truncated oversized and trailing input before allocation" {
    const wire = try encode(std.testing.allocator, fixture(), .{});
    defer std.testing.allocator.free(wire);
    for (0..wire.len) |i| {
        try std.testing.expectError(error.InvalidCompletionSlot, decode(std.testing.failing_allocator, wire[0..i], .{}));
    }
    wire[wire.len - 1] ^= 1;
    try std.testing.expectError(error.CompletionSlotChecksumMismatch, decode(std.testing.failing_allocator, wire, .{}));
    wire[wire.len - 1] ^= 1;
    try std.testing.expectError(error.CompletionSlotTooLarge, decode(std.testing.failing_allocator, wire, .{ .max_wire_bytes = wire.len - 1 }));
    try std.testing.expectError(error.CompletionSlotTooLarge, decode(std.testing.failing_allocator, wire, .{ .max_fd_count = 1 }));
    wire[8] = 2;
    try std.testing.expectError(error.UnsupportedCompletionSlotVersion, decode(std.testing.failing_allocator, wire, .{}));
    wire[8] = 1;
    wire[10] = 1;
    try std.testing.expectError(error.InvalidCompletionSlot, decode(std.testing.failing_allocator, wire, .{}));
}
test "workload admission completion slot rejects invalid binding shapes and canonical structural corruption" {
    var input = fixture();
    input.commit = &.{.{ .kind = .put, .key = "key", .value = "0000000000000000", .bindings = &.{ .{ .kind = .shared_ledger_count, .target = .value, .byte_order = .little, .offset = 0 }, .{ .kind = .shared_ledger_bytes, .target = .value, .byte_order = .little, .offset = 7 } } }};
    try std.testing.expectError(error.InvalidCompletionSlot, encode(std.testing.failing_allocator, input, .{}));
    input.commit = &.{.{ .kind = .delete, .key = "key", .value = "invalid" }};
    try std.testing.expectError(error.InvalidCompletionSlot, encode(std.testing.failing_allocator, input, .{}));
    const wire = try encode(std.testing.allocator, fixture(), .{});
    defer std.testing.allocator.free(wire);
    // A valid checksum does not excuse an invalid namespace discriminator.
    wire[header_len + 16 + 8 + 36] = 2;
    @memcpy(wire[16..48], &digest(wire));
    try std.testing.expectError(error.InvalidCompletionSlot, decode(std.testing.failing_allocator, wire, .{}));
}
test "workload admission completion slot checksum valid malicious bindings and trailing bytes fail before allocation" {
    var input = fixture();
    input.abort = &.{};
    input.commit = &.{.{ .kind = .put, .key = "key", .value = "0000000000000000", .bindings = &.{
        .{ .kind = .shared_ledger_count, .target = .value, .byte_order = .little, .offset = 0 },
        .{ .kind = .shared_ledger_bytes, .target = .value, .byte_order = .little, .offset = 8 },
    } }};
    const wire = try encode(std.testing.allocator, input, .{});
    defer std.testing.allocator.free(wire);
    // Final binding is followed only by the empty abort-operation count.
    const offset_position = wire.len - 8;
    std.mem.writeInt(u32, wire[offset_position..][0..4], 7, .little);
    @memcpy(wire[16..48], &digest(wire));
    try std.testing.expectError(error.InvalidCompletionSlot, decode(std.testing.failing_allocator, wire, .{}));
    std.mem.writeInt(u32, wire[offset_position..][0..4], 9, .little);
    @memcpy(wire[16..48], &digest(wire));
    try std.testing.expectError(error.InvalidCompletionSlot, decode(std.testing.failing_allocator, wire, .{}));
    std.mem.writeInt(u32, wire[offset_position..][0..4], 8, .little);
    wire[offset_position - 4] = 255;
    @memcpy(wire[16..48], &digest(wire));
    try std.testing.expectError(error.InvalidCompletionSlot, decode(std.testing.failing_allocator, wire, .{}));
    wire[offset_position - 4] = @intFromEnum(@as(@FieldType(Binding, "kind"), .shared_ledger_bytes));
    @memcpy(wire[16..48], &digest(wire));
    const extended = try std.testing.allocator.alloc(u8, wire.len + 1);
    defer std.testing.allocator.free(extended);
    @memcpy(extended[0..wire.len], wire);
    extended[wire.len] = 0;
    std.mem.writeInt(u32, extended[12..16], @intCast(extended.len), .little);
    @memcpy(extended[16..48], &digest(extended));
    try std.testing.expectError(error.InvalidCompletionSlot, decode(std.testing.failing_allocator, extended, .{}));
}

test "workload admission completion slot allocation failure leaves no partial owned descriptor" {
    const Case = struct {
        fn run(alloc: Allocator) !void {
            const wire = try encode(alloc, fixture(), .{});
            defer alloc.free(wire);
            var owned = try decode(alloc, wire, .{});
            defer owned.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "workload admission completion slot next replay binding roundtrips distinctly" {
    var input = fixture();
    input.commit = &.{.{ .kind = .put, .key = "next", .value = "00000000", .bindings = &.{.{
        .kind = .replay_next_sequence,
        .target = .value,
        .byte_order = .little,
        .offset = 0,
    }} }};
    const wire = try encode(std.testing.allocator, input, .{});
    defer std.testing.allocator.free(wire);
    var owned = try decode(std.testing.allocator, wire, .{});
    defer owned.deinit();
    try std.testing.expectEqual(.replay_next_sequence, owned.descriptor.commit[0].bindings[0].kind);
}
