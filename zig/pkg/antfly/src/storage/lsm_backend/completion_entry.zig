// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Canonical replicated prepare envelope. Its checksum detects corruption, not
//! authority. Native admission must separately validate policy, baseline, static
//! footprint and reserved capacity before acknowledging the accepted log entry.
//! Applied Raft term/index are supplied by consensus, never sealed by this codec.
const std = @import("std");
const slot = @import("completion_slot.zig");
const Allocator = std.mem.Allocator;
pub const protocol = @import("../../common/completion_entry_protocol.zig");
const magic = protocol.magic;
pub const version: u16 = protocol.version;
pub const profile: u16 = protocol.profile;
pub const max_wire_bytes = protocol.max_wire_bytes;
pub const max_descriptor_bytes = 256 * 1024;
pub const max_operations = 256;
pub const max_baseline_keys = 512;
pub const receipt_prefix = "\x00\x00__metadata__:completion_entry_v1:";
pub const group_progress_key = "\x00\x00__metadata__:completion_group_progress_v1";
pub fn receiptKey(txn_id: [16]u8) [receipt_prefix.len + 16]u8 {
    var key: [receipt_prefix.len + 16]u8 = undefined;
    @memcpy(key[0..receipt_prefix.len], receipt_prefix);
    @memcpy(key[receipt_prefix.len..], &txn_id);
    return key;
}
const header_bytes = 256;
const operation_header_bytes = 12;

pub const Entry = struct {
    /// Mutation entries apply once; they never create a prepared transaction
    /// or wait for a subsequent decision. V1 prepares retain their wire form.
    kind: protocol.Kind = .prepare,
    group_id: u64,
    group_incarnation: [16]u8,
    policy_digest: [32]u8,
    schema_catalog_digest: [32]u8,
    txn_id: [16]u8,
    original_input_digest: [32]u8,
    baseline_digest: [32]u8,
    previous_term: u64 = 0,
    previous_index: u64 = 0,
    baseline_keys: []const []const u8,
    descriptor: []const u8,
    prepare_operations: []const slot.Operation,
};

pub const OwnedEntry = struct {
    allocator: Allocator,
    storage: []align(@alignOf(slot.Operation)) u8,
    decoded_descriptor: slot.OwnedDescriptor,
    entry: Entry,
    digest: [32]u8,

    pub fn deinit(self: *OwnedEntry) void {
        self.decoded_descriptor.deinit();
        self.allocator.free(self.storage);
        self.* = undefined;
    }
};

fn sum(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.CompletionSlotTooLarge;
}

fn checksum(bytes: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(bytes[0..224]);
    hash.update(bytes[header_bytes..]);
    return hash.finalResult();
}

fn validateOperation(op: slot.Operation) !void {
    if (op.key.len == 0 or op.bindings.len != 0 or (op.kind == .delete and op.value.len != 0))
        return error.InvalidCompletionSlot;
    try validateNativeOwnershipKey(op.key);
    // The authoritative marker is appended exactly once by accepted apply.
    if (std.mem.eql(u8, op.key, &@import("../internal_keys.zig").raft_document_applied_entry_key))
        return error.InvalidCompletionSlot;
}

fn validateNativeOwnershipKey(key: []const u8) !void {
    if (std.mem.eql(u8, key, group_progress_key)) return error.InvalidCompletionSlot;
    // Native ownership records cannot be selected or overwritten by a leader's
    // canonical operation list. Accepted apply appends its own indexed records.
    const control = @import("completion_control_record.zig");
    inline for (.{ "\x00\x00__metadata__:completion_slot_v1", "\x00\x00__metadata__:completion_applied_v1", receipt_prefix, control.owner_prefix, control.receipt_prefix }) |prefix| {
        if (std.mem.startsWith(u8, key, prefix)) return error.InvalidCompletionSlot;
    }
}

fn validateKind(kind: protocol.Kind, descriptor: slot.Descriptor) !void {
    // A mutation has no future outcome. Encoding templates here would make
    // its ownership ambiguous and could publish progress before all effects.
    if (kind == .mutation and (descriptor.commit.len != 0 or descriptor.abort.len != 0))
        return error.InvalidCompletionSlot;
    // Outcome templates are also supplied by the leader. Their dynamic public
    // applied marker is allowed, but native ownership remains replica-owned.
    for ([_][]const slot.Operation{ descriptor.commit, descriptor.abort }) |operations|
        for (operations) |op| try validateNativeOwnershipKey(op.key);
}

pub fn encode(allocator: Allocator, entry: Entry) ![]u8 {
    if (entry.group_id == 0 or entry.prepare_operations.len == 0) return error.InvalidCompletionSlot;
    if (entry.descriptor.len > max_descriptor_bytes or entry.prepare_operations.len > max_operations or entry.baseline_keys.len > max_baseline_keys)
        return error.CompletionSlotTooLarge;
    var size = try sum(header_bytes, entry.descriptor.len);
    for (entry.prepare_operations) |op| {
        try validateOperation(op);
        size = try sum(size, try sum(operation_header_bytes, try sum(op.key.len, op.value.len)));
        if (size > max_wire_bytes) return error.CompletionSlotTooLarge;
    }
    try validateBaselineKeys(entry.baseline_keys);
    for (entry.baseline_keys) |key| {
        size = try sum(size, try sum(4, key.len));
        if (size > max_wire_bytes) return error.CompletionSlotTooLarge;
    }
    var descriptor = try slot.decode(allocator, entry.descriptor, .{ .max_wire_bytes = max_descriptor_bytes });
    defer descriptor.deinit();
    if (!std.mem.eql(u8, &descriptor.descriptor.txn_id, &entry.txn_id)) return error.InvalidCompletionSlot;
    try validateKind(entry.kind, descriptor.descriptor);
    try validateCoverage(entry.baseline_keys, entry.prepare_operations, descriptor.descriptor);
    const bytes = try allocator.alloc(u8, size);
    @memset(bytes[0..header_bytes], 0);
    @memcpy(bytes[0..8], magic);
    std.mem.writeInt(u16, bytes[8..10], protocol.wireVersion(entry.kind), .little);
    std.mem.writeInt(u16, bytes[10..12], profile, .little);
    std.mem.writeInt(u32, bytes[12..16], @intCast(size), .little);
    std.mem.writeInt(u64, bytes[16..24], entry.group_id, .little);
    @memcpy(bytes[24..40], &entry.txn_id);
    @memcpy(bytes[40..72], &entry.policy_digest);
    @memcpy(bytes[72..104], &entry.schema_catalog_digest);
    @memcpy(bytes[104..136], &entry.original_input_digest);
    std.mem.writeInt(u32, bytes[136..140], @intCast(entry.descriptor.len), .little);
    std.mem.writeInt(u32, bytes[140..144], @intCast(entry.prepare_operations.len), .little);
    @memcpy(bytes[header_bytes..][0..entry.descriptor.len], entry.descriptor);
    var offset = header_bytes + entry.descriptor.len;
    for (entry.prepare_operations) |op| {
        @memset(bytes[offset..][0..operation_header_bytes], 0);
        bytes[offset] = @intFromEnum(op.kind);
        std.mem.writeInt(u32, bytes[offset + 4 ..][0..4], @intCast(op.key.len), .little);
        std.mem.writeInt(u32, bytes[offset + 8 ..][0..4], @intCast(op.value.len), .little);
        offset += operation_header_bytes;
        @memcpy(bytes[offset..][0..op.key.len], op.key);
        offset += op.key.len;
        @memcpy(bytes[offset..][0..op.value.len], op.value);
        offset += op.value.len;
    }
    @memcpy(bytes[144..176], &entry.baseline_digest);
    @memcpy(bytes[176..192], &entry.group_incarnation);
    std.mem.writeInt(u32, bytes[192..196], @intCast(entry.baseline_keys.len), .little);
    for (entry.baseline_keys) |key| {
        std.mem.writeInt(u32, bytes[offset..][0..4], @intCast(key.len), .little);
        offset += 4;
        @memcpy(bytes[offset..][0..key.len], key);
        offset += key.len;
    }
    std.mem.writeInt(u64, bytes[200..208], entry.previous_term, .little);
    std.mem.writeInt(u64, bytes[208..216], entry.previous_index, .little);
    bytes[216] = @intFromEnum(entry.kind);
    @memcpy(bytes[224..256], &checksum(bytes));
    return bytes;
}

fn readOperations(bytes: []const u8, descriptor_len: usize, count: usize, output: ?[]slot.Operation) !usize {
    var offset = try sum(header_bytes, descriptor_len);
    if (offset > bytes.len) return error.InvalidCompletionSlot;
    for (0..count) |i| {
        if (bytes.len - offset < operation_header_bytes) return error.InvalidCompletionSlot;
        const header = bytes[offset..][0..operation_header_bytes];
        if (!std.mem.allEqual(u8, header[1..4], 0)) return error.InvalidCompletionSlot;
        const kind: @FieldType(slot.Operation, "kind") = switch (header[0]) {
            0 => .put,
            1 => .delete,
            else => return error.InvalidCompletionSlot,
        };
        const key_len = std.mem.readInt(u32, header[4..8], .little);
        const value_len = std.mem.readInt(u32, header[8..12], .little);
        offset += operation_header_bytes;
        const data_len = try sum(key_len, value_len);
        if (data_len > bytes.len - offset) return error.InvalidCompletionSlot;
        const op: slot.Operation = .{ .kind = kind, .key = bytes[offset..][0..key_len], .value = bytes[offset + key_len ..][0..value_len] };
        try validateOperation(op);
        if (output) |operations| operations[i] = op;
        offset += data_len;
    }
    return offset;
}

fn validateBaselineKeys(keys: []const []const u8) !void {
    if (keys.len == 0) return error.InvalidCompletionSlot;
    for (keys, 0..) |key, i| {
        if (key.len == 0 or (i != 0 and std.mem.order(u8, keys[i - 1], key) != .lt)) return error.InvalidCompletionSlot;
    }
}

fn hasBaselineKey(keys: []const []const u8, needle: []const u8) bool {
    var lo: usize = 0;
    var hi = keys.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        switch (std.mem.order(u8, keys[mid], needle)) {
            .lt => lo = mid + 1,
            .gt => hi = mid,
            .eq => return true,
        }
    }
    return false;
}
fn validateCoverage(keys: []const []const u8, prepare: []const slot.Operation, descriptor: slot.Descriptor) !void {
    for (prepare) |op| if (!hasBaselineKey(keys, op.key)) return error.InvalidCompletionSlot;
    for ([_][]const slot.Operation{ descriptor.commit, descriptor.abort }) |ops| for (ops) |op| {
        if (!slot.isSharedDynamicOperation(op) and !hasBaselineKey(keys, op.key)) return error.InvalidCompletionSlot;
    };
}

fn readBaselineKeys(bytes: []const u8, start: usize, count: usize, output: ?[][]const u8) !void {
    if (count == 0) return error.InvalidCompletionSlot;
    var offset = start;
    var previous: ?[]const u8 = null;
    for (0..count) |i| {
        if (bytes.len - offset < 4) return error.InvalidCompletionSlot;
        const len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        if (len == 0 or len > bytes.len - offset) return error.InvalidCompletionSlot;
        const key = bytes[offset..][0..len];
        if (previous) |prior| if (std.mem.order(u8, prior, key) != .lt) return error.InvalidCompletionSlot;
        previous = key;
        if (output) |keys| keys[i] = key;
        offset += len;
    }
    if (offset != bytes.len) return error.InvalidCompletionSlot;
}

/// Both leader and follower feed sorted keys and exact current values. Keys
/// remain borrowed through finish; null distinguishes absence from empty value.
pub const BaselineHasher = struct {
    hash: std.crypto.hash.sha2.Sha256,
    previous: ?[]const u8 = null,
    pub fn init() BaselineHasher {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-completion-baseline-v1\x00");
        return .{ .hash = hash };
    }
    pub fn add(self: *BaselineHasher, key: []const u8, value: ?[]const u8) !void {
        if (key.len == 0) return error.InvalidCompletionSlot;
        if (self.previous) |prior| if (std.mem.order(u8, prior, key) != .lt) return error.InvalidCompletionSlot;
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, key.len, .little);
        self.hash.update(&length);
        self.hash.update(key);
        self.hash.update(&.{@intFromBool(value != null)});
        std.mem.writeInt(u64, &length, if (value) |bytes| bytes.len else 0, .little);
        self.hash.update(&length);
        if (value) |bytes| self.hash.update(bytes);
        self.previous = key;
    }
    pub fn finish(self: *BaselineHasher) [32]u8 {
        return self.hash.finalResult();
    }
};

pub fn decode(allocator: Allocator, encoded: []const u8) !OwnedEntry {
    if (encoded.len > max_wire_bytes) return error.CompletionSlotTooLarge;
    if (encoded.len < header_bytes or !std.mem.eql(u8, encoded[0..8], magic)) return error.InvalidCompletionSlot;
    const wire_version = std.mem.readInt(u16, encoded[8..10], .little);
    if ((wire_version != version and wire_version != protocol.mutation_version) or
        std.mem.readInt(u16, encoded[10..12], .little) != profile) return error.UnsupportedCompletionSlotVersion;
    const kind: protocol.Kind = switch (encoded[216]) {
        0 => .prepare,
        1 => .mutation,
        else => return error.InvalidCompletionSlot,
    };
    // Each operation kind has exactly one encoding. In particular, relabeling
    // a mutation as v1 cannot bypass rolling-version admission.
    if (wire_version != protocol.wireVersion(kind)) return error.UnsupportedCompletionSlotVersion;
    if (std.mem.readInt(u32, encoded[12..16], .little) != encoded.len or
        !std.mem.allEqual(u8, encoded[196..200], 0) or !std.mem.allEqual(u8, encoded[217..224], 0)) return error.InvalidCompletionSlot;
    const digest = checksum(encoded);
    if (!std.mem.eql(u8, encoded[224..256], &digest)) return error.CompletionSlotChecksumMismatch;
    const group_id = std.mem.readInt(u64, encoded[16..24], .little);
    const descriptor_len = std.mem.readInt(u32, encoded[136..140], .little);
    const count = std.mem.readInt(u32, encoded[140..144], .little);
    if (group_id == 0 or count == 0) return error.InvalidCompletionSlot;
    if (descriptor_len > max_descriptor_bytes or count > max_operations) return error.CompletionSlotTooLarge;
    const baseline_count = std.mem.readInt(u32, encoded[192..196], .little);
    if (baseline_count > max_baseline_keys) return error.CompletionSlotTooLarge;
    const keys_offset = try readOperations(encoded, descriptor_len, count, null);
    try readBaselineKeys(encoded, keys_offset, baseline_count, null);
    var descriptor = try slot.decode(allocator, encoded[header_bytes..][0..descriptor_len], .{ .max_wire_bytes = max_descriptor_bytes });
    errdefer descriptor.deinit();
    if (!std.mem.eql(u8, &descriptor.descriptor.txn_id, encoded[24..40])) return error.InvalidCompletionSlot;
    try validateKind(kind, descriptor.descriptor);
    const operation_bytes = @as(usize, count) * @sizeOf(slot.Operation);
    const key_bytes = @as(usize, baseline_count) * @sizeOf([]const u8);
    const wire_offset = try sum(operation_bytes, key_bytes);
    const storage = try allocator.alignedAlloc(u8, .of(slot.Operation), try sum(wire_offset, encoded.len));
    errdefer allocator.free(storage);
    const operations = @as([*]slot.Operation, @ptrCast(storage.ptr))[0..count];
    const keys = @as([*][]const u8, @ptrCast(@alignCast(storage.ptr + operation_bytes)))[0..baseline_count];
    const wire = storage[wire_offset..];
    @memcpy(wire, encoded);
    _ = try readOperations(wire, descriptor_len, count, operations);
    try readBaselineKeys(wire, keys_offset, baseline_count, keys);
    try validateCoverage(keys, operations, descriptor.descriptor);
    return .{ .allocator = allocator, .storage = storage, .decoded_descriptor = descriptor, .digest = protocol.payloadDigest(encoded), .entry = .{
        .kind = kind,
        .group_id = group_id,
        .group_incarnation = wire[176..192].*,
        .baseline_keys = keys,
        .policy_digest = wire[40..72].*,
        .schema_catalog_digest = wire[72..104].*,
        .txn_id = wire[24..40].*,
        .original_input_digest = wire[104..136].*,
        .baseline_digest = wire[144..176].*,
        .previous_term = std.mem.readInt(u64, wire[200..208], .little),
        .previous_index = std.mem.readInt(u64, wire[208..216], .little),
        .descriptor = wire[header_bytes..][0..descriptor_len],
        .prepare_operations = operations,
    } };
}

fn fixtureDescriptor(allocator: Allocator) ![]u8 {
    return slot.encode(allocator, .{
        .txn_id = @splat(7),
        .intent_revision = 9,
        .limits = .{ .memory_bytes = 4096, .wal_bytes = 4096, .flush_bytes = 4096, .fd_count = 2, .max_operations = 4, .max_encoded_bytes = 4096 },
        .profile_fence = "replicated-profile",
        .commit = &.{.{ .kind = .put, .key = "row", .value = "committed" }},
        .abort = &.{.{ .kind = .delete, .key = "intent" }},
    }, .{});
}
fn fixture(descriptor: []const u8) Entry {
    return .{
        .group_id = 3,
        .group_incarnation = @splat(3),
        .baseline_keys = &.{ "\x00binary\xffkey", "absent", "intent", "read-only", "row" },
        .policy_digest = @splat(11),
        .schema_catalog_digest = @splat(12),
        .txn_id = @splat(7),
        .original_input_digest = @splat(13),
        .baseline_digest = @splat(14),
        .descriptor = descriptor,
        .prepare_operations = &.{
            .{ .kind = .put, .key = "\x00binary\xffkey", .value = "first" },
            .{ .kind = .delete, .key = "\x00binary\xffkey" },
            .{ .kind = .put, .key = "\x00binary\xffkey", .value = "last\x00" },
        },
    };
}

test "workload admission completion entry owns canonical ordered prepare and authority bytes" {
    const allocator = std.testing.allocator;
    const descriptor = try fixtureDescriptor(allocator);
    defer allocator.free(descriptor);
    const wire = try encode(allocator, fixture(descriptor));
    defer allocator.free(wire);
    var decoded = try decode(allocator, wire);
    defer decoded.deinit();
    const reencoded = try encode(allocator, decoded.entry);
    defer allocator.free(reencoded);
    try std.testing.expectEqualSlices(u8, wire, reencoded);
    @memset(wire, 0);
    @memset(descriptor, 0);
    try std.testing.expectEqual(@as(u64, 3), decoded.entry.group_id);
    try std.testing.expectEqual(@as(u8, 14), decoded.entry.baseline_digest[0]);
    try std.testing.expectEqual(@as(usize, 3), decoded.entry.prepare_operations.len);
    try std.testing.expectEqualStrings("\x00binary\xffkey", decoded.entry.prepare_operations[1].key);
    try std.testing.expectEqual(.delete, decoded.entry.prepare_operations[1].kind);
    try std.testing.expectEqualStrings("last\x00", decoded.entry.prepare_operations[2].value);
    try std.testing.expectEqualStrings("committed", decoded.decoded_descriptor.descriptor.commit[0].value);
    try std.testing.expectEqual(protocol.Kind.prepare, decoded.entry.kind);
    try std.testing.expectEqual(@as(u16, 1), std.mem.readInt(u16, reencoded[8..10], .little));
    try std.testing.expect(std.mem.allEqual(u8, reencoded[216..224], 0));
}

test "workload admission completion entry distinguishes single-phase mutations from prepared transactions" {
    const allocator = std.testing.allocator;
    const descriptor = try slot.encode(allocator, .{
        .txn_id = @splat(7),
        .intent_revision = 0,
        .limits = .{ .memory_bytes = 4096, .wal_bytes = 4096, .flush_bytes = 4096, .fd_count = 2, .max_operations = 4, .max_encoded_bytes = 4096 },
        .profile_fence = "replicated-mutation",
        .commit = &.{},
        .abort = &.{},
    }, .{});
    defer allocator.free(descriptor);
    var input = fixture(descriptor);
    input.kind = .mutation;
    const wire = try encode(allocator, input);
    defer allocator.free(wire);
    try std.testing.expectEqual(@as(u16, 2), std.mem.readInt(u16, wire[8..10], .little));
    try std.testing.expectEqual(@as(u16, 8), protocol.requiredRaftVersion(input.kind));
    var decoded = try decode(allocator, wire);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.Kind.mutation, decoded.entry.kind);
    try std.testing.expectEqual(@as(usize, 0), decoded.decoded_descriptor.descriptor.commit.len);
    try std.testing.expectEqual(@as(usize, 0), decoded.decoded_descriptor.descriptor.abort.len);
    try std.testing.expectEqual(@as(usize, 3), decoded.entry.prepare_operations.len);
    const copied = try encode(allocator, decoded.entry);
    defer allocator.free(copied);
    try std.testing.expectEqualSlices(u8, wire, copied);

    // Neither a recomputed checksum nor an older advertised version can turn
    // a single-phase mutation into a v1 prepare.
    std.mem.writeInt(u16, wire[8..10], 1, .little);
    @memcpy(wire[224..256], &checksum(wire));
    try std.testing.expectError(error.UnsupportedCompletionSlotVersion, decode(std.testing.failing_allocator, wire));
    std.mem.writeInt(u16, wire[8..10], 2, .little);
    wire[216] = 0;
    @memcpy(wire[224..256], &checksum(wire));
    try std.testing.expectError(error.UnsupportedCompletionSlotVersion, decode(std.testing.failing_allocator, wire));
    wire[216] = 1;
    wire[217] = 1;
    @memcpy(wire[224..256], &checksum(wire));
    try std.testing.expectError(error.InvalidCompletionSlot, decode(std.testing.failing_allocator, wire));
}

test "workload admission completion entry forbids future outcomes on single-phase mutations" {
    const allocator = std.testing.allocator;
    const descriptor = try fixtureDescriptor(allocator);
    defer allocator.free(descriptor);
    var input = fixture(descriptor);
    input.kind = .mutation;
    try std.testing.expectError(error.InvalidCompletionSlot, encode(allocator, input));
    input.kind = .prepare;
    const wire = try encode(allocator, input);
    defer allocator.free(wire);
    std.mem.writeInt(u16, wire[8..10], protocol.mutation_version, .little);
    wire[216] = @intFromEnum(protocol.Kind.mutation);
    @memcpy(wire[224..256], &checksum(wire));
    try std.testing.expectError(error.InvalidCompletionSlot, decode(allocator, wire));
}

test "workload admission completion entry rejects canonical framing corruption and oversized input before allocation" {
    const allocator = std.testing.allocator;
    const descriptor = try fixtureDescriptor(allocator);
    defer allocator.free(descriptor);
    const wire = try encode(allocator, fixture(descriptor));
    defer allocator.free(wire);
    try std.testing.expectError(error.InvalidCompletionSlot, decode(std.testing.failing_allocator, wire[0 .. wire.len - 1]));
    wire[wire.len - 1] ^= 1;
    try std.testing.expectError(error.CompletionSlotChecksumMismatch, decode(std.testing.failing_allocator, wire));
    wire[wire.len - 1] ^= 1;
    // Even a recomputed checksum cannot legitimize ambiguous operation framing.
    const operation_offset = header_bytes + descriptor.len;
    wire[operation_offset + 1] = 1;
    @memcpy(wire[224..256], &checksum(wire));
    try std.testing.expectError(error.InvalidCompletionSlot, decode(std.testing.failing_allocator, wire));
    wire[operation_offset + 1] = 0;
    std.mem.writeInt(u32, wire[140..144], max_operations + 1, .little);
    @memcpy(wire[224..256], &checksum(wire));
    try std.testing.expectError(error.CompletionSlotTooLarge, decode(std.testing.failing_allocator, wire));
}

fn allocationRoundtrip(allocator: Allocator, descriptor: []const u8) !void {
    const wire = try encode(allocator, fixture(descriptor));
    defer allocator.free(wire);
    var decoded = try decode(allocator, wire);
    defer decoded.deinit();
    try std.testing.expectEqualStrings("last\x00", decoded.entry.prepare_operations[2].value);
}
test "workload admission completion entry allocation failures and descriptor identity fail closed" {
    const allocator = std.testing.allocator;
    const descriptor = try fixtureDescriptor(allocator);
    defer allocator.free(descriptor);
    try std.testing.checkAllAllocationFailures(allocator, allocationRoundtrip, .{descriptor});
    var entry = fixture(descriptor);
    entry.txn_id[0] = 19;
    try std.testing.expectError(error.InvalidCompletionSlot, encode(allocator, entry));
    entry = fixture(descriptor);
    entry.prepare_operations = &.{.{ .kind = .put, .key = &@import("../internal_keys.zig").raft_document_applied_entry_key, .value = "arbitrarymarker" }};
    try std.testing.expectError(error.InvalidCompletionSlot, encode(allocator, entry));
    entry.prepare_operations = &.{.{ .kind = .put, .key = "\x00\x00__metadata__:completion_slot_v1_3", .value = "forged" }};
    try std.testing.expectError(error.InvalidCompletionSlot, encode(allocator, entry));
    entry.prepare_operations = &.{.{ .kind = .put, .key = "row", .value = "00000000", .bindings = &.{.{ .kind = .raft_term, .target = .value, .byte_order = .little, .offset = 0 }} }};
    try std.testing.expectError(error.InvalidCompletionSlot, encode(allocator, entry));
}

test "workload admission completion entry baseline binds absence empty values and complete sorted footprint" {
    var absent = BaselineHasher.init();
    try absent.add("key", null);
    var empty = BaselineHasher.init();
    try empty.add("key", "");
    try std.testing.expect(!std.mem.eql(u8, &absent.finish(), &empty.finish()));
    try std.testing.expectError(error.InvalidCompletionSlot, empty.add("key", "duplicate"));
    try std.testing.expectError(error.InvalidCompletionSlot, empty.add("earlier", "out-of-order"));
    const allocator = std.testing.allocator;
    const descriptor = try fixtureDescriptor(allocator);
    defer allocator.free(descriptor);
    var entry = fixture(descriptor);
    entry.baseline_keys = &.{ "absent", "intent", "read-only", "row" };
    try std.testing.expectError(error.InvalidCompletionSlot, encode(allocator, entry));
    entry.baseline_keys = &.{ "\x00binary\xffkey", "absent", "read-only", "row" };
    try std.testing.expectError(error.InvalidCompletionSlot, encode(allocator, entry));
    entry.baseline_keys = &.{ "row", "absent" };
    try std.testing.expectError(error.InvalidCompletionSlot, encode(std.testing.failing_allocator, entry));
}

test "workload admission completion compiler cannot forge native transaction control ownership" {
    const control = @import("completion_control_record.zig");
    const alloc = std.testing.allocator;
    const descriptor = try fixtureDescriptor(alloc);
    defer alloc.free(descriptor);
    const owner_key = control.ownerKey(@splat(7));
    const receipt_key = control.receiptKey(@splat(7));
    for ([_][]const u8{ &owner_key, &receipt_key }) |private_key| {
        var input = fixture(descriptor);
        input.prepare_operations = &.{.{ .kind = .put, .key = private_key, .value = "forged" }};
        // Full sorted baseline coverage makes this an otherwise valid plan;
        // rejection must come from native ownership, not a missing dependency.
        input.baseline_keys = &.{ private_key, "intent", "row" };
        try std.testing.expectError(error.InvalidCompletionSlot, encode(std.testing.failing_allocator, input));
        input.prepare_operations = &.{.{ .kind = .delete, .key = private_key }};
        try std.testing.expectError(error.InvalidCompletionSlot, encode(std.testing.failing_allocator, input));
        var decoded = try slot.decode(alloc, descriptor, .{});
        defer decoded.deinit();
        for ([_]bool{ false, true }) |commit| {
            var outcome = decoded.descriptor;
            const forged: []const slot.Operation = &.{.{ .kind = .delete, .key = private_key }};
            if (commit) outcome.commit = forged else outcome.abort = forged;
            const wire = try slot.encode(alloc, outcome, .{});
            defer alloc.free(wire);
            input = fixture(wire);
            input.baseline_keys = &.{ private_key, "\x00binary\xffkey", "absent", "intent", "read-only", "row" };
            try std.testing.expectError(error.InvalidCompletionSlot, encode(alloc, input));
        }
    }
}
