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

//! Versioned durable generated effects. This codec does not enable a prepare
//! vote or reserve completion capacity. Callers must atomically publish its
//! immutable bytes with the matching intents and grants before voting.
const std = @import("std");
const effect_types = @import("prepared_effect_types.zig");
const derived = @import("derived/derived_types.zig");
const control = @import("../../common/read_admission.zig");

pub const Effects = effect_types.PrecomputedGeneratedBatch;
pub const Mode = enum(u8) { deferred = 0, synchronous = 1 };
/// Durable requirement recorded atomically with BEGIN, independently of the
/// later sealed effect frame and the caller's acknowledgement level.
pub const Requirement = struct {
    mode: Mode,

    pub fn encode(self: Requirement) [40]u8 {
        var raw: [40]u8 = undefined;
        @memcpy(raw[0..4], "ATRQ");
        std.mem.writeInt(u16, raw[4..6], 1, .little);
        raw[6] = 0;
        raw[7] = @backingInt(self.mode);
        std.crypto.hash.Blake3.hash(raw[0..8], raw[8..40], .{});
        return raw;
    }
    pub fn decode(raw: []const u8) !Requirement {
        if (raw.len != 40 or !std.mem.eql(u8, raw[0..4], "ATRQ")) return error.InvalidPreparationRequirement;
        if (std.mem.readInt(u16, raw[4..6], .little) != 1) return error.UnsupportedVersion;
        if (raw[6] != 0) return error.InvalidPreparationRequirement;
        var actual: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw[0..8], &actual, .{});
        if (!std.mem.eql(u8, &actual, raw[8..40])) return error.InvalidPreparationRequirement;
        return .{ .mode = std.enums.fromInt(Mode, raw[7]) orelse return error.InvalidPreparationRequirement };
    }
};

/// Exact route/participant identity supplied by a verified route owner at
/// BEGIN. These hashes bind an agreement; they do not mint route authority.
pub const Binding = struct {
    group_id: u64,
    topology_epoch: u64,
    participant_digest: [32]u8,
    route_digest: [32]u8,
    pub const encoded_bytes = 80;
    pub fn encode(self: Binding) ![encoded_bytes]u8 {
        if (self.group_id == 0) return error.InvalidPreparationRequirement;
        var raw: [encoded_bytes]u8 = undefined;
        std.mem.writeInt(u64, raw[0..8], self.group_id, .little);
        std.mem.writeInt(u64, raw[8..16], self.topology_epoch, .little);
        raw[16..48].* = self.participant_digest;
        raw[48..80].* = self.route_digest;
        return raw;
    }
    pub fn decode(raw: []const u8) !Binding {
        if (raw.len != encoded_bytes) return error.InvalidPreparationRequirement;
        const result: Binding = .{
            .group_id = std.mem.readInt(u64, raw[0..8], .little),
            .topology_epoch = std.mem.readInt(u64, raw[8..16], .little),
            .participant_digest = raw[16..48].*,
            .route_digest = raw[48..80].*,
        };
        if (result.group_id == 0) return error.InvalidPreparationRequirement;
        return result;
    }
};

pub const BoundReceipt = struct {
    txn_id: [16]u8,
    requirement: Requirement,
    binding: Binding,
    intent_revision: u64,
    intent_digest: [32]u8,
    catalog_digest: [32]u8,
    frame_digest: [32]u8,
};

pub const Contract = struct {
    txn_id: [16]u8,
    intent_revision: u64,
    intent_digest: [32]u8,
    catalog_digest: [32]u8,
    schema_version: ?u32 = null,
    mode: Mode,
};

/// Canonical v1 catalog context, shared by preparation and cold recovery.
/// Keep byte framing and missing-as-empty semantics compatible with accepted
/// frames. The caller pins the namespace/range and the transaction's catalog
/// view; this digest does not grant route or execution-lifetime authority.
pub fn catalogDigest(txn: anytype, namespace: @import("doc_identity_namespace.zig").Namespace, byte_range: @import("../byte_range.zig").ByteRange, original: ?control.Port) ![32]u8 {
    var trace: control.RejectionTrace = .{};
    const result = catalogDigestInner(txn, namespace, byte_range, trace.bind(original)) catch |cause| return trace.failure orelse cause;
    if (trace.failure) |cause| return cause;
    return result;
}
fn catalogDigestInner(txn: anytype, namespace: @import("doc_identity_namespace.zig").Namespace, byte_range: @import("../byte_range.zig").ByteRange, owner: ?control.Port) ![32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly-prepared-catalog-v1");
    for ([_]u64{ namespace.table_id, namespace.shard_id, namespace.range_id }) |id| {
        var raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &raw, id, .little);
        hash.update(&raw);
    }
    // Consume borrowed range bytes before any get can rotate a source buffer.
    for ([_][]const u8{ byte_range.start, byte_range.end, "\x00\x00__metadata__:indexes", "\x00\x00__metadata__:enrichments", "\x00\x00__metadata__:resolvers" }, 0..) |key, index| {
        if (owner) |port| try port.checkpoint();
        const bytes = if (index < 2) key else txn.get(key) catch |cause| if (cause == error.NotFound) "" else return cause;
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, bytes.len, .little);
        hash.update(&length);
        var offset: usize = 0;
        while (offset < bytes.len) {
            if (owner) |port| try port.checkpoint();
            const end = offset + @min(@as(usize, 4096), bytes.len - offset);
            hash.update(bytes[offset..end]);
            offset = end;
        }
    }
    if (owner) |port| try port.checkpoint();
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

const CatalogDigestTest = struct {
    const namespace: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 0x0102030405060708, .shard_id = 0x1112131415161718, .range_id = 0x2122232425262728 };
    const byte_range: @import("../byte_range.zig").ByteRange = .{ .start = "\x00a", .end = "\xff\x00" };
    const Txn = struct {
        rows: [3]?[]const u8 = .{ "\x00idx", null, "\xffcfg" },
        calls: usize = 0,
        rotate: ?[]u8 = null,
        failure: ?anyerror = null,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            self.calls += 1;
            if (self.rotate) |bytes| @memset(bytes, 'x');
            if (self.failure) |cause| return cause;
            for ([_][]const u8{ "\x00\x00__metadata__:indexes", "\x00\x00__metadata__:enrichments", "\x00\x00__metadata__:resolvers" }, 0..) |expected, i|
                if (std.mem.eql(u8, key, expected)) return self.rows[i] orelse error.NotFound;
            return error.TestUnexpectedResult;
        }
    };
};

test "prepared transaction effects catalog digest preserves independently framed v1 bytes and namespace endianness" {
    const F = CatalogDigestTest;
    const fixture = "antfly-prepared-catalog-v1" ++
        "\x08\x07\x06\x05\x04\x03\x02\x01" ++
        "\x18\x17\x16\x15\x14\x13\x12\x11" ++
        "\x28\x27\x26\x25\x24\x23\x22\x21" ++
        "\x02\x00\x00\x00\x00\x00\x00\x00\x00a" ++
        "\x02\x00\x00\x00\x00\x00\x00\x00\xff\x00" ++
        "\x04\x00\x00\x00\x00\x00\x00\x00\x00idx" ++
        "\x00\x00\x00\x00\x00\x00\x00\x00" ++
        "\x04\x00\x00\x00\x00\x00\x00\x00\xffcfg";
    var expected: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(fixture, &expected, .{});
    var txn: F.Txn = .{};
    try std.testing.expectEqualDeep(expected, try catalogDigest(&txn, F.namespace, F.byte_range, null));
    try std.testing.expectEqual(@as(usize, 3), txn.calls);
}

test "prepared transaction effects catalog digest binds every context field and consumes range before rotating metadata reads" {
    const F = CatalogDigestTest;
    var txn: F.Txn = .{};
    const expected = try catalogDigest(&txn, F.namespace, F.byte_range, null);
    inline for (.{ "table_id", "shard_id", "range_id" }) |field| {
        var changed = F.namespace;
        @field(changed, field) += 1;
        try std.testing.expect(!std.meta.eql(expected, try catalogDigest(&txn, changed, F.byte_range, null)));
    }
    for (0..3) |field| {
        txn = .{};
        txn.rows[field] = "replacement";
        try std.testing.expect(!std.meta.eql(expected, try catalogDigest(&txn, F.namespace, F.byte_range, null)));
    }
    txn = .{};
    txn.rows[1] = "";
    try std.testing.expectEqualDeep(expected, try catalogDigest(&txn, F.namespace, F.byte_range, null));
    const left = try catalogDigest(&txn, F.namespace, .{ .start = "ab", .end = "c" }, null);
    try std.testing.expect(!std.meta.eql(left, try catalogDigest(&txn, F.namespace, .{ .start = "a", .end = "bc" }, null)));
    var bounds = [_]u8{ 'a', 'z' };
    txn = .{};
    const before = try catalogDigest(&txn, F.namespace, .{ .start = bounds[0..1], .end = bounds[1..] }, null);
    txn.rotate = &bounds;
    try std.testing.expectEqualDeep(before, try catalogDigest(&txn, F.namespace, .{ .start = bounds[0..1], .end = bounds[1..] }, null));
}

test "prepared transaction effects catalog digest preserves every original checkpoint and metadata failure" {
    const F = CatalogDigestTest;
    const Checks = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.NotFound,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
        fn run(self: *@This()) ![32]u8 {
            const bytes: [12289]u8 = @splat(5);
            var txn: F.Txn = .{ .rows = .{ &bytes, null, "" } };
            var admission_payload: control.Payload = .{ .ptr = self, .vtable = &.{ .checkpoint = check } };
            return catalogDigest(&txn, F.namespace, F.byte_range, control.Port.init(&admission_payload));
        }
    };
    var baseline: Checks = .{};
    _ = try baseline.run();
    try std.testing.expect(baseline.calls > 8);
    for ([_]anyerror{ error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..baseline.calls + 1) |stop| {
        var checks: Checks = .{ .stop = stop, .cause = cause };
        try std.testing.expectError(cause, checks.run());
    };
    for ([_]anyerror{ error.OutOfMemory, error.DeadlineExceeded }) |cause| {
        var txn: F.Txn = .{ .failure = cause };
        try std.testing.expectError(cause, catalogDigest(&txn, F.namespace, F.byte_range, null));
    }
}

pub const Plan = struct { contract: Contract, effects: Effects };

// Layout follows the declared durable fields, little endian integers and float
// bits, u32 enum tags/counts, u8 optional discriminants. Field/order/type changes
// require a new envelope version. Request-local JSON roots are never durable.
const magic = "ATPE";
const version: u16 = 1;
const prefix_bytes = 12;
const checksum_bytes = 32;
pub const max_frame_bytes = 64 * 1024 * 1024;
pub const max_decoded_bytes = 256 * 1024 * 1024;
const quantum = 4096;

fn localOnly(comptime T: type, comptime name: []const u8) bool {
    return T == derived.DerivedDocument and
        (std.mem.eql(u8, name, "prepared_text_root") or std.mem.eql(u8, name, "prepared_text_source_bytes"));
}

fn reserveScratch(total: *usize, comptime T: type, count: usize) !void {
    if (count == 0) return;
    const size = std.math.mul(usize, @sizeOf(T), count) catch return error.TransactionTooLarge;
    const padded = std.math.add(usize, size, @alignOf(T) - 1) catch return error.TransactionTooLarge;
    total.* = std.math.add(usize, total.*, padded) catch return error.TransactionTooLarge;
    if (total.* > max_decoded_bytes) return error.TransactionTooLarge;
}

const Writer = struct {
    output: ?[]u8 = null,
    offset: usize = 0,
    scratch: usize = 0,
    owner: ?control.Port,

    fn checkpoint(self: *Writer) !void {
        if (self.owner) |owner| try owner.checkpoint();
    }
    fn bytes(self: *Writer, input: []const u8) !void {
        const end = std.math.add(usize, self.offset, input.len) catch return error.TransactionTooLarge;
        if (end > max_frame_bytes - checksum_bytes) return error.TransactionTooLarge;
        if (self.output) |out| {
            if (end > out.len) return error.InvalidPreparedEffects;
            var pos: usize = 0;
            while (pos < input.len) {
                try self.checkpoint();
                const n = @min(quantum, input.len - pos);
                @memcpy(out[self.offset + pos ..][0..n], input[pos..][0..n]);
                pos += n;
            }
        }
        self.offset = end;
    }
    fn int(self: *Writer, comptime T: type, input: T) !void {
        var raw: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &raw, input, .little);
        try self.bytes(&raw);
    }
    fn value(self: *Writer, comptime T: type, item: T) anyerror!void {
        try self.checkpoint();
        switch (@typeInfo(T)) {
            .int => try self.int(T, item),
            .float => try self.int(@Int(.unsigned, @bitSizeOf(T)), @bitCast(item)),
            .@"enum" => try self.int(u32, @intCast(@backingInt(item))),
            .optional => |info| {
                try self.int(u8, @intFromBool(item != null));
                if (item) |child| try self.value(info.child, child);
            },
            .array => |info| {
                if (info.child == u8) try self.bytes(&item) else for (item) |child| try self.value(info.child, child);
            },
            .pointer => |info| {
                if (info.size != .slice) @compileError("prepared effects require slices");
                const count = std.math.cast(u32, item.len) orelse return error.TransactionTooLarge;
                try reserveScratch(&self.scratch, info.child, count);
                try self.int(u32, count);
                if (info.child == u8) try self.bytes(item) else for (item) |child| try self.value(info.child, child);
            },
            .@"struct" => inline for (@typeInfo(T).@"struct".field_names, @typeInfo(T).@"struct".field_types) |name, Field| {
                if (!comptime localOnly(T, name)) try self.value(Field, @field(item, name));
            },
            else => @compileError("unsupported durable effects type " ++ @typeName(T)),
        }
    }
};

fn digest(data: []const u8, owner: ?control.Port) ![32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    var offset: usize = 0;
    while (offset < data.len) {
        if (owner) |port| try port.checkpoint();
        const end = offset + @min(quantum, data.len - offset);
        hash.update(data[offset..end]);
        offset = end;
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub fn encodedSize(plan: Plan, owner: ?control.Port) !usize {
    if (plan.contract.intent_revision == 0) return error.InvalidPreparedEffects;
    var writer: Writer = .{ .offset = prefix_bytes, .owner = owner };
    try writer.value(Plan, plan);
    return writer.offset + checksum_bytes;
}

pub fn encode(alloc: std.mem.Allocator, plan: Plan, owner: ?control.Port) ![]u8 {
    const size = try encodedSize(plan, owner);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    @memcpy(out[0..4], magic);
    std.mem.writeInt(u16, out[4..6], version, .little);
    std.mem.writeInt(u16, out[6..8], 0, .little);
    std.mem.writeInt(u32, out[8..12], @intCast(size), .little);
    var writer: Writer = .{ .output = out, .offset = prefix_bytes, .owner = owner };
    try writer.value(Plan, plan);
    if (writer.offset != size - checksum_bytes) return error.InvalidPreparedEffects;
    out[size - checksum_bytes ..][0..checksum_bytes].* = try digest(out[0 .. size - checksum_bytes], owner);
    return out;
}

const Reader = struct {
    data: []const u8,
    offset: usize = 0,
    scratch: usize = 0,
    allocator: ?std.mem.Allocator = null,
    owner: ?control.Port,

    fn checkpoint(self: *Reader) !void {
        if (self.owner) |owner| try owner.checkpoint();
    }
    fn bytes(self: *Reader, count: usize) ![]const u8 {
        if (count > self.data.len - self.offset) return error.InvalidPreparedEffects;
        const result = self.data[self.offset..][0..count];
        self.offset += count;
        return result;
    }
    fn int(self: *Reader, comptime T: type) !T {
        return std.mem.readInt(T, (try self.bytes(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
    fn value(self: *Reader, comptime T: type) anyerror!T {
        try self.checkpoint();
        return switch (@typeInfo(T)) {
            .int => try self.int(T),
            .float => @bitCast(try self.int(@Int(.unsigned, @bitSizeOf(T)))),
            .@"enum" => std.enums.fromInt(T, try self.int(u32)) orelse return error.InvalidPreparedEffects,
            .optional => |info| switch (try self.int(u8)) {
                0 => null,
                1 => try self.value(info.child),
                else => error.InvalidPreparedEffects,
            },
            .array => |info| blk: {
                var result: T = undefined;
                if (info.child == u8) @memcpy(&result, try self.bytes(info.len)) else for (&result) |*child| child.* = try self.value(info.child);
                break :blk result;
            },
            .pointer => |info| blk: {
                const count = try self.int(u32);
                // Every element has at least one encoded byte. Reject malicious
                // counts before allocating descriptor arrays or entering loops.
                if (count > self.data.len - self.offset) return error.InvalidPreparedEffects;
                try reserveScratch(&self.scratch, info.child, count);
                const out = if (self.allocator) |alloc| try alloc.alloc(info.child, count) else null;
                if (info.child == u8) {
                    const input = try self.bytes(count);
                    if (out) |dest| {
                        var pos: usize = 0;
                        while (pos < input.len) {
                            try self.checkpoint();
                            const n = @min(quantum, input.len - pos);
                            @memcpy(dest[pos..][0..n], input[pos..][0..n]);
                            pos += n;
                        }
                    }
                } else {
                    for (0..count) |i| {
                        const child = try self.value(info.child);
                        if (out) |dest| dest[i] = child;
                    }
                }
                break :blk out orelse &.{};
            },
            .@"struct" => blk: {
                var result: T = undefined;
                inline for (@typeInfo(T).@"struct".field_names, @typeInfo(T).@"struct".field_types) |name, Field| {
                    if (comptime localOnly(T, name)) {
                        @field(result, name) = if (Field == ?std.json.Value) null else 0;
                    } else @field(result, name) = try self.value(Field);
                }
                break :blk result;
            },
            else => @compileError("unsupported durable effects type " ++ @typeName(T)),
        };
    }
};

pub const Decoded = struct {
    alloc: std.mem.Allocator,
    backing: []u8,
    plan: Plan,
    /// All slices borrow this one backing allocation, not the encoded input.
    /// Do not call effects.deinit: this owner retires the whole plan once.
    pub fn deinit(self: *Decoded) void {
        self.alloc.free(self.backing);
        self.* = undefined;
    }
};

fn payload(frame: []const u8, owner: ?control.Port) ![]const u8 {
    if (owner) |port| try port.checkpoint();
    if (frame.len < prefix_bytes + checksum_bytes or frame.len > max_frame_bytes) return error.InvalidPreparedEffects;
    if (!std.mem.eql(u8, frame[0..4], magic) or std.mem.readInt(u16, frame[4..6], .little) != version) return error.UnsupportedVersion;
    if (std.mem.readInt(u16, frame[6..8], .little) != 0 or std.mem.readInt(u32, frame[8..12], .little) != frame.len) return error.InvalidPreparedEffects;
    const end = frame.len - checksum_bytes;
    const actual = try digest(frame[0..end], owner);
    if (!std.mem.eql(u8, &actual, frame[end..])) return error.InvalidPreparedEffects;
    return frame[prefix_bytes..end];
}

pub const Inspection = struct {
    contract: Contract,
    decoded_bytes: usize,
    fingerprint: [32]u8,
};

/// Validate the complete immutable frame without allocating. The fingerprint
/// covers header and body and is suitable for idempotent sealed-plan matching.
pub fn inspect(frame: []const u8, owner: ?control.Port) !Inspection {
    const data = try payload(frame, owner);
    var scan: Reader = .{ .data = data, .owner = owner };
    const contract = try scan.value(Contract);
    if (contract.intent_revision == 0) return error.InvalidPreparedEffects;
    _ = try scan.value(Effects);
    if (scan.offset != data.len) return error.InvalidPreparedEffects;
    return .{ .contract = contract, .decoded_bytes = scan.scratch, .fingerprint = frame[frame.len - checksum_bytes ..][0..checksum_bytes].* };
}

/// Validate all lengths/tags and measure bounded decode storage before the one
/// allocation. The caller's expected identity comes from durable intent state.
pub fn decode(alloc: std.mem.Allocator, frame: []const u8, expected: Contract, owner: ?control.Port) !Decoded {
    const inspection = try inspect(frame, owner);
    if (!std.meta.eql(inspection.contract, expected)) return error.PreparedEffectsMismatch;
    const data = frame[prefix_bytes .. frame.len - checksum_bytes];
    const backing = try alloc.alloc(u8, inspection.decoded_bytes);
    errdefer alloc.free(backing);
    var storage = std.heap.FixedBufferAllocator.init(backing);
    var reader: Reader = .{ .data = data, .owner = owner, .allocator = storage.allocator() };
    const plan = try reader.value(Plan);
    if (reader.offset != data.len) return error.InvalidPreparedEffects;
    return .{ .alloc = alloc, .backing = backing, .plan = plan };
}

/// Convert the decoded slab into the ordinary per-field ownership expected by
/// the mutation/replay pipeline. Charge every allocation to the caller's
/// preparation allocator; no pointer into Decoded survives this copy.
pub fn cloneForMaterialization(alloc: std.mem.Allocator, plan: Plan, owner: ?control.Port) !Effects {
    _ = try encodedSize(plan, owner); // exact traversal and decoded-storage bound
    var clone: Cloner = .{ .alloc = alloc, .owner = owner };
    return try clone.value(Effects, plan.effects);
}

const Cloner = struct {
    alloc: std.mem.Allocator,
    owner: ?control.Port,

    fn ownsSlices(comptime T: type) bool {
        return switch (@typeInfo(T)) {
            .pointer => true,
            .optional => |info| ownsSlices(info.child),
            .array => |info| ownsSlices(info.child),
            .@"struct" => blk: {
                inline for (@typeInfo(T).@"struct".field_names, @typeInfo(T).@"struct".field_types) |name, Field| {
                    if (!comptime localOnly(T, name)) if (comptime ownsSlices(Field)) break :blk true;
                }
                break :blk false;
            },
            else => false,
        };
    }

    fn release(self: *Cloner, comptime T: type, item: T) void {
        switch (@typeInfo(T)) {
            .optional => |info| if (item) |child| self.release(info.child, child),
            .pointer => |info| {
                if (comptime ownsSlices(info.child)) for (item) |child| self.release(info.child, child);
                self.alloc.free(item);
            },
            .array => |info| if (comptime ownsSlices(info.child)) {
                for (item) |child| self.release(info.child, child);
            },
            .@"struct" => inline for (@typeInfo(T).@"struct".field_names, @typeInfo(T).@"struct".field_types) |name, Field| {
                if (!comptime localOnly(T, name)) self.release(Field, @field(item, name));
            },
            else => {},
        }
    }
    fn value(self: *Cloner, comptime T: type, item: T) anyerror!T {
        if (self.owner) |owner| try owner.checkpoint();
        return switch (@typeInfo(T)) {
            .optional => |info| if (item) |child| try self.value(info.child, child) else null,
            .pointer => |info| blk: {
                const result = try self.alloc.alloc(info.child, item.len);
                var initialized: usize = 0;
                errdefer {
                    if (comptime ownsSlices(info.child)) for (result[0..initialized]) |child| self.release(info.child, child);
                    self.alloc.free(result);
                }
                if (info.child == u8) {
                    var offset: usize = 0;
                    while (offset < item.len) {
                        if (self.owner) |owner| try owner.checkpoint();
                        const end = offset + @min(quantum, item.len - offset);
                        @memcpy(result[offset..end], item[offset..end]);
                        offset = end;
                    }
                } else for (item, result) |child, *destination| {
                    destination.* = try self.value(info.child, child);
                    initialized += 1;
                }
                break :blk result;
            },
            .@"struct" => blk: {
                var result: T = undefined;
                var initialized: usize = 0;
                errdefer inline for (@typeInfo(T).@"struct".field_names, @typeInfo(T).@"struct".field_types, 0..) |name, Field, index| {
                    if (index < initialized) {
                        if (!comptime localOnly(T, name)) self.release(Field, @field(result, name));
                    }
                };
                inline for (@typeInfo(T).@"struct".field_names, @typeInfo(T).@"struct".field_types) |name, Field| {
                    @field(result, name) = if (comptime localOnly(T, name)) (if (Field == usize) 0 else null) else try self.value(Field, @field(item, name));
                    initialized += 1;
                }
                break :blk result;
            },
            else => item,
        };
    }
};

test "prepared transaction effects materialization clone has independent per-field ownership" {
    const a = std.testing.allocator;
    const frame = try encode(a, testPlan(), null);
    var decoded = try decode(a, frame, testContract(), null);
    a.free(frame);
    var result = try cloneForMaterialization(a, decoded.plan, null);
    decoded.deinit();
    defer result.deinit(a);
    const roundtrip = try encode(a, .{ .contract = testContract(), .effects = result }, null);
    defer a.free(roundtrip);
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(roundtrip, &expected_hash, .{});
    try std.testing.expectEqualStrings("6e47d6f7c9058a6ca7e5245a62a0086ec69fe7d159269bb5c65e63b3f945f387", &std.fmt.bytesToHex(expected_hash, .lower));
    try std.testing.expect(result.documents[0].prepared_text_root == null);
}

test "prepared transaction effects materialization clone releases every failed allocation" {
    const Harness = struct {
        fn run(a: std.mem.Allocator) !void {
            var result = try cloneForMaterialization(a, testPlan(), null);
            defer result.deinit(a);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

fn testContract() Contract {
    return .{ .txn_id = @splat(7), .intent_revision = 3, .intent_digest = @splat(11), .catalog_digest = @splat(13), .schema_version = 19, .mode = .synchronous };
}

fn testPlan() Plan {
    return .{ .contract = testContract(), .effects = .{
        .artifact_writes = @constCast(&[_]@import("types.zig").BatchWrite{.{ .key = "doc\x00artifact", .value = "\xff\x00payload" }}),
        .artifact_delete_keys = &.{ "old\x00key", "" },
        .documents = &.{.{
            .key = "doc",
            .cleaned_value = "{}",
            .targets = &.{.{ .kind = .full_text, .index_name = "text" }},
            .prepared_text_root = .{ .integer = 42 },
            .prepared_text_source_bytes = 64,
            .prepared_schema_version = 19,
            .prepared_write_plan_generation = 23,
        }},
        .dense_embeddings = &.{.{ .index_name = "dense", .parent_doc_key = "parent", .doc_key = "doc", .artifact_key = "vector", .vector = &.{ 1.5, -0.0, -2.25 } }},
        .sparse_embeddings = &.{.{ .index_name = "sparse", .doc_key = "doc", .indices = &.{ 1, 9 }, .values = &.{ 0.25, 2.0 } }},
        .generated_enrichment_refs = &.{.{ .kind = .asset, .index_name = "consumer", .artifact_name = "summary", .embedding_name = "", .doc_key = "doc" }},
        .coverage_outcomes = @constCast(&[_]effect_types.PrecomputedCoverageOutcome{.{ .index_name = @constCast("consumer"), .doc_key = @constCast("doc"), .outcome = .skipped }}),
    } };
}

test "prepared transaction effects retain all durable families independently of input lifetime" {
    const a = std.testing.allocator;
    const plan = testPlan();
    const encoded = try encode(a, plan, null);
    try std.testing.expectEqual(try encodedSize(plan, null), encoded.len);
    var fixture_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(encoded, &fixture_hash, .{});
    try std.testing.expectEqualStrings("6e47d6f7c9058a6ca7e5245a62a0086ec69fe7d159269bb5c65e63b3f945f387", &std.fmt.bytesToHex(fixture_hash, .lower));
    var decoded = try decode(a, encoded, plan.contract, null);
    defer decoded.deinit();
    // Decoded slices must survive release/overwrite of the original frame.
    @memset(encoded, 0xaa);
    a.free(encoded);
    try std.testing.expectEqualDeep(plan.contract, decoded.plan.contract);
    try std.testing.expectEqualDeep(plan.effects.artifact_writes, decoded.plan.effects.artifact_writes);
    try std.testing.expectEqualDeep(plan.effects.artifact_delete_keys, decoded.plan.effects.artifact_delete_keys);
    const doc = decoded.plan.effects.documents[0];
    try std.testing.expectEqual(@as(?std.json.Value, null), doc.prepared_text_root);
    try std.testing.expectEqual(@as(usize, 0), doc.prepared_text_source_bytes);
    try std.testing.expectEqual(plan.effects.documents[0].prepared_schema_version, doc.prepared_schema_version);
    try std.testing.expectEqual(plan.effects.documents[0].prepared_write_plan_generation, doc.prepared_write_plan_generation);
    try std.testing.expectEqualDeep(plan.effects.documents[0].targets, doc.targets);
    try std.testing.expectEqualDeep(plan.effects.dense_embeddings, decoded.plan.effects.dense_embeddings);
    try std.testing.expectEqualDeep(plan.effects.sparse_embeddings, decoded.plan.effects.sparse_embeddings);
    try std.testing.expectEqualDeep(plan.effects.generated_enrichment_refs, decoded.plan.effects.generated_enrichment_refs);
    try std.testing.expectEqualDeep(plan.effects.coverage_outcomes, decoded.plan.effects.coverage_outcomes);
    const reencoded = try encode(a, decoded.plan, null);
    defer a.free(reencoded);
    const original = try encode(a, plan, null);
    defer a.free(original);
    try std.testing.expectEqualSlices(u8, original, reencoded);
    var deferred = testContract();
    deferred.mode = .deferred;
    deferred.schema_version = null;
    const empty_frame = try encode(a, .{ .contract = deferred, .effects = .{} }, null);
    defer a.free(empty_frame);
    var empty_plan = try decode(a, empty_frame, deferred, null);
    defer empty_plan.deinit();
    try std.testing.expectEqualDeep(deferred, empty_plan.plan.contract);
    try std.testing.expectEqual(@as(usize, 0), empty_plan.plan.effects.artifact_writes.len);
}

test "prepared transaction effects reject corruption identity and malicious counts before allocation" {
    const a = std.testing.allocator;
    const frame = try encode(a, .{ .contract = testContract(), .effects = .{} }, null);
    defer a.free(frame);
    var no_memory: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&no_memory);
    for (0..frame.len) |n| try std.testing.expectError(error.InvalidPreparedEffects, decode(fixed.allocator(), frame[0..n], testContract(), null));
    var wrong = testContract();
    wrong.txn_id[0] ^= 1;
    try std.testing.expectError(error.PreparedEffectsMismatch, decode(fixed.allocator(), frame, wrong, null));
    wrong = testContract();
    wrong.intent_revision += 1;
    try std.testing.expectError(error.PreparedEffectsMismatch, decode(fixed.allocator(), frame, wrong, null));
    wrong = testContract();
    wrong.intent_digest[0] ^= 1;
    try std.testing.expectError(error.PreparedEffectsMismatch, decode(fixed.allocator(), frame, wrong, null));
    wrong = testContract();
    wrong.catalog_digest[0] ^= 1;
    try std.testing.expectError(error.PreparedEffectsMismatch, decode(fixed.allocator(), frame, wrong, null));
    wrong = testContract();
    wrong.schema_version = null;
    try std.testing.expectError(error.PreparedEffectsMismatch, decode(fixed.allocator(), frame, wrong, null));
    wrong = testContract();
    wrong.mode = .deferred;
    try std.testing.expectError(error.PreparedEffectsMismatch, decode(fixed.allocator(), frame, wrong, null));
    frame[4] = 2;
    try std.testing.expectError(error.UnsupportedVersion, decode(a, frame, testContract(), null));
    frame[4] = 1;
    frame[frame.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidPreparedEffects, decode(a, frame, testContract(), null));
    frame[frame.len - 1] ^= 1;
    // Re-signed but malformed tags must fail independently of checksums.
    const optional_offset = prefix_bytes + 16 + 8 + 32 + 32;
    frame[optional_offset] = 2;
    frame[frame.len - checksum_bytes ..][0..checksum_bytes].* = try digest(frame[0 .. frame.len - checksum_bytes], null);
    try std.testing.expectError(error.InvalidPreparedEffects, decode(fixed.allocator(), frame, testContract(), null));
    frame[optional_offset] = 1;
    const mode_offset = optional_offset + 1 + 4;
    std.mem.writeInt(u32, frame[mode_offset..][0..4], std.math.maxInt(u32), .little);
    frame[frame.len - checksum_bytes ..][0..checksum_bytes].* = try digest(frame[0 .. frame.len - checksum_bytes], null);
    try std.testing.expectError(error.InvalidPreparedEffects, decode(fixed.allocator(), frame, testContract(), null));
    std.mem.writeInt(u32, frame[mode_offset..][0..4], 1, .little);
    var count: Writer = .{ .offset = prefix_bytes, .owner = null };
    try count.value(Contract, testContract());
    std.mem.writeInt(u32, frame[count.offset..][0..4], std.math.maxInt(u32), .little);
    frame[frame.len - checksum_bytes ..][0..checksum_bytes].* = try digest(frame[0 .. frame.len - checksum_bytes], null);
    try std.testing.expectError(error.InvalidPreparedEffects, decode(fixed.allocator(), frame, testContract(), null));
}

test "prepared transaction effects retire every allocation failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const frame = try encode(a, testPlan(), null);
            defer a.free(frame);
            var decoded = try decode(a, frame, testContract(), null);
            defer decoded.deinit();
            try std.testing.expectEqualDeep(testPlan().effects.coverage_outcomes, decoded.plan.effects.coverage_outcomes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "prepared transaction effects preserve original control through copy hash and decode" {
    const a = std.testing.allocator;
    const large = try a.alloc(u8, 1024 * 1024);
    defer a.free(large);
    @memset(large, 37);
    const plan: Plan = .{ .contract = testContract(), .effects = .{ .artifact_writes = @constCast(&[_]@import("types.zig").BatchWrite{.{ .key = "key", .value = large }}) } };
    const Control = struct {
        checks: usize = 0,
        reject: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject) return error.MemoryLimitExceeded;
        }
    };
    var c: Control = .{};
    const payload_control: control.Payload = .{ .ptr = &c, .vtable = &.{ .checkpoint = Control.check } };
    const port = control.Port.init(&payload_control);
    const frame = try encode(a, plan, port);
    defer a.free(frame);
    const encode_checks = c.checks;
    try std.testing.expect(encode_checks > 512);
    for ([_]usize{ 1, 32, 128, 256, encode_checks }) |reject| {
        c = .{ .reject = reject };
        try std.testing.expectError(error.MemoryLimitExceeded, encode(a, plan, port));
        try std.testing.expectEqual(reject, c.checks);
    }
    c = .{};
    var decoded = try decode(a, frame, plan.contract, port);
    defer decoded.deinit();
    const decode_checks = c.checks;
    try std.testing.expect(decode_checks > 512);
    for ([_]usize{ 1, 32, 128, 256, decode_checks }) |reject| {
        c = .{ .reject = reject };
        try std.testing.expectError(error.MemoryLimitExceeded, decode(a, frame, plan.contract, port));
        try std.testing.expectEqual(reject, c.checks);
    }
}

test "prepared transaction effects requirement metadata rejects corruption versions and tags" {
    for (std.enums.values(Mode)) |mode| {
        var raw = (Requirement{ .mode = mode }).encode();
        try std.testing.expectEqual(mode, (try Requirement.decode(&raw)).mode);
        for (0..raw.len) |n| try std.testing.expectError(error.InvalidPreparationRequirement, Requirement.decode(raw[0..n]));
        raw[4] = 2;
        try std.testing.expectError(error.UnsupportedVersion, Requirement.decode(&raw));
        raw[4] = 1;
        raw[39] ^= 1;
        try std.testing.expectError(error.InvalidPreparationRequirement, Requirement.decode(&raw));
        raw[39] ^= 1;
        raw[7] = 255;
        std.crypto.hash.Blake3.hash(raw[0..8], raw[8..40], .{});
        try std.testing.expectError(error.InvalidPreparationRequirement, Requirement.decode(&raw));
        raw[7] = @backingInt(mode);
        raw[6] = 1;
        std.crypto.hash.Blake3.hash(raw[0..8], raw[8..40], .{});
        try std.testing.expectError(error.InvalidPreparationRequirement, Requirement.decode(&raw));
    }
}

test "prepared transaction effects materialization clone preserves every original rejection" {
    const Control = struct {
        checks: usize = 0,
        reject: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject) return error.NotFound;
        }
    };
    const a = std.testing.allocator;
    var c: Control = .{};
    const payload_control: control.Payload = .{ .ptr = &c, .vtable = &.{ .checkpoint = Control.check } };
    const port = control.Port.init(&payload_control);
    var result = try cloneForMaterialization(a, testPlan(), port);
    result.deinit(a);
    const checks = c.checks;
    for (1..checks + 1) |reject| {
        c = .{ .reject = reject };
        try std.testing.expectError(error.NotFound, cloneForMaterialization(a, testPlan(), port));
        try std.testing.expectEqual(reject, c.checks);
    }
}
