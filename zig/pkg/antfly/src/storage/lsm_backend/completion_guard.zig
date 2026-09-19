// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Bounded native cohort sidecar. Legacy raw descriptors remain readable.
const std = @import("std");
pub const max_slots = 4;
pub const header_bytes = 80;
const magic = "AFCSSET2";
pub const Info = struct {
    index: u8 = 0,
    base_run_id: u64,
    initial_runs: u32,
    cohort_id: [16]u8,
    legacy: bool = false,
    pub fn capacity(self: Info) usize {
        return if (self.legacy) 1 else max_slots;
    }
};
pub const Decoded = struct { info: Info, descriptor: []const u8 };
pub fn encode(alloc: std.mem.Allocator, info: Info, descriptor: []const u8) ![]u8 {
    if (info.index >= max_slots or info.initial_runs > 64 or info.base_run_id == 0) return error.InvalidCompletionSlot;
    const out = try alloc.alloc(u8, header_bytes + descriptor.len);
    @memset(out[0..header_bytes], 0);
    @memcpy(out[0..8], magic);
    out[8] = info.index;
    std.mem.writeInt(u32, out[12..16], info.initial_runs, .little);
    std.mem.writeInt(u64, out[16..24], info.base_run_id, .little);
    @memcpy(out[24..40], &info.cohort_id);
    std.mem.writeInt(u32, out[40..44], @intCast(descriptor.len), .little);
    @memcpy(out[header_bytes..], descriptor);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(out[0..48]);
    hash.update(descriptor);
    hash.final(out[48..80]);
    return out;
}
pub fn decode(bytes: []const u8, legacy_base: u64, legacy_runs: usize) !Decoded {
    if (std.mem.startsWith(u8, bytes, "AFCSLOT\x00")) return .{ .info = .{ .base_run_id = legacy_base, .initial_runs = @intCast(@min(legacy_runs, 64)), .cohort_id = @splat(0), .legacy = true }, .descriptor = bytes };
    if (bytes.len < header_bytes or !std.mem.eql(u8, bytes[0..8], magic) or bytes[8] >= max_slots or
        std.mem.readInt(u32, bytes[40..44], .little) != bytes.len - header_bytes) return error.InvalidCompletionSlot;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(bytes[0..48]);
    hash.update(bytes[header_bytes..]);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    if (!std.mem.eql(u8, &digest, bytes[48..80])) return error.CompletionSlotChecksumMismatch;
    const info: Info = .{ .index = bytes[8], .initial_runs = std.mem.readInt(u32, bytes[12..16], .little), .base_run_id = std.mem.readInt(u64, bytes[16..24], .little), .cohort_id = bytes[24..40].* };
    if (info.initial_runs > 64 or info.base_run_id == 0 or info.base_run_id > std.math.maxInt(u64) - max_slots) return error.InvalidCompletionSlot;
    return .{ .info = info, .descriptor = bytes[header_bytes..] };
}

test "workload admission completion cohort guard binds output range and detects corruption" {
    const info: Info = .{ .index = 2, .initial_runs = 64, .base_run_id = 1000, .cohort_id = @splat(73) };
    const wire = try encode(std.testing.allocator, info, "descriptor\x00bytes");
    defer std.testing.allocator.free(wire);
    const decoded = try decode(wire, 1, 0);
    try std.testing.expectEqualDeep(info, decoded.info);
    try std.testing.expectEqualStrings("descriptor\x00bytes", decoded.descriptor);
    wire[16] ^= 1;
    try std.testing.expectError(error.CompletionSlotChecksumMismatch, decode(wire, 1, 0));
    wire[16] ^= 1;
    wire[wire.len - 1] ^= 1;
    try std.testing.expectError(error.CompletionSlotChecksumMismatch, decode(wire, 1, 0));
    try std.testing.expectError(error.InvalidCompletionSlot, decode(wire[0 .. wire.len - 1], 1, 0));
}

test "workload admission completion cohort guard retains legacy single slot boundary" {
    const legacy = "AFCSLOT\x00opaque-descriptor-validated-by-caller";
    const decoded = try decode(legacy, 71, 65);
    try std.testing.expect(decoded.info.legacy);
    try std.testing.expectEqual(@as(usize, 1), decoded.info.capacity());
    try std.testing.expectEqual(@as(u64, 71), decoded.info.base_run_id);
    try std.testing.expectEqualStrings(legacy, decoded.descriptor);
}
