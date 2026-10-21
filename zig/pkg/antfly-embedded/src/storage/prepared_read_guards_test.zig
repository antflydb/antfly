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

const std = @import("std");
const guards = @import("prepared_read_guards.zig");
const admission = @import("../common/read_admission.zig");
const A = std.mem.Allocator;
const Fixture = struct {
    fn record(a: A) guards.Record {
        return .{
            .receipt = .{ .txn_id = @splat(7), .requirement = .{ .mode = .deferred }, .binding = .{ .group_id = 1, .topology_epoch = 2, .participant_digest = @splat(9), .route_digest = @splat(11) }, .intent_revision = 1, .intent_digest = @splat(19), .catalog_digest = @splat(21), .frame_digest = @splat(17) },
            .namespace = @splat(13),
            .observations_digest = @splat(15),
            .predicates = .{ .arena = std.heap.ArenaAllocator.init(a), .spans = &.{.{ .lower = "\x01a", .upper = "\x01c", .lower_inclusive = true, .upper_inclusive = false }}, .membership_spans = &.{.{ .lower = "\x01m", .upper = "\x01z", .lower_inclusive = true, .upper_inclusive = false }} },
        };
    }
    fn rehash(raw: []u8) void {
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
    }
    fn exercise(a: A, control: ?admission.Port) !void {
        var source = record(a);
        defer source.deinit();
        const raw = try guards.encode(a, &source, control);
        defer a.free(raw);
        var decoded = try guards.decode(a, raw, control);
        defer decoded.deinit();
        try std.testing.expectEqualDeep(source.receipt, decoded.receipt);
        try std.testing.expect(try decoded.predicates.conflictsMutation("\x01b", true, true, control));
        try std.testing.expect(!try decoded.predicates.conflictsMutation("\x01n", true, true, control));
        try std.testing.expect(try decoded.predicates.conflictsMutation("\x01n", false, true, control));
        try std.testing.expect(!try decoded.predicates.conflicts("\x01z", control));
    }
    fn allocations(a: A) !void {
        try exercise(a, null);
    }
};

test "prepared transaction effects semantic registry codec has an independently framed golden binding and owned intervals" {
    const a = std.testing.allocator;
    var source = Fixture.record(a);
    defer source.deinit();
    const raw = try guards.encode(a, &source, null);
    var live = true;
    defer if (live) a.free(raw);
    try std.testing.expectEqual(@as(usize, 335), raw.len);
    try std.testing.expectEqualSlices(u8, "PRGR", raw[0..4]);
    try std.testing.expectEqual(@as(u32, 335), std.mem.readInt(u32, raw[8..12], .little));
    const golden = [_]u8{ 0x66, 0x72, 0x10, 0x8c, 0xec, 0x17, 0x12, 0x12, 0x6d, 0xa3, 0xf7, 0x56, 0x86, 0x0c, 0xa0, 0xef, 0x86, 0xcd, 0xa4, 0x3c, 0x9c, 0x95, 0x98, 0x8c, 0x40, 0x96, 0xc6, 0xe6, 0x44, 0x0c, 0xa0, 0xa3 };
    try std.testing.expectEqualSlices(u8, &golden, raw[303..335]);
    var decoded = try guards.decode(a, raw, null);
    defer decoded.deinit();
    a.free(raw);
    live = false;
    try std.testing.expectEqualDeep(source.receipt, decoded.receipt);
    try std.testing.expectEqualDeep(source.namespace, decoded.namespace);
    try std.testing.expectEqualDeep(source.observations_digest, decoded.observations_digest);
    try std.testing.expectEqualStrings("\x01a", decoded.predicates.spans[0].lower);
    try std.testing.expect(!try decoded.predicates.conflictsMutation("\x01n", true, true, null));
    try std.testing.expect(try decoded.predicates.conflictsMutation("\x01n", true, false, null));
}

test "prepared transaction effects semantic registry codec rejects every byte corruption truncation and impossible header before allocation" {
    const a = std.testing.allocator;
    var source = Fixture.record(a);
    defer source.deinit();
    const raw = try guards.encode(a, &source, null);
    defer a.free(raw);
    var fixed = std.heap.FixedBufferAllocator.init(&.{});
    for (0..raw.len) |offset| {
        raw[offset] ^= 1;
        try std.testing.expectError(error.InvalidTxnRecord, guards.decode(fixed.allocator(), raw, null));
        raw[offset] ^= 1;
    }
    for (0..raw.len) |length| try std.testing.expectError(error.InvalidTxnRecord, guards.decode(fixed.allocator(), raw[0..length], null));
    for ([_]usize{ 6, 52, 269, 273, 277 }) |offset| {
        const old = raw[offset];
        raw[offset] = 255;
        Fixture.rehash(raw);
        try std.testing.expectError(error.InvalidTxnRecord, guards.decode(fixed.allocator(), raw, null));
        raw[offset] = old;
        Fixture.rehash(raw);
    }
    raw[4] = 2;
    Fixture.rehash(raw);
    try std.testing.expectError(error.UnsupportedVersion, guards.decode(fixed.allocator(), raw, null));
}

test "prepared transaction effects semantic registry codec rejects unsafe control intervals and noncanonical unions" {
    const a = std.testing.allocator;
    var source = Fixture.record(a);
    defer source.deinit();
    var fixed = std.heap.FixedBufferAllocator.init(&.{});
    source.predicates.spans = &.{.{ .lower = "", .upper = "\x02", .lower_inclusive = true, .upper_inclusive = false }};
    try std.testing.expectError(error.Unsupported, guards.encode(fixed.allocator(), &source, null));
    source.predicates.spans = &.{.{ .lower = "\x01", .upper = null, .lower_inclusive = true, .upper_inclusive = false }};
    try std.testing.expectError(error.Unsupported, guards.encode(fixed.allocator(), &source, null));
    source.predicates.spans = &.{.{ .lower = "\x01z", .upper = "\x01a", .lower_inclusive = true, .upper_inclusive = false }};
    try std.testing.expectError(error.InvalidTxnRecord, guards.encode(fixed.allocator(), &source, null));
    source.predicates.spans = &.{
        .{ .lower = "\x01a", .upper = "\x01c", .lower_inclusive = true, .upper_inclusive = true },
        .{ .lower = "\x01c", .upper = "\x01d", .lower_inclusive = false, .upper_inclusive = false },
    };
    try std.testing.expectError(error.InvalidTxnRecord, guards.encode(fixed.allocator(), &source, null));
}

test "prepared transaction effects semantic registry codec unwinds all allocations and preserves original admission causes" {
    const a = std.testing.allocator;
    try std.testing.checkAllAllocationFailures(a, Fixture.allocations, .{});
    const Checks = struct {
        calls: usize = 0,
        stop: usize = 0,
        cause: anyerror = error.Unsupported,
        fn checkpoint(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
        fn run(checks: *@This()) !void {
            var payload: admission.Payload = .{ .ptr = checks, .vtable = &.{ .checkpoint = checkpoint } };
            try Fixture.exercise(std.testing.allocator, admission.Port.init(&payload));
        }
    };
    var baseline: Checks = .{};
    try baseline.run();
    for ([_]anyerror{ error.Unsupported, error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..baseline.calls + 1) |stop| {
        var checks: Checks = .{ .stop = stop, .cause = cause };
        try std.testing.expectError(cause, checks.run());
    };
}
