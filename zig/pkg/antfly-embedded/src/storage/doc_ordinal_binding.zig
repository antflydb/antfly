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

//! Snapshot-owned native document identities, independent of posting bytes.
//! The column uses the existing ordinal section encoding. Native publication
//! seals its fixed buffer after authoritative identity assignment, before any
//! reader can observe the new snapshot.
const std = @import("std");
const values = @import("native_publication_value.zig");
const source_mod = @import("../segment_source.zig");
const resources = @import("resource_manager.zig");
const A = std.mem.Allocator;

pub fn encodedLength(count: u32) !usize {
    return std.math.add(usize, 5, try std.math.mul(usize, count, 4));
}

pub fn validate(column: []const u8, count: u32) !void {
    try validateHeader(column, count);
    for (0..count) |i| if (std.mem.readInt(u32, column[5 + i * 4 ..][0..4], .big) == 0) return error.InvalidSegment;
}

fn validateHeader(column: []const u8, count: u32) !void {
    if (column.len != try encodedLength(count) or column[0] != 1 or std.mem.readInt(u32, column[1..5], .big) != count) return error.InvalidSegment;
}

pub fn encodeInto(column: []u8, ordinals: []const u32) !void {
    const count = std.math.cast(u32, ordinals.len) orelse return error.InvalidSegment;
    if (column.len != try encodedLength(count)) return error.InvalidSegment;
    // Refuse the whole binding before changing any prepared bytes.
    for (ordinals) |ordinal| if (ordinal == 0) return error.InvalidSegment;
    column[0] = 1;
    std.mem.writeInt(u32, column[1..5], count, .big);
    for (ordinals, 0..) |ordinal, i| std.mem.writeInt(u32, column[5 + i * 4 ..][0..4], ordinal, .big);
}

pub const Binding = struct {
    value: values.Value,
    offset: usize,
    count: u32,

    /// The producer initializes the fixed column header before capture and
    /// validates every authoritative ordinal before sealing the enclosing value.
    pub fn capturePrepared(value: values.Value, offset: usize, count: u32) !Binding {
        const length = try encodedLength(count);
        if (offset > value.initialization_view.len or length > value.initialization_view.len - offset) return error.InvalidSegment;
        try validateHeader(value.initialization_view[offset..][0..length], count);
        return .{ .value = value.retained(), .offset = offset, .count = count };
    }
    pub fn captureSealed(value: values.Value, offset: usize, count: u32) !Binding {
        const raw = try value.read();
        const length = try encodedLength(count);
        if (offset > raw.len or length > raw.len - offset) return error.InvalidSegment;
        try validate(raw[offset..][0..length], count);
        return capturePrepared(value, offset, count);
    }
    pub fn retained(self: Binding) Binding {
        return .{ .value = self.value.retained(), .offset = self.offset, .count = self.count };
    }
    pub fn deinit(self: *Binding) void {
        self.value.deinit();
        self.* = undefined;
    }
    pub fn column(self: *const Binding) ![]const u8 {
        const raw = try self.value.read();
        const length = try encodedLength(self.count);
        if (self.offset > raw.len or length > raw.len - self.offset) return error.InvalidSegment;
        const result = raw[self.offset..][0..length];
        try validateHeader(result, self.count);
        return result;
    }
    pub fn ordinal(self: *const Binding, doc: u32) !?u32 {
        if (doc >= self.count) return null;
        const raw = try self.column();
        const result = std.mem.readInt(u32, raw[5 + @as(usize, doc) * 4 ..][0..4], .big);
        if (result == 0) return error.InvalidSegment;
        return result;
    }
};

const SourceOwner = struct {
    alloc: A,
    source: source_mod.Source,
    references: std.atomic.Value(usize) = .init(1),
    fn retain(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0);
    }
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const a = self.alloc;
        var source = self.source;
        a.destroy(self);
        source.close();
    }
};

/// Decode metadata into an independent, sealed snapshot column. A pooled source
/// can belong to an older runtime; its accounting owner, rather than the new
/// decoder's DB, owns this column's quota and survives until its final release.
pub fn copySealed(a: A, source: source_mod.Source, column: []const u8, count: u32) !Binding {
    try validate(column, count);
    const manager = if (source == .ranges) source.ranges.resource_manager else null;
    var builder = if (manager) |rm| charged: {
        const lease = try a.create(SourceOwner);
        const retained = source.retainOwned() catch |err| {
            a.destroy(lease);
            return err;
        };
        lease.* = .{ .alloc = a, .source = retained };
        defer SourceOwner.release(lease);
        const charge = try values.Builder.budgetedBytes(resources.Reservation, column.len);
        var credit = try rm.reserveWithoutReclaim(.lite_docstore_snapshot_cache, try std.math.add(usize, charge, @sizeOf(SourceOwner)));
        errdefer credit.release();
        break :charged try values.Builder.createBudgeted(a, column.len, .{ .ptr = lease, .retain_fn = SourceOwner.retain, .release_fn = SourceOwner.release }, credit);
    } else try values.Builder.create(a, column.len);
    defer builder.deinit();
    try builder.write(0, column);
    try builder.seal();
    var value = builder.value();
    defer value.deinit();
    return Binding.captureSealed(value, 0, count);
}

test "native document ordinal binding requires its seal and keeps authoritative values after producer close" {
    const a = std.testing.allocator;
    var builder = try values.Builder.create(a, 17);
    var builder_live = true;
    defer if (builder_live) builder.deinit();
    const raw = try builder.bytes();
    raw[0] = 1;
    std.mem.writeInt(u32, raw[1..5], 3, .big);
    var value = builder.value();
    defer value.deinit();
    var binding = try Binding.capturePrepared(value, 0, 3);
    defer binding.deinit();
    try std.testing.expectError(error.NativePublicationValueNotBound, binding.ordinal(0));
    try encodeInto(raw, &.{ 42, 7, 900 });
    try builder.seal();
    builder.deinit();
    builder_live = false;
    try std.testing.expectEqual(@as(?u32, 42), try binding.ordinal(0));
    try std.testing.expectEqual(@as(?u32, 7), try binding.ordinal(1));
    try std.testing.expectEqual(@as(?u32, 900), try binding.ordinal(2));
    try std.testing.expect(try binding.ordinal(3) == null);
}

test "native document ordinal binding rejects incomplete identities and malformed columns" {
    var bytes: [13]u8 = @splat(0x5a);
    const original = bytes;
    try std.testing.expectError(error.InvalidSegment, encodeInto(&bytes, &.{ 1, 0 }));
    try std.testing.expectEqualSlices(u8, &original, &bytes);
    try encodeInto(&bytes, &.{ 10, 20 });
    try validate(&bytes, 2);
    try std.testing.expectError(error.InvalidSegment, validate(bytes[0..12], 2));
    try std.testing.expectError(error.InvalidSegment, validate(&bytes, 1));
    bytes[9..13].* = @splat(0);
    try std.testing.expectError(error.InvalidSegment, validate(&bytes, 2));
}
