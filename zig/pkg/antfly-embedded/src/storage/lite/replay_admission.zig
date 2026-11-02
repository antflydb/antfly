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

//! File-wide durable admission, maintained by the physical document writer.
//! Includes every namespace and hint lane; cursor/consumer state does not own
//! these credits. Deleting a retired record returns its exact physical charge.
const std = @import("std");
pub const reservation_prefix = "\x00\x00__txn_intent_admission__:";
pub const coordinator_key = "\x00antfly/embedded/sql-commit/v1";
pub const key = "\x00antfly/lite/replay-admission/v8";
pub const Limits = struct {
    bytes: u64 = 16 * 1024 * 1024,
    records: u64 = 4096,
};
pub const State = struct {
    bytes: u64 = 0,
    records: u64 = 0,
    limits: Limits = .{},

    pub fn encode(self: State) [40]u8 {
        var raw: [40]u8 = @splat(0);
        @memcpy(raw[0..4], "RAB1");
        std.mem.writeInt(u64, raw[4..12], self.bytes, .little);
        std.mem.writeInt(u64, raw[12..20], self.records, .little);
        std.mem.writeInt(u64, raw[20..28], self.limits.bytes, .little);
        std.mem.writeInt(u64, raw[28..36], self.limits.records, .little);
        std.mem.writeInt(u32, raw[36..40], std.hash.Crc32.hash(raw[0..36]), .little);
        return raw;
    }
    pub fn decode(raw: []const u8) !State {
        if (raw.len != 40 or !std.mem.eql(u8, raw[0..4], "RAB1") or
            std.mem.readInt(u32, raw[36..40], .little) != std.hash.Crc32.hash(raw[0..36])) return error.InvalidNativeReplayAdmission;
        return .{ .bytes = std.mem.readInt(u64, raw[4..12], .little), .records = std.mem.readInt(u64, raw[12..20], .little), .limits = .{ .bytes = std.mem.readInt(u64, raw[20..28], .little), .records = std.mem.readInt(u64, raw[28..36], .little) } };
    }
    pub fn checkIncrease(before: State, after: State, limits: Limits) !void {
        // Consumer retirement and control-only transactions always retain
        // admission, including when reopening an older over-limit database.
        // One oversized atomic source record must still be able to make
        // progress on an empty journal (at most eight hints plus the all lane).
        if (before.bytes == 0 and before.records == 0 and after.records <= 9) return;
        if ((after.bytes > before.bytes and after.bytes > limits.bytes) or
            (after.records > before.records and after.records > limits.records)) return error.LiteReplayBacklogExceeded;
    }
};

/// A refusal hint is never an admission grant. It only avoids reconstructing
/// serving runtimes while the same producer footprint still cannot fit. The
/// physical transaction repeats exact accounting before it can commit.
pub const WaitHint = struct {
    growth: Cost,
    inode: u64,
    metadata: [16]u8,

    pub fn ready(self: WaitHint, state: State) bool {
        var after = state;
        self.growth.add(&after) catch return false;
        State.checkIncrease(state, after, state.limits) catch return false;
        return true;
    }
};

fn logicalKey(physical: []const u8) []const u8 {
    var logical = physical;
    if (std.mem.startsWith(u8, logical, "\x02db/")) {
        const end = std.mem.indexOfScalar(u8, logical, 0) orelse return logical;
        logical = logical[end + 1 ..];
    }
    return logical;
}

pub fn isEntry(physical: []const u8) bool {
    const logical = logicalKey(physical);
    return logical.len == 10 and logical[0] == 2 and (logical[1] <= 7 or logical[1] == 0xfe);
}

pub fn isReservation(physical: []const u8) bool {
    const logical = logicalKey(physical);
    return logical.len == reservation_prefix.len + 16 and std.mem.startsWith(u8, logical, reservation_prefix);
}

pub fn isTracked(physical: []const u8) bool {
    return isEntry(physical) or isReservation(physical);
}

pub const Cost = struct {
    bytes: u64,
    records: u64,

    pub fn add(self: Cost, state: *State) !void {
        state.bytes = try std.math.add(u64, state.bytes, self.bytes);
        state.records = try std.math.add(u64, state.records, self.records);
    }
    pub fn subtract(self: Cost, state: *State) !void {
        if (state.bytes < self.bytes or state.records < self.records) return error.InvalidNativeReplayAdmission;
        state.bytes -= self.bytes;
        state.records -= self.records;
    }
};

pub fn cost(physical: []const u8, value_len: u64, descriptor: []const u8) !Cost {
    if (!isReservation(physical)) return .{ .bytes = try charge(physical, value_len), .records = 1 };
    // One durable intent admission reserves every possible replay hint lane.
    // Resolution deletes this descriptor atomically with its source/replay
    // commit. Credits therefore survive crash, refresh and process changes.
    if (descriptor.len != 16 and descriptor.len != 32) return error.InvalidNativeReplayAdmission;
    const count = std.mem.readInt(u64, descriptor[0..8], .little);
    if (count == 0) return .{ .bytes = 0, .records = 0 };
    const intent_bytes = std.mem.readInt(u64, descriptor[8..16], .little);
    return .{ .bytes = try std.math.add(u64, try std.math.mul(u64, intent_bytes, 9), 4096), .records = 9 };
}

pub fn charge(physical: []const u8, value_len: u64) !u64 {
    return std.math.add(u64, value_len, physical.len + 28) catch error.InvalidNativeReplayAdmission;
}

test "native replay admission recognizes lanes across namespaces and rejects control keys" {
    try std.testing.expect(isEntry("\x02\xfe" ++ "12345678"));
    try std.testing.expect(isEntry("\x02db/embedded/tables/2\x00\x02\x00" ++ "12345678"));
    try std.testing.expect(!isEntry(key));
    try std.testing.expect(!isEntry("\x02\xff" ++ "12345678"));
}

test "native replay admission rejects growth but permits an over-limit consumer drain" {
    const state = State{ .bytes = 20, .records = 2 };
    try std.testing.expectEqualDeep(state, try State.decode(&state.encode()));
    try std.testing.expectError(error.LiteReplayBacklogExceeded, State.checkIncrease(state, .{ .bytes = 21, .records = 2 }, .{ .bytes = 20, .records = 2 }));
    try State.checkIncrease(state, .{ .bytes = 10, .records = 1 }, .{ .bytes = 5, .records = 0 });
    var corrupt = state.encode();
    corrupt[8] ^= 1;
    try std.testing.expectError(error.InvalidNativeReplayAdmission, State.decode(&corrupt));
}

test "native replay refusal hints wait for actual credits and permit empty oversized records" {
    const hint = WaitHint{ .growth = .{ .bytes = 10, .records = 5 }, .inode = 1, .metadata = @splat(1) };
    try std.testing.expect(!hint.ready(.{ .bytes = 1, .records = 4095 }));
    try std.testing.expect(hint.ready(.{ .bytes = 1, .records = 4091 }));
    try std.testing.expect(!hint.ready(.{ .bytes = 15, .records = 1, .limits = .{ .bytes = 20 } }));
    try std.testing.expect(hint.ready(.{ .limits = .{ .bytes = 1, .records = 1 } }));
}
