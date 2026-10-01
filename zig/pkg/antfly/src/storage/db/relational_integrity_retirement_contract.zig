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

//! Data-only relational lifecycle contract. Physical execution stays in its owner.
const std = @import("std");
const Allocator = std.mem.Allocator;
const integrity = @import("relational_integrity_contract.zig");

pub const key = "\x00\x00__metadata__:relational_integrity_retirement";

pub const Phase = enum(u8) { fenced, foreign_keys, unique, ready };

pub const Progress = struct {
    job_id: integrity.Generation,
    generation_set: integrity.Digest,
    owner: integrity.Digest,
    target_schema_digest: integrity.Digest,
    schema_version: u32,
    phase: Phase = .fenced,
    rows_scanned: u64 = 0,
    generations: []const integrity.Generation,
    cursor: []const u8 = "",

    pub fn includes(self: Progress, generation: integrity.Generation) bool {
        for (self.generations) |item| if (std.mem.eql(u8, &item, &generation)) return true;
        return false;
    }

    pub fn encode(self: Progress, alloc: Allocator) ![]u8 {
        if (std.mem.allEqual(u8, &self.job_id, 0) or self.generations.len == 0 or self.generations.len > 1024 or self.cursor.len > 1024 * 1024 or
            ((self.phase == .fenced or self.phase == .ready) and self.cursor.len != 0)) return error.InvalidConstraintRetirement;
        for (self.generations, 0..) |generation, index| {
            if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidConstraintRetirement;
            for (self.generations[0..index]) |previous| if (std.mem.eql(u8, &previous, &generation)) return error.InvalidConstraintRetirement;
        }
        const out = try alloc.alloc(u8, 136 + self.generations.len * 16 + self.cursor.len + 32);
        @memset(out[0..136], 0);
        @memcpy(out[0..4], "AIR1");
        out[4] = @intFromEnum(self.phase);
        @memcpy(out[8..24], &self.job_id);
        @memcpy(out[24..56], &self.generation_set);
        @memcpy(out[56..88], &self.owner);
        @memcpy(out[88..120], &self.target_schema_digest);
        std.mem.writeInt(u32, out[120..124], self.schema_version, .little);
        std.mem.writeInt(u32, out[124..128], @intCast(self.generations.len), .little);
        std.mem.writeInt(u64, out[128..136], self.rows_scanned, .little);
        for (self.generations, 0..) |generation, index| @memcpy(out[136 + index * 16 ..][0..16], &generation);
        @memcpy(out[136 + self.generations.len * 16 ..][0..self.cursor.len], self.cursor);
        std.crypto.hash.Blake3.hash(out[0 .. out.len - 32], out[out.len - 32 ..][0..32], .{});
        return out;
    }

    /// Returned generations and cursor borrow the checksummed input.
    pub fn decode(bytes: []const u8) !Progress {
        if (bytes.len < 184 or bytes.len > 136 + 1024 * 16 + 1024 * 1024 + 32 or !std.mem.eql(u8, bytes[0..4], "AIR1") or
            !std.mem.allEqual(u8, bytes[5..8], 0)) return error.InvalidConstraintRetirement;
        var checksum: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[bytes.len - 32 ..])) return error.InvalidConstraintRetirement;
        const count = std.mem.readInt(u32, bytes[124..128], .little);
        if (count == 0 or count > 1024 or 136 + @as(usize, count) * 16 > bytes.len - 32) return error.InvalidConstraintRetirement;
        const generations: []const integrity.Generation = std.mem.bytesAsSlice(integrity.Generation, bytes[136 .. 136 + @as(usize, count) * 16]);
        const result: Progress = .{
            .job_id = bytes[8..24].*,
            .generation_set = bytes[24..56].*,
            .owner = bytes[56..88].*,
            .target_schema_digest = bytes[88..120].*,
            .schema_version = std.mem.readInt(u32, bytes[120..124], .little),
            .phase = std.enums.fromInt(Phase, bytes[4]) orelse return error.InvalidConstraintRetirement,
            .rows_scanned = std.mem.readInt(u64, bytes[128..136], .little),
            .generations = generations,
            .cursor = bytes[136 + @as(usize, count) * 16 .. bytes.len - 32],
        };
        if (std.mem.allEqual(u8, &result.job_id, 0) or result.cursor.len > 1024 * 1024 or
            ((result.phase == .fenced or result.phase == .ready) and result.cursor.len != 0)) return error.InvalidConstraintRetirement;
        for (generations, 0..) |generation, index| {
            if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidConstraintRetirement;
            for (generations[0..index]) |previous| if (std.mem.eql(u8, &previous, &generation)) return error.InvalidConstraintRetirement;
        }
        return result;
    }
};

pub const Command = struct {
    routing_key: []const u8,
    expected: ?[]const u8,
    next: []const u8,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};
