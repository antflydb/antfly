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

//! Optional, content-authenticated raw compiler evidence. It is metadata of
//! actual held field nodes; a hash or this blob alone never grants reuse.
const std = @import("std");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
const prefix = "field_input_cache:";
const header_bytes = 40;
pub const maximum_documents = 16384;
pub fn key(owner: Digest, input: Digest) [prefix.len + 64]u8 {
    return prefix.* ++ owner ++ input;
}
fn header(context: Digest, documents: usize) [header_bytes]u8 {
    var raw: [header_bytes]u8 = undefined;
    @memcpy(raw[0..4], "IFL1");
    @memcpy(raw[4..36], &context);
    std.mem.writeInt(u32, raw[36..40], @intCast(documents), .little);
    return raw;
}
pub fn digest(context: Digest, rows: []const ?Digest) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&header(context, rows.len));
    for (rows) |row| {
        hash.update(&.{@intFromBool(row != null)});
        hash.update(if (row) |*value| value else &@as(Digest, @splat(0)));
    }
    return hash.finalResult();
}
pub fn encode(a: A, context: Digest, rows: []const ?Digest) ![]u8 {
    if (rows.len == 0 or rows.len > maximum_documents) return error.ResourceBudgetExceeded;
    const out = try a.alloc(u8, header_bytes + rows.len * 33);
    @memcpy(out[0..header_bytes], &header(context, rows.len));
    for (rows, 0..) |row, index| {
        const offset = header_bytes + index * 33;
        out[offset] = @intFromBool(row != null);
        @memcpy(out[offset + 1 ..][0..32], if (row) |*value| value else &@as(Digest, @splat(0)));
    }
    return out;
}
pub const View = struct {
    raw: []const u8,
    pub fn documents(self: View) u32 {
        return std.mem.readInt(u32, self.raw[36..40], .little);
    }
    pub fn context(self: View) Digest {
        return self.raw[4..36].*;
    }
    pub fn row(self: View, ordinal: u32) !?Digest {
        if (ordinal >= self.documents()) return error.InvalidIndexFieldGeneration;
        const offset = header_bytes + @as(usize, ordinal) * 33;
        return if (self.raw[offset] == 1) self.raw[offset + 1 ..][0..32].* else null;
    }
};
pub fn open(raw: []const u8, expected: Digest) !View {
    if (raw.len < header_bytes or !std.mem.eql(u8, raw[0..4], "IFL1")) return error.InvalidIndexFieldGeneration;
    const documents = std.mem.readInt(u32, raw[36..40], .little);
    if (documents == 0 or documents > maximum_documents or raw.len != header_bytes + @as(usize, documents) * 33) return error.InvalidIndexFieldGeneration;
    var actual: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &actual, .{});
    if (!std.mem.eql(u8, &actual, &expected)) return error.IndexPublicationFenceMismatch;
    for (0..documents) |ordinal| if (raw[header_bytes + ordinal * 33] > 1) return error.InvalidIndexFieldGeneration;
    return .{ .raw = raw };
}

// A disposable, content-addressed cache. Slots own no graph resources or
// leases; replacing evidence cannot invalidate a retained compilation proof.
pub const entry_bytes = 256 * 1024;
const buckets = 32;
const slot_prefix = "field_input_cache_slot:";
fn slotKey(owner: Digest, slot: u8) [slot_prefix.len + 33]u8 {
    return slot_prefix.* ++ owner ++ .{slot};
}
pub fn stage(writer: anytype, owner: Digest, raw: []const u8, expected: Digest) !void {
    if (raw.len > entry_bytes) return;
    _ = try open(raw, expected);
    const name = key(owner, expected);
    if (writer.get(&name)) |prior| {
        if (std.mem.eql(u8, prior, raw)) return;
    } else |err| if (err != error.NotFound) return err;
    const bucket: u8 = expected[0] % buckets;
    var victim: u8 = bucket * 2 + (expected[1] & 1);
    for (0..2) |way| {
        const slot: u8 = bucket * 2 + @as(u8, @intCast(way));
        const prior = writer.get(&slotKey(owner, slot)) catch |err| switch (err) {
            error.NotFound => {
                victim = slot;
                break;
            },
            else => return err,
        };
        if (prior.len != 32 or std.mem.eql(u8, prior, &expected)) {
            victim = slot;
            break;
        }
    }
    const slot = slotKey(owner, victim);
    if (writer.get(&slot)) |prior| {
        if (prior.len == 32 and !std.mem.eql(u8, prior, &expected)) try writer.delete(&key(owner, prior[0..32].*));
    } else |err| if (err != error.NotFound) return err;
    // Both keys change in the caller's publication transaction. At most 64
    // entries of 256 KiB survive; reads and hits never write recency metadata.
    try writer.put(&name, raw);
    try writer.put(&slot, &expected);
}

test "persistent source publication evidence cache shares digests and bounds eviction" {
    const Fake = struct {
        a: A,
        writes: usize = 0,
        values: std.StringHashMapUnmanaged([]u8) = .empty,
        fn deinit(self: *@This()) void {
            var entries = self.values.iterator();
            while (entries.next()) |entry| {
                self.a.free(entry.key_ptr.*);
                self.a.free(entry.value_ptr.*);
            }
            self.values.deinit(self.a);
        }
        pub fn get(self: *@This(), name: []const u8) anyerror![]const u8 {
            return self.values.get(name) orelse error.NotFound;
        }
        pub fn put(self: *@This(), name: []const u8, value: []const u8) !void {
            self.writes += 1;
            const bytes = try self.a.dupe(u8, value);
            errdefer self.a.free(bytes);
            const entry = try self.values.getOrPut(self.a, name);
            if (entry.found_existing) self.a.free(entry.value_ptr.*) else {
                errdefer _ = self.values.remove(name);
                entry.key_ptr.* = try self.a.dupe(u8, name);
            }
            entry.value_ptr.* = bytes;
        }
        pub fn delete(self: *@This(), name: []const u8) !void {
            if (self.values.fetchRemove(name)) |entry| {
                self.a.free(entry.key);
                self.a.free(entry.value);
            }
        }
    };
    var writer: Fake = .{ .a = std.testing.allocator };
    defer writer.deinit();
    const owner: Digest = @splat(4);
    var inserted: usize = 0;
    var first: Digest = undefined;
    for (0..4096) |number| {
        var context: Digest = @splat(3);
        std.mem.writeInt(u64, context[0..8], number, .little);
        const rows = [_]?Digest{ @splat(2), null };
        const expected = digest(context, &rows);
        if (expected[0] % buckets != 0) continue;
        const raw = try encode(writer.a, context, &rows);
        defer writer.a.free(raw);
        try stage(&writer, owner, raw, expected);
        const count = writer.values.count();
        const writes = writer.writes;
        try stage(&writer, owner, raw, expected);
        try std.testing.expectEqual(count, writer.values.count());
        try std.testing.expectEqual(writes, writer.writes);
        if (inserted == 0) first = expected;
        inserted += 1;
        if (inserted == 3) break;
    }
    try std.testing.expectEqual(@as(usize, 3), inserted);
    // Two ways and two content values: no growth with field generations.
    try std.testing.expectEqual(@as(usize, 4), writer.values.count());
    const oversized = try writer.a.alloc(u8, entry_bytes + 1);
    defer writer.a.free(oversized);
    const before_oversized = writer.writes;
    try stage(&writer, owner, oversized, @splat(0));
    try std.testing.expectEqual(before_oversized, writer.writes);
    // The first item may be the selected victim; either retained item is
    // independently verified against its held field's compiler-input digest.
    if (writer.get(&key(owner, first))) |raw| _ = try open(raw, first) else |err| try std.testing.expectEqual(error.NotFound, err);
}
