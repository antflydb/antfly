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

//! Immutable committed pgoutput spooling. The store supplies durable scoped
//! put/get operations; publication, fencing, reservations and reclamation belong
//! to its metadata owner. A sealed object is not itself permission to publish.
//! The writer retains a bounded index frontier, never an entire transaction.
const std = @import("std");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const part_bytes = 256 * 1024;
pub const max_message_bytes = 16 * 1024 * 1024;
pub const max_bytes: u64 = 1024 * 1024 * 1024;
pub const max_messages: u32 = 65536;
const fanout = 64;
const levels = 4;
const reference_bytes = 48;
const manifest_bytes = 304;
const max_node_bytes = 8 + fanout * reference_bytes;
pub fn digest(bytes: []const u8) Digest {
    var out: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &out, .{});
    return out;
}
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
fn nz(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
const Codec = struct {
    fn writeInt(bytes: []u8, position: *usize, comptime T: type, value: T) void {
        std.mem.writeInt(T, bytes[position.*..][0..@sizeOf(T)], value, .big);
        position.* += @sizeOf(T);
    }
    fn readInt(bytes: []const u8, position: *usize, comptime T: type) T {
        const value = std.mem.readInt(T, bytes[position.*..][0..@sizeOf(T)], .big);
        position.* += @sizeOf(T);
        return value;
    }
    fn put(bytes: []u8, position: *usize, value: []const u8) void {
        @memcpy(bytes[position.*..][0..value.len], value);
        position.* += value.len;
    }
    fn take(bytes: []const u8, position: *usize, comptime length: usize) [length]u8 {
        const value = bytes[position.*..][0..length].*;
        position.* += length;
        return value;
    }
};
pub const Binding = struct {
    feed: [16]u8,
    epoch: u64,
    definition: Digest,
    projection: Digest,
    storage: Digest,
    pub fn validate(self: Binding) !void {
        if (!nz(self.feed) or self.epoch == 0 or !nz(self.definition) or !nz(self.projection) or !nz(self.storage)) return error.InvalidPgSpool;
    }
};
pub const Plan = struct {
    binding: Binding,
    before: Digest,
    after: Digest,
    completion: Digest,
    start_lsn: u64,
    bytes: u64,
    commit_offset: u64,
    xid: u32,
    commit_time: i64,
    pub fn validate(self: Plan) !void {
        try self.binding.validate();
        if (!nz(self.before) or !nz(self.after) or same(self.before, self.after) or !nz(self.completion) or self.start_lsn == 0 or self.bytes < 47 or self.bytes > max_bytes or self.commit_offset != self.bytes - 26 or self.xid == 0) return error.InvalidPgSpool;
        _ = std.math.add(u64, self.start_lsn, self.bytes) catch return error.InvalidPgSpool;
    }
    fn encode(self: Plan, out: []u8, p: *usize) void {
        Codec.put(out, p, &self.binding.feed);
        Codec.writeInt(out, p, u64, self.binding.epoch);
        Codec.put(out, p, &self.binding.definition);
        Codec.put(out, p, &self.binding.projection);
        Codec.put(out, p, &self.binding.storage);
        Codec.put(out, p, &self.before);
        Codec.put(out, p, &self.after);
        Codec.put(out, p, &self.completion);
        Codec.writeInt(out, p, u64, self.start_lsn);
        Codec.writeInt(out, p, u64, self.bytes);
        Codec.writeInt(out, p, u64, self.commit_offset);
        Codec.writeInt(out, p, u32, self.xid);
        Codec.writeInt(out, p, i64, self.commit_time);
    }
    fn decode(bytes: []const u8, p: *usize) !Plan {
        const result: Plan = .{
            .binding = .{ .feed = Codec.take(bytes, p, 16), .epoch = Codec.readInt(bytes, p, u64), .definition = Codec.take(bytes, p, 32), .projection = Codec.take(bytes, p, 32), .storage = Codec.take(bytes, p, 32) },
            .before = Codec.take(bytes, p, 32),
            .after = Codec.take(bytes, p, 32),
            .completion = Codec.take(bytes, p, 32),
            .start_lsn = Codec.readInt(bytes, p, u64),
            .bytes = Codec.readInt(bytes, p, u64),
            .commit_offset = Codec.readInt(bytes, p, u64),
            .xid = Codec.readInt(bytes, p, u32),
            .commit_time = Codec.readInt(bytes, p, i64),
        };
        try result.validate();
        return result;
    }
};
pub const Reference = struct {
    hash: Digest,
    bytes: u64,
    messages: u32,
    height: u8,
    encoded_bytes: u16,
    fn validate(self: Reference) !void {
        if (!nz(self.hash) or self.bytes == 0 or self.bytes > max_bytes or self.messages == 0 or self.messages > max_messages or self.height >= levels or self.encoded_bytes < 8 or self.encoded_bytes > max_node_bytes or (self.height == 0 and (self.messages != 1 or self.bytes > max_message_bytes))) return error.PgSpoolCorrupt;
    }
    fn encode(self: Reference, out: []u8) void {
        var p: usize = 0;
        Codec.put(out, &p, &self.hash);
        Codec.writeInt(out, &p, u64, self.bytes);
        Codec.writeInt(out, &p, u32, self.messages);
        Codec.writeInt(out, &p, u8, self.height);
        Codec.writeInt(out, &p, u16, self.encoded_bytes);
        out[p] = 0;
    }
    fn decode(bytes: []const u8) !Reference {
        var p: usize = 0;
        const value: Reference = .{ .hash = Codec.take(bytes, &p, 32), .bytes = Codec.readInt(bytes, &p, u64), .messages = Codec.readInt(bytes, &p, u32), .height = Codec.readInt(bytes, &p, u8), .encoded_bytes = Codec.readInt(bytes, &p, u16) };
        if (bytes[p] != 0) return error.PgSpoolCorrupt;
        try value.validate();
        return value;
    }
};
pub const Seal = struct {
    /// Authenticated storage locator, including its immutable namespace scope.
    identity: Digest,
    /// Wire identity remains stable when an authorized owner reconstructs the
    /// exact historical spool in a replacement storage/upload namespace.
    serialization: Digest,
    plan: Plan,
    root: Reference,
};
fn serializationIdentity(plan: Plan, root: Reference) Digest {
    var wire = plan;
    wire.binding.storage = @splat(0);
    var bytes: [manifest_bytes]u8 = @splat(0);
    @memcpy(bytes[0..4], "PGC1");
    var p: usize = 4;
    wire.encode(&bytes, &p);
    root.encode(bytes[p..][0..reference_bytes]);
    return digest(&bytes);
}
fn checkedGet(a: A, store: anytype, hash: Digest, size: usize) ![]u8 {
    const bytes = try store.get(a, hash, size);
    errdefer a.free(bytes);
    if (bytes.len != size or !same(digest(bytes), hash)) return error.PgSpoolCorrupt;
    return bytes;
}
fn checkedPut(store: anytype, bytes: []const u8) !Digest {
    const hash = digest(bytes);
    try store.put(hash, bytes);
    return hash;
}
pub const Builder = struct {
    plan: Plan,
    frontier: [levels][fanout]Reference = undefined,
    counts: [levels]u8 = @splat(0),
    bytes: u64 = 0,
    messages: u32 = 0,
    last: u8 = 0,
    poisoned: bool = false,
    sealed: ?Seal = null,
    pub fn init(plan: Plan) !Builder {
        try plan.validate();
        return .{ .plan = plan };
    }
    fn branch(store: anytype, children: []const Reference) !Reference {
        if (children.len == 0 or children.len > fanout) return error.InvalidPgSpool;
        var buffer: [max_node_bytes]u8 = @splat(0);
        @memcpy(buffer[0..4], "PGI1");
        std.mem.writeInt(u16, buffer[4..6], @intCast(children.len), .big);
        var result: Reference = .{ .hash = @splat(0), .bytes = 0, .messages = 0, .height = children[0].height + 1, .encoded_bytes = @intCast(8 + children.len * reference_bytes) };
        for (children, 0..) |child, index| {
            try child.validate();
            if (child.height + 1 != result.height) return error.InvalidPgSpool;
            result.bytes = std.math.add(u64, result.bytes, child.bytes) catch return error.InvalidPgSpool;
            result.messages = std.math.add(u32, result.messages, child.messages) catch return error.InvalidPgSpool;
            child.encode(buffer[8 + index * reference_bytes ..][0..reference_bytes]);
        }
        result.hash = try checkedPut(store, buffer[0..result.encoded_bytes]);
        try result.validate();
        return result;
    }
    fn add(self: *Builder, store: anytype, reference: Reference) !void {
        const level = reference.height;
        if (level >= levels) return error.InvalidPgSpool;
        self.frontier[level][self.counts[level]] = reference;
        self.counts[level] += 1;
        if (self.counts[level] == fanout) {
            const parent = try branch(store, &self.frontier[level]);
            self.counts[level] = 0;
            try self.add(store, parent);
        }
    }
    fn message(self: *Builder, bytes: []const u8) !void {
        if (bytes.len == 0 or bytes.len > max_message_bytes or bytes.len > self.plan.bytes - self.bytes or self.messages == max_messages) return error.InvalidPgSpool;
        if (self.messages == 0) {
            if (bytes.len != 21 or bytes[0] != 'B' or std.mem.readInt(u64, bytes[1..9], .big) != self.plan.start_lsn + self.plan.commit_offset or std.mem.readInt(i64, bytes[9..17], .big) != self.plan.commit_time or std.mem.readInt(u32, bytes[17..21], .big) != self.plan.xid) return error.InvalidPgSpool;
        } else if (bytes[0] == 'C') {
            if (self.bytes != self.plan.commit_offset or bytes.len != 26 or bytes[1] != 0 or std.mem.readInt(u64, bytes[2..10], .big) != self.plan.start_lsn + self.plan.commit_offset or std.mem.readInt(u64, bytes[10..18], .big) != self.plan.start_lsn + self.plan.bytes or std.mem.readInt(i64, bytes[18..26], .big) != self.plan.commit_time) return error.InvalidPgSpool;
        } else {
            if (self.bytes >= self.plan.commit_offset or bytes.len > self.plan.commit_offset - self.bytes or std.mem.indexOfScalar(u8, "RYIUDTO", bytes[0]) == null) return error.InvalidPgSpool;
        }
    }
    /// One complete logical message. A failure poisons the writer: rebuild
    /// from retained input in its original reservation, never continue after
    /// an uncertain partial object upload or changed projection.
    pub fn append(self: *Builder, store: anytype, bytes: []const u8) !void {
        if (self.poisoned or self.sealed != null) return error.InvalidPgSpool;
        errdefer self.poisoned = true;
        try self.message(bytes);
        const parts = std.math.divCeil(usize, bytes.len, part_bytes) catch unreachable;
        var leaf: [16 + 32 * (max_message_bytes / part_bytes)]u8 = @splat(0);
        @memcpy(leaf[0..4], "PGM1");
        std.mem.writeInt(u32, leaf[4..8], @intCast(bytes.len), .big);
        std.mem.writeInt(u16, leaf[8..10], @intCast(parts), .big);
        leaf[10] = bytes[0];
        for (0..parts) |index| {
            const offset = index * part_bytes;
            const hash = try checkedPut(store, bytes[offset..@min(bytes.len, offset + part_bytes)]);
            @memcpy(leaf[16 + index * 32 ..][0..32], &hash);
        }
        const size = 16 + parts * 32;
        try self.add(store, .{ .hash = try checkedPut(store, leaf[0..size]), .bytes = bytes.len, .messages = 1, .height = 0, .encoded_bytes = @intCast(size) });
        self.bytes += bytes.len;
        self.messages += 1;
        self.last = bytes[0];
    }
    pub fn finish(self: *Builder, store: anytype) !Seal {
        if (self.sealed) |sealed| return sealed;
        if (self.poisoned or self.bytes != self.plan.bytes or self.last != 'C' or self.messages < 2) return error.PgSpoolIncomplete;
        errdefer self.poisoned = true;
        for (0..levels) |level| {
            const count = self.counts[level];
            if (count == 0) continue;
            var higher = false;
            for (self.counts[level + 1 ..]) |value| if (value != 0) {
                higher = true;
                break;
            };
            if (count == 1 and !higher) {
                const root = self.frontier[level][0];
                var buffer: [manifest_bytes]u8 = @splat(0);
                @memcpy(buffer[0..4], "PGS1");
                var p: usize = 4;
                self.plan.encode(&buffer, &p);
                root.encode(buffer[p..][0..reference_bytes]);
                const result: Seal = .{ .identity = try checkedPut(store, &buffer), .serialization = serializationIdentity(self.plan, root), .plan = self.plan, .root = root };
                self.sealed = result;
                return result;
            }
            const parent = try branch(store, self.frontier[level][0..count]);
            self.counts[level] = 0;
            try self.add(store, parent);
        }
        return error.InvalidPgSpool;
    }
};
pub const Reader = struct {
    seal: Seal,
    pub fn open(a: A, store: anytype, identity: Digest, binding: Binding) !Reader {
        try binding.validate();
        const bytes = try checkedGet(a, store, identity, manifest_bytes);
        defer a.free(bytes);
        if (!std.mem.eql(u8, bytes[0..4], "PGS1")) return error.PgSpoolCorrupt;
        var p: usize = 4;
        const plan = Plan.decode(bytes, &p) catch return error.PgSpoolCorrupt;
        const root = try Reference.decode(bytes[p..][0..reference_bytes]);
        if (!std.meta.eql(plan.binding, binding) or root.bytes != plan.bytes or root.messages < 2) return error.PgSpoolCorrupt;
        return .{ .seal = .{ .identity = identity, .serialization = serializationIdentity(plan, root), .plan = plan, .root = root } };
    }
    /// offset is relative to this transaction, not a source/native frontier.
    /// An interior message position requires rewind through the slot authority.
    pub fn message(self: Reader, a: A, store: anytype, offset: u64, limit: u32) !?[]u8 {
        if (offset == self.seal.plan.bytes) return null;
        if (offset > self.seal.plan.bytes) return error.PgSpoolCursorInvalid;
        if (limit == 0 or limit > max_message_bytes) return error.InvalidPgSpool;
        var reference = self.seal.root;
        var position = offset;
        while (true) {
            const node = try checkedGet(a, store, reference.hash, reference.encoded_bytes);
            defer a.free(node);
            if (reference.height == 0) {
                if (position != 0) return error.PgSpoolCursorInvalid;
                if (node.len < 16 or !std.mem.eql(u8, node[0..4], "PGM1") or !std.mem.allEqual(u8, node[11..16], 0)) return error.PgSpoolCorrupt;
                const size = std.mem.readInt(u32, node[4..8], .big);
                const parts = std.mem.readInt(u16, node[8..10], .big);
                const expected_parts = std.math.divCeil(u32, size, part_bytes) catch return error.PgSpoolCorrupt;
                if (size != reference.bytes or parts != expected_parts or parts == 0 or node.len != 16 + @as(usize, parts) * 32 or std.mem.indexOfScalar(u8, "BRYIUDTOC", node[10]) == null) return error.PgSpoolCorrupt;
                if (size > limit) return error.PgSpoolMessageTooLarge;
                const result = try a.alloc(u8, size);
                errdefer a.free(result);
                for (0..parts) |index| {
                    const start = index * part_bytes;
                    const count = @min(size - start, part_bytes);
                    const bytes = try checkedGet(a, store, node[16 + index * 32 ..][0..32].*, count);
                    defer a.free(bytes);
                    @memcpy(result[start..][0..count], bytes);
                }
                if (result[0] != node[10]) return error.PgSpoolCorrupt;
                return result;
            }
            if (node.len < 8 or !std.mem.eql(u8, node[0..4], "PGI1") or node[6] != 0 or node[7] != 0) return error.PgSpoolCorrupt;
            const count = std.mem.readInt(u16, node[4..6], .big);
            if (count == 0 or count > fanout or node.len != 8 + @as(usize, count) * reference_bytes) return error.PgSpoolCorrupt;
            var total_bytes: u64 = 0;
            var total_messages: u32 = 0;
            var selected: ?Reference = null;
            var child_position: u64 = 0;
            for (0..count) |index| {
                const child = try Reference.decode(node[8 + index * reference_bytes ..][0..reference_bytes]);
                if (child.height + 1 != reference.height) return error.PgSpoolCorrupt;
                const end = std.math.add(u64, total_bytes, child.bytes) catch return error.PgSpoolCorrupt;
                if (position >= total_bytes and position < end) {
                    selected = child;
                    child_position = position - total_bytes;
                }
                total_bytes = end;
                total_messages = std.math.add(u32, total_messages, child.messages) catch return error.PgSpoolCorrupt;
            }
            if (total_bytes != reference.bytes or total_messages != reference.messages) return error.PgSpoolCorrupt;
            reference = selected orelse return error.PgSpoolCorrupt;
            position = child_position;
        }
    }
};

const MemoryStore = struct {
    a: A,
    objects: std.AutoHashMap(Digest, []u8),
    puts: usize = 0,
    reads: usize = 0,
    fail_at: ?usize = null,
    fn init(a: A) MemoryStore {
        return .{ .a = a, .objects = .init(a) };
    }
    fn deinit(self: *@This()) void {
        var it = self.objects.valueIterator();
        while (it.next()) |value| self.a.free(value.*);
        self.objects.deinit();
    }
    pub fn put(self: *@This(), hash: Digest, bytes: []const u8) !void {
        const index = self.puts;
        self.puts += 1;
        if (self.fail_at == index) return error.TestWriteFailure;
        if (self.objects.get(hash)) |old| {
            if (!std.mem.eql(u8, old, bytes)) return error.PgSpoolCorrupt;
            return;
        }
        const owned = try self.a.dupe(u8, bytes);
        errdefer self.a.free(owned);
        try self.objects.put(hash, owned);
    }
    pub fn get(self: *@This(), a: A, hash: Digest, size: usize) ![]u8 {
        self.reads += 1;
        const bytes = self.objects.get(hash) orelse return error.PgSpoolCorrupt;
        if (bytes.len != size) return error.PgSpoolCorrupt;
        return a.dupe(u8, bytes);
    }
};
fn expectReservationCovers(store: *MemoryStore, bytes: u64) !void {
    var actual: u64 = 0;
    var objects = store.objects.valueIterator();
    while (objects.next()) |object| actual += object.len;
    try std.testing.expect(actual <= try @import("pg_spool_control.zig").reservationBytes(bytes));
}
const pgoutput = @import("../pgwire/pgoutput.zig");
fn testPlan(rows: u32, size: u32) Plan {
    const bytes = 47 + @as(u64, rows) * (@as(u64, size) + 13);
    return .{ .binding = .{ .feed = @splat(1), .epoch = 1, .definition = @splat(2), .projection = @splat(3), .storage = @splat(4) }, .before = @splat(5), .after = @splat(6), .completion = @splat(7), .start_lsn = 10, .bytes = bytes, .commit_offset = bytes - 26, .xid = 42, .commit_time = -1 };
}
pub fn testBuild(a: A, store: anytype, plan: Plan, rows: u32, size: u32) !Seal {
    var writer = try Builder.init(plan);
    const begin = try pgoutput.encode(a, .{ .begin = .{ .final_lsn = @fromBackingInt(plan.start_lsn + plan.commit_offset), .commit_time = plan.commit_time, .xid = plan.xid } }, max_message_bytes);
    defer a.free(begin);
    try writer.append(store, begin);
    const row = try a.alloc(u8, size);
    defer a.free(row);
    @memset(row, 'x');
    for (0..rows) |index| {
        if (size >= 16) _ = try std.fmt.bufPrint(row[0..16], "{d:0>16}", .{index});
        const bytes = try pgoutput.encode(a, .{ .insert = .{ .relation = 42, .values = &.{.{ .text = row }} } }, max_message_bytes);
        defer a.free(bytes);
        try writer.append(store, bytes);
    }
    const commit = try pgoutput.encode(a, .{ .commit = .{ .commit_lsn = @fromBackingInt(plan.start_lsn + plan.commit_offset), .end_lsn = @fromBackingInt(plan.start_lsn + plan.bytes), .commit_time = plan.commit_time } }, max_message_bytes);
    defer a.free(commit);
    try writer.append(store, commit);
    return writer.finish(store);
}
test "pg spool sealed index resumes exact messages and rejects interior cursors and corrupt bytes" {
    const a = std.testing.allocator;
    var store = MemoryStore.init(a);
    defer store.deinit();
    const plan = testPlan(130, 32);
    const sealed = try testBuild(a, &store, plan, 130, 32);
    try expectReservationCovers(&store, plan.bytes);
    try std.testing.expectEqual(@as(u8, 2), sealed.root.height);
    const reader = try Reader.open(a, &store, sealed.identity, plan.binding);
    store.reads = 0;
    const row = (try reader.message(a, &store, 21 + 75 * 45, 1024)).?;
    defer a.free(row);
    try std.testing.expectEqual(@as(u8, 'I'), row[0]);
    try std.testing.expect(std.mem.indexOf(u8, row, "0000000000000075") != null);
    try std.testing.expect(store.reads <= 5);
    try std.testing.expectError(error.PgSpoolCursorInvalid, reader.message(a, &store, 22, 1024));
    try std.testing.expectError(error.PgSpoolMessageTooLarge, reader.message(a, &store, 21, 44));
    try std.testing.expect((try reader.message(a, &store, plan.bytes, 1024)) == null);
    var wrong = plan.binding;
    wrong.projection = @splat(9);
    try std.testing.expectError(error.PgSpoolCorrupt, Reader.open(a, &store, sealed.identity, wrong));
    const first = (try reader.message(a, &store, 21, 1024)).?;
    defer a.free(first);
    const hash = digest(first);
    store.objects.getPtr(hash).?.*[44] ^= 1;
    try std.testing.expectError(error.PgSpoolCorrupt, reader.message(a, &store, 21, 1024));
}
test "pg spool multipart messages authenticate the final partial part before returning bytes" {
    const a = std.testing.allocator;
    var store = MemoryStore.init(a);
    defer store.deinit();
    const size = 3 * part_bytes + 17;
    const plan = testPlan(1, size);
    const sealed = try testBuild(a, &store, plan, 1, size);
    try expectReservationCovers(&store, plan.bytes);
    const reader = try Reader.open(a, &store, sealed.identity, plan.binding);
    try std.testing.expectError(error.PgSpoolMessageTooLarge, reader.message(a, &store, 21, size));
    const row = (try reader.message(a, &store, 21, size + 13)).?;
    defer a.free(row);
    try std.testing.expectEqual(@as(usize, size + 13), row.len);
    try std.testing.expectEqual(@as(u8, 'I'), row[0]);
    try std.testing.expectEqualSlices(u8, "0000000000000000", row[13..29]);
    const tail = row[3 * part_bytes ..];
    try std.testing.expectEqual(@as(usize, 30), tail.len);
    const hash = digest(tail);
    store.objects.getPtr(hash).?.*[tail.len - 1] ^= 1;
    try std.testing.expectError(error.PgSpoolCorrupt, reader.message(a, &store, 21, size + 13));
}
test "pg spool wire identity survives storage relocation while locator and publication binding stay distinct" {
    const a = std.testing.allocator;
    var store = MemoryStore.init(a);
    defer store.deinit();
    const plan = testPlan(1, 32);
    const original = try testBuild(a, &store, plan, 1, 32);
    var moved = plan;
    moved.binding.storage = @splat(8);
    const replacement = try testBuild(a, &store, moved, 1, 32);
    try std.testing.expect(same(original.serialization, replacement.serialization));
    try std.testing.expect(!same(original.identity, replacement.identity));
    try std.testing.expectError(error.PgSpoolCorrupt, Reader.open(a, &store, replacement.identity, plan.binding));
    moved.binding.projection = @splat(9);
    const changed = try testBuild(a, &store, moved, 1, 32);
    try std.testing.expect(!same(original.serialization, changed.serialization));
}
test "pg spool incomplete or uncertain uploads never produce a publishable seal" {
    const a = std.testing.allocator;
    var store = MemoryStore.init(a);
    defer store.deinit();
    const plan = testPlan(0, 0);
    var writer = try Builder.init(plan);
    try std.testing.expectError(error.PgSpoolIncomplete, writer.finish(&store));
    const begin = try pgoutput.encode(a, .{ .begin = .{ .final_lsn = @fromBackingInt(plan.start_lsn + 21), .commit_time = -1, .xid = 42 } }, 128);
    defer a.free(begin);
    store.fail_at = 1;
    try std.testing.expectError(error.TestWriteFailure, writer.append(&store, begin));
    try std.testing.expectError(error.InvalidPgSpool, writer.append(&store, begin));
    try std.testing.expectError(error.PgSpoolIncomplete, writer.finish(&store));
    store.fail_at = null;
    const retried = try testBuild(a, &store, plan, 0, 0);
    const same_again = try testBuild(a, &store, plan, 0, 0);
    try std.testing.expect(same(retried.identity, same_again.identity));
}
test "pg spool large committed transaction reads within a small independent memory budget" {
    const a = std.testing.allocator;
    var store = MemoryStore.init(a);
    defer store.deinit();
    var budget: @import("../pgwire/budget.zig").Budget = .{ .child = a, .limit = 64 * 1024 };
    const bounded = budget.allocator();
    const plan = testPlan(4096, 8192);
    try std.testing.expect(plan.bytes > 32 * 1024 * 1024);
    const sealed = try testBuild(bounded, &store, plan, 4096, 8192);
    try expectReservationCovers(&store, plan.bytes);
    try std.testing.expect(@sizeOf(Builder) < 64 * 1024);
    const reader = try Reader.open(bounded, &store, sealed.identity, plan.binding);
    store.reads = 0;
    const bytes = (try reader.message(bounded, &store, 21 + 4095 * 8205, 16384)).?;
    try std.testing.expect(std.mem.indexOf(u8, bytes, "0000000000004095") != null);
    try std.testing.expect(store.reads <= 6);
    bounded.free(bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.used);
}
fn allocationProbe(a: A) !void {
    var store = MemoryStore.init(a);
    defer store.deinit();
    const plan = testPlan(2, 32);
    const sealed = try testBuild(a, &store, plan, 2, 32);
    const reader = try Reader.open(a, &store, sealed.identity, plan.binding);
    const row = (try reader.message(a, &store, 21, 1024)).?;
    defer a.free(row);
}
test "pg spool allocation failures release every unpublished object and read page" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationProbe, .{});
}
