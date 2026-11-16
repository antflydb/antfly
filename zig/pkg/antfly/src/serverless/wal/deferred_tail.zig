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

//! Owned, bounded HEAD delta. Both records and duplicate receipts publish in
//! the existing HEAD CAS. No pointer into a JSON parse arena escapes decoding.
const std = @import("std");
const codec = @import("record_codec.zig");
const tree = @import("../graph_segment/page_tree.zig");
const A = std.mem.Allocator;
pub const Tail = struct {
    pub const capacity = 1536;
    pub const max_records = 8;
    bytes: [capacity]u8 = @splat(0),
    len: u16 = 2,
    pub fn count(self: *const Tail) usize {
        return self.bytes[0];
    }
    pub const View = struct {
        records: [max_records][]const u8 = undefined,
        operations: [max_records]tree.Mutation = undefined,
        record_count: usize = 0,
        operation_count: usize = 0,
    };
    pub fn view(self: *const Tail) !View {
        if (self.len < 2 or self.len > capacity or self.bytes[0] > max_records or self.bytes[1] > max_records) return error.InvalidWal;
        var out = View{ .record_count = self.bytes[0], .operation_count = self.bytes[1] };
        var cursor: usize = 2;
        for (out.records[0..out.record_count]) |*record| {
            if (cursor + 2 > self.len) return error.InvalidWal;
            const n = std.mem.readInt(u16, self.bytes[cursor..][0..2], .little);
            cursor += 2;
            if (n == 0 or n > self.len - cursor) return error.InvalidWal;
            record.* = self.bytes[cursor..][0..n];
            cursor += n;
        }
        for (out.operations[0..out.operation_count], 0..) |*op, i| {
            if (cursor + 3 > self.len) return error.InvalidWal;
            const n = std.mem.readInt(u16, self.bytes[cursor..][0..2], .little);
            const value_len = self.bytes[cursor + 2];
            cursor += 3;
            if (n == 0 or n > 128 or (value_len != 48 and value_len != 56) or n + @as(usize, value_len) > self.len - cursor) return error.InvalidWal;
            const key = self.bytes[cursor..][0..n];
            cursor += n;
            const value = self.bytes[cursor..][0..value_len];
            cursor += value_len;
            for (out.operations[0..i]) |prior| if (std.mem.eql(u8, prior.key, key)) return error.InvalidWal;
            op.* = .{ .key = key, .value = value };
        }
        if (cursor != self.len) return error.InvalidWal;
        return out;
    }
    pub fn validate(self: *const Tail, latest: u64) !void {
        const v = try self.view();
        if (v.record_count == 0 or v.record_count > latest) return error.InvalidWal;
        const first = latest + 1 - v.record_count;
        for (v.records[0..v.record_count], 0..) |bytes, i| {
            const record = try codec.parseRecord(bytes, 0);
            if (record.next_cursor != bytes.len or record.lsn != first + i) return error.InvalidWal;
        }
        for (v.operations[0..v.operation_count]) |op| {
            const value = op.value.?;
            const start = std.mem.readInt(u64, value[0..8], .little);
            const end = if (value.len == 56) std.mem.readInt(u64, value[8..16], .little) else start;
            if (start < first or end < start or end > latest) return error.InvalidWal;
        }
    }
    pub fn lookup(self: *const Tail, id: []const u8) !?[]const u8 {
        const v = try self.view();
        for (v.operations[0..v.operation_count]) |op| if (std.mem.eql(u8, op.key, id)) return op.value;
        return null;
    }
    pub fn append(self: *const Tail, records: []const tree.Mutation, operations: []const tree.Mutation) !?Tail {
        const v = try self.view();
        if (v.record_count + records.len > max_records or v.operation_count + operations.len > max_records) return null;
        var out = Tail{};
        out.bytes[0] = @intCast(v.record_count + records.len);
        out.bytes[1] = @intCast(v.operation_count + operations.len);
        for (v.records[0..v.record_count]) |record| if (!out.addRecord(record)) return null;
        for (records) |record| if (!out.addRecord(record.value orelse return error.InvalidWal)) return null;
        for (v.operations[0..v.operation_count]) |op| if (!out.addOperation(op)) return null;
        for (operations) |op| if (!out.addOperation(op)) return null;
        return out;
    }
    fn addRecord(self: *Tail, value: []const u8) bool {
        if (value.len > capacity - @as(usize, self.len) or value.len + 2 > capacity - @as(usize, self.len)) return false;
        std.mem.writeInt(u16, self.bytes[self.len..][0..2], @intCast(value.len), .little);
        self.len += 2;
        @memcpy(self.bytes[self.len..][0..value.len], value);
        self.len += @intCast(value.len);
        return true;
    }
    fn addOperation(self: *Tail, op: tree.Mutation) bool {
        const value = op.value orelse return false;
        if (op.key.len == 0 or op.key.len > 128 or (value.len != 48 and value.len != 56) or op.key.len + value.len + 3 > capacity - @as(usize, self.len)) return false;
        std.mem.writeInt(u16, self.bytes[self.len..][0..2], @intCast(op.key.len), .little);
        self.bytes[self.len + 2] = @intCast(value.len);
        self.len += 3;
        @memcpy(self.bytes[self.len..][0..op.key.len], op.key);
        self.len += @intCast(op.key.len);
        @memcpy(self.bytes[self.len..][0..value.len], value);
        self.len += @intCast(value.len);
        return true;
    }
    pub fn jsonStringify(self: Tail, writer: anytype) !void {
        var encoded: [2048]u8 = undefined;
        return writer.write(std.base64.standard.Encoder.encode(&encoded, self.bytes[0..self.len]));
    }
    pub fn jsonParse(a: A, source: anytype, options: std.json.ParseOptions) !Tail {
        const text = try std.json.innerParse([]const u8, a, source, options);
        const len = std.base64.standard.Decoder.calcSizeForSlice(text) catch return error.UnexpectedToken;
        if (len < 2 or len > capacity) return error.UnexpectedToken;
        var out = Tail{ .len = @intCast(len) };
        std.base64.standard.Decoder.decode(out.bytes[0..len], text) catch return error.UnexpectedToken;
        _ = out.view() catch return error.UnexpectedToken;
        return out;
    }
};
