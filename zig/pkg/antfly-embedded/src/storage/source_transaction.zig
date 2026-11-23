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

//! Provider-neutral complete row transactions. Storage adapters validate their
//! schema and durability capabilities separately; accepting this envelope does
//! not establish a commit, an index frontier, or an outbound feed position.
const std = @import("std");

pub const Batch = struct {
    batch_id: []const u8,
    source: []const u8,
    epoch: []const u8,
    checkpoint: []const u8,
    expected_checkpoint: ?[]const u8 = null,
    key_fields: []const []const u8,
    /// A complete checkpoint-only transaction is valid. Destinations must
    /// commit its progress under the same ownership and predecessor guards.
    changes: []const Change,

    /// Set only by a qualified owner publishing a complete initial cut.
    bootstrap: bool = false,

    pub fn jsonStringify(self: Batch, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Batch).@"struct".field_names) |name| {
            if (!std.mem.eql(u8, name, "bootstrap") or self.bootstrap) {
                try stream.objectField(name);
                try stream.write(@field(self, name));
            }
        }
        try stream.endObject();
    }

    pub const Change = struct { op: enum { upsert, delete }, row: std.json.Value };

    pub fn validate(self: Batch, max_changes: usize) !void {
        try self.validateHeader(max_changes);
        for (self.changes) |change| {
            if (change.row != .object) return error.InvalidSourceTransaction;
            for (self.key_fields) |name| {
                const value = change.row.object.get(name) orelse return error.InvalidSourceTransaction;
                if (value == .null) return error.InvalidSourceTransaction;
            }
            if (change.op == .delete and change.row.object.count() != self.key_fields.len) return error.InvalidSourceTransaction;
        }
    }

    pub fn validateHeader(self: Batch, max_changes: usize) !void {
        if (self.batch_id.len == 0 or self.batch_id.len > 256 or
            self.source.len == 0 or self.source.len > 256 or
            self.epoch.len == 0 or self.epoch.len > 256 or
            self.checkpoint.len == 0 or self.checkpoint.len > 1024 or
            self.key_fields.len == 0 or self.key_fields.len > 32 or
            self.changes.len > max_changes) return error.InvalidSourceTransaction;
        if (self.bootstrap and self.expected_checkpoint != null) return error.InvalidSourceTransaction;
        if (self.expected_checkpoint) |previous| {
            if (previous.len == 0 or previous.len > 1024 or std.mem.eql(u8, previous, self.checkpoint)) return error.InvalidSourceTransaction;
        }
        for (self.key_fields, 0..) |name, i| {
            if (name.len == 0) return error.InvalidSourceTransaction;
            for (self.key_fields[0..i]) |other| if (std.mem.eql(u8, name, other)) return error.InvalidSourceTransaction;
        }
    }

    /// Checkpoints are opaque, so only equality is meaningful. This tests
    /// continuity and ownership, not ordering between provider offsets.
    pub fn follows(self: Batch, previous: Batch) bool {
        return std.mem.eql(u8, self.source, previous.source) and
            std.mem.eql(u8, self.epoch, previous.epoch) and
            self.expected_checkpoint != null and
            std.mem.eql(u8, self.expected_checkpoint.?, previous.checkpoint) and
            keyFieldsEqual(self.key_fields, previous.key_fields);
    }

    /// Identity excludes the payload deliberately: the durable admission store
    /// must reject reuse of this identity with a different payload.
    pub fn identity(self: Batch, alloc: std.mem.Allocator) ![32]u8 {
        const bytes = try std.json.Stringify.valueAlloc(alloc, .{ self.source, self.epoch, self.batch_id }, .{});
        defer alloc.free(bytes);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        return digest;
    }
};

pub fn keyFieldsEqual(left: []const []const u8, right: []const []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| if (!std.mem.eql(u8, l, r)) return false;
    return true;
}

test "source transaction validates key images and opaque predecessor ownership" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(Batch, a,
        \\{"batch_id":"b","source":"pg","epoch":"slot-1","checkpoint":"z","expected_checkpoint":"a","key_fields":["id"],"changes":[{"op":"upsert","row":{"id":1,"name":"n"}}]}
    , .{});
    defer parsed.deinit();
    var next = parsed.value;
    try next.validate(1);
    var previous = next;
    previous.checkpoint = "a";
    try std.testing.expect(next.follows(previous));
    previous.epoch = "slot-2";
    try std.testing.expect(!next.follows(previous));
    previous.epoch = next.epoch;
    previous.key_fields = &.{"other"};
    try std.testing.expect(!next.follows(previous));
    next.expected_checkpoint = "z";
    try std.testing.expectError(error.InvalidSourceTransaction, next.validate(1));
    next = parsed.value;
    next.key_fields = &.{ "id", "id" };
    try std.testing.expectError(error.InvalidSourceTransaction, next.validate(1));
    next = parsed.value;
    next.key_fields = &.{"missing"};
    try std.testing.expectError(error.InvalidSourceTransaction, next.validate(1));
    next = parsed.value;
    try std.testing.expectError(error.InvalidSourceTransaction, next.validate(0));
}

test "source transaction identity preserves existing lake retry namespace" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(Batch, a,
        \\{"batch_id":"b","source":"pg","epoch":"1","checkpoint":"2","key_fields":["id"],"changes":[{"op":"delete","row":{"id":1}}]}
    , .{});
    defer parsed.deinit();
    try parsed.value.validate(1);
    const old_identity = try std.json.Stringify.valueAlloc(a, .{ parsed.value.source, parsed.value.epoch, parsed.value.batch_id }, .{});
    defer a.free(old_identity);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(old_identity, &digest, .{});
    try std.testing.expectEqual(digest, try parsed.value.identity(a));
    var retry = parsed.value;
    retry.checkpoint = "different-payload";
    try std.testing.expectEqual(digest, try retry.identity(a));
    retry.epoch = "reseeded";
    try std.testing.expect(!std.mem.eql(u8, &digest, &try retry.identity(a)));
}

test "source transaction checkpoint-only envelopes retain ownership and predecessor validation" {
    const batch: Batch = .{ .batch_id = "poll", .source = "pg", .epoch = "slot", .checkpoint = "next", .expected_checkpoint = "previous", .key_fields = &.{"id"}, .changes = &.{} };
    try batch.validate(0);
    var previous = batch;
    previous.checkpoint = "previous";
    try std.testing.expect(batch.follows(previous));
    var invalid = batch;
    invalid.batch_id = "";
    try std.testing.expectError(error.InvalidSourceTransaction, invalid.validate(0));
    invalid = batch;
    invalid.key_fields = &.{};
    try std.testing.expectError(error.InvalidSourceTransaction, invalid.validate(0));
    invalid = batch;
    invalid.expected_checkpoint = "";
    try std.testing.expectError(error.InvalidSourceTransaction, invalid.validate(0));
    previous.source = "other";
    try std.testing.expect(!batch.follows(previous));
    previous = batch;
    previous.epoch = "recreated-slot";
    try std.testing.expect(!batch.follows(previous));
}
