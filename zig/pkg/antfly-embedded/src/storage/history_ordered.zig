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

//! Atomic result of the latest ordered retention command. This is a bounded
//! replay aid, not an unbounded command journal or a feed delivery cursor.
//! After a newer control replaces it, recovery must inspect the live lease;
//! superseded never certifies a command's success or advances a public ACK.
const std = @import("std");
const control = @import("history_consumer.zig");
const history = @import("committed_history.zig");
pub const key = "\x00\x00__committed_history__:ordered_control";
pub const encoded_size = 192;
pub const Rejection = enum(u8) {
    invalid = 1,
    resynchronization = 2,
    consumer_conflict = 3,
    consumer_limit = 4,
    checkpoint_conflict = 5,
    full = 6,
    prepared_transactions = 7,
    pub fn fromError(err: anyerror) ?Rejection {
        return switch (err) {
            error.InvalidHistoryAdmission => .invalid,
            error.HistoryResynchronizationRequired => .resynchronization,
            error.HistoryConsumerConflict => .consumer_conflict,
            error.HistoryConsumerLimit => .consumer_limit,
            error.HistoryCheckpointConflict => .checkpoint_conflict,
            error.HistoryFull => .full,
            error.HistoryPreparedTransactionsPresent => .prepared_transactions,
            else => null,
        };
    }
    pub fn asError(self: Rejection) anyerror {
        return switch (self) {
            .invalid => error.InvalidHistoryAdmission,
            .resynchronization => error.HistoryResynchronizationRequired,
            .consumer_conflict => error.HistoryConsumerConflict,
            .consumer_limit => error.HistoryConsumerLimit,
            .checkpoint_conflict => error.HistoryCheckpointConflict,
            .full => error.HistoryFull,
            .prepared_transactions => error.HistoryPreparedTransactionsPresent,
        };
    }
};
pub const Outcome = union(enum) { applied: control.Result, rejected: Rejection, superseded };
pub const Entry = struct { term: u64, index: u64 };
pub const Record = struct {
    namespace: [24]u8,
    entry: Entry,
    command: [32]u8,
    outcome: Outcome,
    pub fn encode(self: Record) ![encoded_size]u8 {
        if (self.entry.term == 0 or self.entry.index == 0 or std.mem.allEqual(u8, &self.namespace, 0) or std.mem.allEqual(u8, &self.command, 0)) return error.HistoryCorrupt;
        var bytes: [encoded_size]u8 = @splat(0);
        @memcpy(bytes[0..4], "HCO1");
        @memcpy(bytes[8..32], &self.namespace);
        std.mem.writeInt(u64, bytes[32..40], self.entry.term, .little);
        std.mem.writeInt(u64, bytes[40..48], self.entry.index, .little);
        @memcpy(bytes[48..80], &self.command);
        switch (self.outcome) {
            .superseded => return error.HistoryCorrupt,
            .rejected => |reason| {
                bytes[4] = 1;
                bytes[5] = @backingInt(reason);
            },
            .applied => |result| {
                if (result.reclaimed_frames > 32) return error.HistoryCorrupt;
                bytes[7] = result.reclaimed_frames;
                if (result.consumer) |consumer| {
                    if (std.mem.allEqual(u8, &consumer.id, 0) or consumer.epoch == 0 or consumer.start > consumer.acknowledged or consumer.expires_at_seconds == 0 or consumer.policy_revision == 0) return error.HistoryCorrupt;
                    bytes[6] = 1;
                    @memcpy(bytes[80..96], &consumer.id);
                    for ([_]u64{ consumer.epoch, consumer.start, consumer.acknowledged, consumer.expires_at_seconds, consumer.policy_revision }, 0..) |value, i| std.mem.writeInt(u64, bytes[96 + i * 8 ..][0..8], value, .little);
                }
            },
        }
        std.crypto.hash.sha2.Sha256.hash(bytes[0..160], bytes[160..192], .{});
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Record {
        if (bytes.len != encoded_size or !std.mem.eql(u8, bytes[0..4], "HCO1") or !std.mem.allEqual(u8, bytes[136..160], 0)) return error.HistoryCorrupt;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[0..160], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[160..192])) return error.HistoryCorrupt;
        var record: Record = .{ .namespace = bytes[8..32].*, .entry = .{ .term = std.mem.readInt(u64, bytes[32..40], .little), .index = std.mem.readInt(u64, bytes[40..48], .little) }, .command = bytes[48..80].*, .outcome = .{ .applied = .{} } };
        switch (bytes[4]) {
            1 => {
                if (!std.mem.allEqual(u8, bytes[6..8], 0) or !std.mem.allEqual(u8, bytes[80..136], 0)) return error.HistoryCorrupt;
                record.outcome = .{ .rejected = std.enums.fromInt(Rejection, bytes[5]) orelse return error.HistoryCorrupt };
            },
            0 => {
                if (bytes[5] != 0 or bytes[6] > 1) return error.HistoryCorrupt;
                var result: control.Result = .{ .reclaimed_frames = bytes[7] };
                if (bytes[6] == 1) {
                    var consumer: history.Consumer = .{ .id = bytes[80..96].* };
                    inline for (.{ "epoch", "start", "acknowledged", "expires_at_seconds", "policy_revision" }, 0..) |name, i| @field(consumer, name) = std.mem.readInt(u64, bytes[96 + i * 8 ..][0..8], .little);
                    result.consumer = consumer;
                } else if (!std.mem.allEqual(u8, bytes[80..136], 0)) return error.HistoryCorrupt;
                record.outcome = .{ .applied = result };
            },
            else => return error.HistoryCorrupt,
        }
        // The encoder is also the structural validator and canonicality proof.
        if (!std.mem.eql(u8, bytes, &(try record.encode()))) return error.HistoryCorrupt;
        return record;
    }
};
pub fn fingerprint(a: std.mem.Allocator, command: control.Command) ![32]u8 {
    // Command has only fixed-size fields. Typed JSON gives deterministic field
    // order without hashing Zig padding or process-local union representation.
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:ordered-history-command:v1");
    hash.update(bytes);
    return hash.finalResult();
}
pub fn load(txn: anytype) !?Record {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try Record.decode(raw);
}
/// Read-only result recovery after a proposer loses its response. The owner
/// supplies a leader read cut at or beyond the original committed entry.
pub const Lookup = struct { command: control.Command, entry: Entry, minimum_applied_index: u64 };

pub fn replay(txn: anytype, namespace: [24]u8, entry: Entry, command: [32]u8) !Outcome {
    const record = try load(txn) orelse return .superseded;
    if (!std.mem.eql(u8, &record.namespace, &namespace)) return error.HistoryResynchronizationRequired;
    if (record.entry.index != entry.index) return .superseded;
    if (record.entry.term != entry.term or !std.mem.eql(u8, &record.command, &command)) return error.ConflictingOrderedApplyReceipt;
    return record.outcome;
}

test "ordered history receipt has bounded canonical authenticated outcomes" {
    const command: control.Command = .{ .collect = .{ .namespace = @splat(1), .authority = @splat(2) } };
    const hash = try fingerprint(std.testing.allocator, command);
    var record: Record = .{ .namespace = @splat(1), .entry = .{ .term = 1, .index = 2 }, .command = hash, .outcome = .{ .applied = .{ .reclaimed_frames = 1, .consumer = .{ .id = @splat(4), .epoch = 1, .expires_at_seconds = 1000, .policy_revision = 1 } } } };
    try std.testing.expectEqualDeep(record, try Record.decode(&(try record.encode())));
    for (std.enums.values(Rejection)) |reason| {
        record.outcome = .{ .rejected = reason };
        try std.testing.expectEqualDeep(record, try Record.decode(&(try record.encode())));
        try std.testing.expectEqual(reason, Rejection.fromError(reason.asError()).?);
    }
    var corrupt = try record.encode();
    corrupt[40] ^= 1;
    try std.testing.expectError(error.HistoryCorrupt, Record.decode(&corrupt));
    try std.testing.expect(Rejection.fromError(error.OutOfMemory) == null);
    try std.testing.expect(Rejection.fromError(error.HistoryCorrupt) == null);
    record.outcome = .superseded;
    try std.testing.expectError(error.HistoryCorrupt, record.encode());
}
