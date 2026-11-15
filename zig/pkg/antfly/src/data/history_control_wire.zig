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

//! Bounded private node-to-node control contract. HTTP adapters must require
//! service authority and a catalog route fence before invoking an owner.
const std = @import("std");
const source = @import("antfly_local_sources").api_local_table_writes.table_write_source;
pub const max_bytes = 4096;
pub const Request = struct {
    version: u8,
    operation: enum { submit, recover },
    command: source.history_consumer.Command,
    receipt: ?source.history_ordered.Entry = null,
    pub fn validate(self: Request, namespace: [24]u8) !void {
        if (self.version != 1 or std.mem.allEqual(u8, &namespace, 0) or !std.mem.eql(u8, &namespace, &self.command.namespace())) return error.InvalidHistoryControlWire;
        switch (self.operation) {
            .submit => if (self.receipt != null) return error.InvalidHistoryControlWire,
            .recover => if (self.receipt) |entry| {
                if (entry.term == 0 or entry.index == 0) return error.InvalidHistoryControlWire;
            } else return error.InvalidHistoryControlWire,
        }
    }
};
pub const Response = struct {
    version: u8,
    receipt: ?source.history_ordered.Entry = null,
    outcome: ?source.history_ordered.Outcome = null,
    /// A known result may be a rejection; this is not permission to ACK.
    pub fn confirmed(self: Response) bool {
        return self.version == 1 and self.receipt != null and self.receipt.?.term != 0 and self.receipt.?.index != 0 and self.outcome != null and self.outcome.? != .superseded;
    }
    pub fn validate(self: Response, request: Request) !void {
        if (self.version != 1) return error.InvalidHistoryControlWire;
        if (self.receipt) |entry| {
            if (entry.term == 0 or entry.index == 0) return error.InvalidHistoryControlWire;
        } else if (self.outcome != null) return error.InvalidHistoryControlWire;
        if (request.operation == .recover and !std.meta.eql(self.receipt, request.receipt)) return error.InvalidHistoryControlWire;
        if (self.outcome) |outcome| if (outcome == .applied) {
            const result = outcome.applied;
            if (result.consumer) |consumer| if (consumer.epoch == 0 or consumer.expires_at_seconds == 0 or consumer.policy_revision == 0 or consumer.acknowledged < consumer.start) return error.InvalidHistoryControlWire;
            const reference: ?source.history_consumer.Reference = switch (request.command) {
                .inspect => |value| value,
                .acknowledge => |value| value.reference,
                .renew => |value| value.reference,
                .reclaim => |value| value.reference,
                else => null,
            };
            if (reference) |ref| {
                const consumer = result.consumer orelse return error.InvalidHistoryControlWire;
                if (consumer.epoch != ref.epoch or !std.mem.eql(u8, &consumer.id, &ref.consumer)) return error.InvalidHistoryControlWire;
            } else if (request.command == .enroll) {
                const consumer = result.consumer orelse return error.InvalidHistoryControlWire;
                if (consumer.epoch == 0 or !std.mem.eql(u8, &consumer.id, &request.command.enroll.consumer)) return error.InvalidHistoryControlWire;
            } else if (result.consumer != null) return error.InvalidHistoryControlWire;
            switch (request.command) {
                .acknowledge => |value| if (result.consumer.?.acknowledged < value.next) return error.InvalidHistoryControlWire,
                .renew => |value| if (result.consumer.?.expires_at_seconds < value.next) return error.InvalidHistoryControlWire,
                else => {},
            }
            const maximum: u8 = switch (request.command) {
                .collect => |value| value.max_frames,
                .reclaim => |value| value.max_frames,
                else => 0,
            };
            if (result.reclaimed_frames > maximum) return error.InvalidHistoryControlWire;
        };
    }
};
pub fn decodeRequest(a: std.mem.Allocator, bytes: []const u8, namespace: [24]u8) !Request {
    if (bytes.len > max_bytes) return error.InvalidHistoryControlWire;
    var parsed = try std.json.parseFromSlice(Request, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    try parsed.value.validate(namespace);
    return parsed.value;
}
pub fn decodeResponse(a: std.mem.Allocator, bytes: []const u8, request: Request) !Response {
    if (bytes.len > max_bytes) return error.InvalidHistoryControlWire;
    var parsed = try std.json.parseFromSlice(Response, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    try parsed.value.validate(request);
    return parsed.value;
}

test "history control entry remote wire distinguishes recovery and unknown outcomes" {
    const a = std.testing.allocator;
    var request: Request = .{ .version = 1, .operation = .submit, .command = .{ .collect = .{ .namespace = @splat(1), .authority = @splat(2) } } };
    const encoded = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(encoded);
    try std.testing.expectEqualDeep(request, try decodeRequest(a, encoded, @splat(1)));
    try std.testing.expectError(error.InvalidHistoryControlWire, decodeRequest(a, encoded, @splat(3)));
    request.operation = .recover;
    try std.testing.expectError(error.InvalidHistoryControlWire, request.validate(@splat(1)));
    request.receipt = .{ .term = std.math.maxInt(u64), .index = std.math.maxInt(u64) };
    const recovery = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(recovery);
    try std.testing.expectEqualDeep(request, try decodeRequest(a, recovery, @splat(1)));
    var response: Response = .{ .version = 1, .receipt = request.receipt };
    try std.testing.expect(!response.confirmed());
    response.outcome = .superseded;
    try std.testing.expect(!response.confirmed());
    response.outcome = .{ .rejected = .checkpoint_conflict };
    try std.testing.expect(response.confirmed());
    const result = try std.json.Stringify.valueAlloc(a, response, .{});
    defer a.free(result);
    try std.testing.expectEqualDeep(response, try decodeResponse(a, result, request));
    response.receipt = .{ .term = 1, .index = 2 };
    try std.testing.expectError(error.InvalidHistoryControlWire, response.validate(request));
    response.receipt = null;
    try std.testing.expectError(error.InvalidHistoryControlWire, response.validate(request));
    const extra = try std.fmt.allocPrint(a, "{s},\"unexpected\":true}}", .{encoded[0 .. encoded.len - 1]});
    defer a.free(extra);
    try std.testing.expectError(error.UnknownField, decodeRequest(a, extra, @splat(1)));
    const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{encoded[0 .. encoded.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.DuplicateField, decodeRequest(a, duplicate, @splat(1)));
    try std.testing.expectError(error.MissingField, decodeResponse(a, "{}", request));
    const oversized: [max_bytes + 1]u8 = @splat(' ');
    try std.testing.expectError(error.InvalidHistoryControlWire, decodeRequest(a, &oversized, @splat(1)));
    try std.testing.checkAllAllocationFailures(a, decodeBoth, .{ recovery, result });
}

fn decodeBoth(a: std.mem.Allocator, request_bytes: []const u8, response_bytes: []const u8) !void {
    const request = try decodeRequest(a, request_bytes, @splat(1));
    _ = try decodeResponse(a, response_bytes, request);
}

test "history control entry remote wire binds applied consumer and reclamation bounds" {
    var request: Request = .{ .version = 1, .operation = .submit, .command = .{ .collect = .{ .namespace = @splat(1), .authority = @splat(2), .max_frames = 1 } } };
    var response: Response = .{ .version = 1, .receipt = .{ .term = 1, .index = 2 }, .outcome = .{ .applied = .{ .reclaimed_frames = 1 } } };
    try response.validate(request);
    response.outcome.?.applied.reclaimed_frames = 2;
    try std.testing.expectError(error.InvalidHistoryControlWire, response.validate(request));
    request.command = .{ .enroll = .{ .namespace = @splat(1), .authority = @splat(2), .consumer = @splat(3), .definition = @splat(4), .sequence = 1, .expires_at_seconds = 1000, .policy_revision = 1 } };
    response.outcome = .{ .applied = .{ .consumer = .{ .id = @splat(3), .epoch = 1, .expires_at_seconds = 1000, .policy_revision = 1 } } };
    try response.validate(request);
    response.outcome.?.applied.consumer.?.id[0] ^= 1;
    try std.testing.expectError(error.InvalidHistoryControlWire, response.validate(request));
    response.outcome.?.applied.consumer.?.id[0] ^= 1;
    request.command = .{ .acknowledge = .{ .reference = .{ .namespace = @splat(1), .authority = @splat(2), .consumer = @splat(3), .definition = @splat(4), .epoch = 1, .grant = 1 }, .previous = 0, .next = 9 } };
    try std.testing.expectError(error.InvalidHistoryControlWire, response.validate(request));
    response.outcome.?.applied.consumer.?.acknowledged = 9;
    try response.validate(request);
    response.version = 2;
    try std.testing.expect(!response.confirmed());
}
