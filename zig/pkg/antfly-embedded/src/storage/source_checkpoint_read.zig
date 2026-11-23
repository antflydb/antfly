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

//! Request-bound checkpoint evidence at one qualified native snapshot.
//! Absence is unproven; local commitment is not global transaction completion.
const std = @import("std");
pub const checkpoint = @import("source_checkpoint.zig");
pub const completion = @import("transaction_completion.zig");
const A = std.mem.Allocator;
pub const max_request_bytes = 16384;
pub const max_reply_bytes = 8192;
pub const Cut = struct { term: u64, index: u64 };
pub const Request = struct {
    version: u8,
    source_owner: completion.RoutedOwner,
    transaction_id: [16]u8,
    command: checkpoint.Command,
    minimum_applied_index: u64,
    pub fn validate(self: Request) !void {
        if (self.version != 1 or self.minimum_applied_index == 0 or std.mem.allEqual(u8, &self.transaction_id, 0)) return error.InvalidSourceAdmission;
        self.source_owner.validate() catch return error.InvalidSourceAdmission;
        try self.command.validate();
    }
    pub fn digest(self: Request) checkpoint.Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-checkpoint-observation:v1");
        hash.update(&.{self.version});
        hash.update(&self.source_owner.digest());
        hash.update(&self.transaction_id);
        hash.update(&self.command.digest());
        var index: [8]u8 = undefined;
        std.mem.writeInt(u64, &index, self.minimum_applied_index, .little);
        hash.update(&index);
        return hash.finalResult();
    }
};
/// Fixed owned evidence crosses callbacks without allocator/lifetime coupling.
/// JSON uses canonical base64, not provider strings or large byte-array expansion.
pub const Evidence = struct {
    len: u16,
    bytes: [checkpoint.max_record_bytes]u8 = @splat(0),
    pub fn init(encoded: []const u8) !Evidence {
        _ = try checkpoint.Head.decode(encoded);
        var result: Evidence = .{ .len = @intCast(encoded.len) };
        @memcpy(result.bytes[0..encoded.len], encoded);
        return result;
    }
    pub fn raw(self: *const Evidence) ![]const u8 {
        if (self.len > self.bytes.len or !std.mem.allEqual(u8, self.bytes[self.len..], 0)) return error.SourceAdmissionCorrupt;
        return self.bytes[0..self.len];
    }
    pub fn decode(self: *const Evidence) !checkpoint.Head {
        return checkpoint.Head.decode(try self.raw());
    }
    pub fn jsonStringify(self: Evidence, stream: anytype) @TypeOf(stream.*).Error!void {
        // Only validated observations may reach transport serialization.
        var encoded: [std.base64.standard.Encoder.calcSize(checkpoint.max_record_bytes)]u8 = undefined;
        const length: usize = @min(self.len, self.bytes.len);
        try stream.write(std.base64.standard.Encoder.encode(&encoded, self.bytes[0..length]));
    }
    fn fromBase64(encoded: []const u8) !Evidence {
        if (encoded.len > std.base64.standard.Encoder.calcSize(checkpoint.max_record_bytes)) return error.SourceAdmissionCorrupt;
        const length = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.SourceAdmissionCorrupt;
        if (length > checkpoint.max_record_bytes) return error.SourceAdmissionCorrupt;
        var value: Evidence = .{ .len = @intCast(length) };
        std.base64.standard.Decoder.decode(value.bytes[0..length], encoded) catch return error.SourceAdmissionCorrupt;
        var canonical: [std.base64.standard.Encoder.calcSize(checkpoint.max_record_bytes)]u8 = undefined;
        if (!std.mem.eql(u8, std.base64.standard.Encoder.encode(&canonical, value.bytes[0..length]), encoded)) return error.SourceAdmissionCorrupt;
        _ = try value.decode();
        return value;
    }
};
pub const Disposition = enum { absent, committed, expired };
pub const Reply = struct {
    version: u8 = 1,
    request_digest: checkpoint.Digest,
    cut: Cut,
    disposition: Disposition,
    head: ?Evidence = null,
    receipt: ?Evidence = null,
    pub fn validate(self: *const Reply, request: Request) !void {
        try request.validate();
        if (self.version != 1 or !std.mem.eql(u8, &self.request_digest, &request.digest()) or self.cut.term == 0 or self.cut.index < request.minimum_applied_index) return error.SourceAdmissionCorrupt;
        if (self.head) |evidence| {
            const head = try evidence.decode();
            if (!std.mem.eql(u8, &head.binding, &request.command.binding)) return error.SourceAdmissionCorrupt;
            if (head.receipt_window != request.command.receipt_window) return error.SourceCheckpointConflict;
            if (head.sequence < request.command.sequence) {
                if (self.disposition != .absent or self.receipt != null) return error.SourceAdmissionCorrupt;
            } else if (head.sequence > request.command.sequence and (head.receipt_window == 0 or head.sequence - request.command.sequence >= head.receipt_window)) {
                if (self.disposition != .expired or self.receipt != null) return error.SourceAdmissionCorrupt;
            } else {
                const retained = self.receipt orelse return error.SourceAdmissionCorrupt;
                const receipt = try retained.decode();
                try receipt.requireRetry(request.transaction_id, request.command);
                if (self.disposition != .committed) return error.SourceAdmissionCorrupt;
                if (head.sequence == request.command.sequence and !std.mem.eql(u8, try evidence.raw(), try retained.raw())) return error.SourceAdmissionCorrupt;
            }
        } else if (self.receipt != null or self.disposition != .absent) return error.SourceAdmissionCorrupt;
    }
};
pub fn decodeReply(a: A, request: Request, bytes: []const u8) !Reply {
    if (bytes.len > max_reply_bytes) return error.SourceAdmissionCorrupt;
    const Json = struct { version: u8, request_digest: checkpoint.Digest, cut: Cut, disposition: Disposition, head: ?[]const u8 = null, receipt: ?[]const u8 = null };
    var parsed = std.json.parseFromSlice(Json, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    const value = parsed.value;
    const result: Reply = .{ .version = value.version, .request_digest = value.request_digest, .cut = value.cut, .disposition = value.disposition, .head = if (value.head) |head| try Evidence.fromBase64(head) else null, .receipt = if (value.receipt) |receipt| try Evidence.fromBase64(receipt) else null };
    try result.validate(request);
    return result;
}
pub fn read(txn: anytype, request: Request, cut: Cut) !Reply {
    try request.validate();
    var result: Reply = .{ .request_digest = request.digest(), .cut = cut, .disposition = .absent };
    const raw = txn.get(&checkpoint.key(request.command.binding)) catch |err| switch (@as(anyerror, err)) {
        error.NotFound => null,
        else => return err,
    };
    if (raw) |bytes| {
        // Own the head before any other lookup can reuse probe scratch.
        result.head = try Evidence.init(bytes);
        const head = try result.head.?.decode();
        if (!std.mem.eql(u8, &head.binding, &request.command.binding)) return error.SourceAdmissionCorrupt;
        if (head.receipt_window != request.command.receipt_window) return error.SourceCheckpointConflict;
        if (head.sequence >= request.command.sequence) {
            if (head.sequence == request.command.sequence) {
                result.receipt = result.head;
                result.disposition = .committed;
            } else if (head.receipt_window == 0 or head.sequence - request.command.sequence >= head.receipt_window) {
                result.disposition = .expired;
            } else {
                const key = try checkpoint.receiptKey(request.command.binding, request.command.sequence, head.receipt_window);
                const retained = txn.get(&key) catch |err| switch (@as(anyerror, err)) {
                    error.NotFound => return error.SourceAdmissionCorrupt,
                    else => return err,
                };
                result.receipt = try Evidence.init(retained);
                result.disposition = .committed;
            }
        }
    }
    try result.validate(request);
    return result;
}

test "source checkpoint observation owns binary receipts and rejects forged or expired evidence" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const Store = struct {
        values: std.StringHashMap([]const u8),
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.values.get(key) orelse error.NotFound;
        }
    };
    var store: Store = .{ .values = .init(scratch) };
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(2), .table_binding = @splat(3), .root_generation = 0, .group_id = 7, .topology_epoch = 1 };
    var requests: [4]Request = undefined;
    for (&requests, 0..) |*request, i| {
        request.* = .{ .version = 1, .source_owner = owner, .transaction_id = @splat(@as(u8, @intCast(10 + i))), .command = .{ .binding = @splat(5), .purpose = @splat(@as(u8, @intCast(20 + i))), .sequence = i + 1, .checkpoint = if (i % 2 == 0) "\xff\x00" else "other", .expected_checkpoint = if (i == 0) null else if (i % 2 == 0) "other" else "\xff\x00", .accepted_at_ns = 100 + i, .receipt_window = 3 }, .minimum_applied_index = 1 };
        const plan = try checkpoint.prepare(scratch, &store, request.transaction_id, request.command.purpose, request.command);
        try store.values.put(plan.key, plan.value);
        try store.values.put(plan.receipt_key.?, plan.value);
    }
    const cut: Cut = .{ .term = 2, .index = 10 };
    const observed = try read(&store, requests[1], cut);
    try std.testing.expectEqual(.committed, observed.disposition);
    try std.testing.expectEqual(@as(u64, 4), (try observed.head.?.decode()).sequence);
    try std.testing.expectEqual(@as(u64, 2), (try observed.receipt.?.decode()).sequence);
    const bytes = try std.json.Stringify.valueAlloc(a, observed, .{});
    defer a.free(bytes);
    const decoded = try decodeReply(a, requests[1], bytes);
    try std.testing.expectEqualDeep(observed, decoded);
    try std.testing.expect(bytes.len < max_reply_bytes);
    const binary = try read(&store, requests[2], cut);
    const binary_json = try std.json.Stringify.valueAlloc(a, binary, .{});
    defer a.free(binary_json);
    const binary_decoded = try decodeReply(a, requests[2], binary_json);
    try std.testing.expectEqualSlices(u8, "\xff\x00", (try binary_decoded.receipt.?.decode()).checkpoint);
    try std.testing.expectEqual(.expired, (try read(&store, requests[0], cut)).disposition);
    var next = requests[3];
    next.command.sequence = 5;
    next.command.expected_checkpoint = "other";
    try std.testing.expectEqual(.absent, (try read(&store, next, cut)).disposition);
    var forged = observed;
    forged.disposition = .absent;
    try std.testing.expectError(error.SourceAdmissionCorrupt, forged.validate(requests[1]));
    forged = observed;
    forged.cut.index = 0;
    try std.testing.expectError(error.SourceAdmissionCorrupt, forged.validate(requests[1]));
    forged = observed;
    forged.receipt.?.bytes[10] ^= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, forged.validate(requests[1]));
    var changed = requests[1];
    changed.command.expected_checkpoint = "altered";
    try std.testing.expectError(error.SourcePayloadConflict, read(&store, changed, cut));
    changed = requests[1];
    changed.source_owner.root_generation += 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeReply(a, changed, bytes));
    changed = requests[1];
    changed.command.receipt_window = 4;
    try std.testing.expectError(error.SourceCheckpointConflict, read(&store, changed, cut));
    const key = try checkpoint.receiptKey(requests[1].command.binding, 2, 3);
    _ = store.values.remove(&key);
    try std.testing.expectError(error.SourceAdmissionCorrupt, read(&store, requests[1], cut));
}

test "source checkpoint observation absence remains unproven and reply parsing is strict" {
    const a = std.testing.allocator;
    const Empty = struct {
        fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
    };
    var store: Empty = .{};
    const request: Request = .{ .version = 1, .source_owner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(2), .table_binding = @splat(3), .root_generation = 0, .group_id = 7, .topology_epoch = 1 }, .transaction_id = @splat(4), .command = .{ .binding = @splat(5), .purpose = @splat(6), .sequence = 1, .checkpoint = "first", .accepted_at_ns = 1, .receipt_window = 3 }, .minimum_applied_index = 1 };
    const result = try read(&store, request, .{ .term = 1, .index = 1 });
    try std.testing.expect(result.head == null and result.receipt == null and result.disposition == .absent);
    var forged = result;
    forged.disposition = .committed;
    try std.testing.expectError(error.SourceAdmissionCorrupt, forged.validate(request));
    const bytes = try std.json.Stringify.valueAlloc(a, result, .{});
    defer a.free(bytes);
    const unknown = try std.fmt.allocPrint(a, "{{\"unexpected\":1,{s}", .{bytes[1..]});
    defer a.free(unknown);
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeReply(a, request, unknown));
    const duplicate = try std.fmt.allocPrint(a, "{{\"version\":1,{s}", .{bytes[1..]});
    defer a.free(duplicate);
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeReply(a, request, duplicate));
    try std.testing.expectError(error.SourceAdmissionCorrupt, Evidence.fromBase64("bad padding"));
}
