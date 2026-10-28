// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Fixed-size v1 context carried INSIDE verified service credential metadata.
//! It is not authority when supplied as an unsigned/public request header.
const std = @import("std");
const attempts = @import("remote_attempts.zig");
pub const Kind = enum(u8) { read = 1, status = 2, fence = 3, cancel = 4, capabilities = 5 };
pub const encoded_size = 256;
const binary_size = 128;

pub const Context = struct {
    kind: Kind,
    id: attempts.Id,
    worker_incarnation: u128,
    remaining_ns: u64,
    charged_bytes: u64,
    request_digest: [32]u8,

    pub fn validate(self: Context) !void {
        if (self.id.coordinator == 0 or self.id.generation == 0 or self.worker_incarnation == 0) return error.InvalidRemoteAttempt;
        if (self.remaining_ns == 0) return error.DeadlineExceeded;
        switch (self.kind) {
            .fence, .capabilities => if (self.id.operation != 0 or self.id.sequence != 0 or self.charged_bytes != 0) return error.InvalidRemoteAttempt,
            .cancel => if (self.id.operation == 0 or self.id.sequence == 0 or self.id.sequence == std.math.maxInt(u64) or self.charged_bytes != 0) return error.InvalidRemoteAttempt,
            .status => if (self.id.operation == 0 or self.id.sequence == 0 or self.charged_bytes != 0) return error.InvalidRemoteAttempt,
            .read => if (self.id.operation == 0 or self.id.sequence == 0 or self.charged_bytes == 0) return error.InvalidRemoteAttempt,
        }
    }
    pub fn encode(self: Context) ![encoded_size]u8 {
        try self.validate();
        var bytes: [binary_size]u8 = @splat(0);
        @memcpy(bytes[0..4], "AFAT");
        std.mem.writeInt(u16, bytes[4..6], 1, .little);
        bytes[6] = @backingInt(self.kind);
        std.mem.writeInt(u128, bytes[8..24], self.id.coordinator, .little);
        std.mem.writeInt(u64, bytes[24..32], self.id.generation, .little);
        std.mem.writeInt(u128, bytes[32..48], self.id.operation, .little);
        std.mem.writeInt(u64, bytes[48..56], self.id.sequence, .little);
        std.mem.writeInt(u128, bytes[56..72], self.worker_incarnation, .little);
        std.mem.writeInt(u64, bytes[72..80], self.remaining_ns, .little);
        std.mem.writeInt(u64, bytes[80..88], self.charged_bytes, .little);
        @memcpy(bytes[88..120], &self.request_digest);
        return std.fmt.bytesToHex(bytes, .lower);
    }
    pub fn decode(encoded: []const u8) !Context {
        if (encoded.len != encoded_size) return error.InvalidRemoteAttempt;
        var bytes: [binary_size]u8 = undefined;
        _ = std.fmt.hexToBytes(&bytes, encoded) catch return error.InvalidRemoteAttempt;
        if (!std.mem.eql(u8, bytes[0..4], "AFAT") or std.mem.readInt(u16, bytes[4..6], .little) != 1 or bytes[7] != 0 or !std.mem.allEqual(u8, bytes[120..], 0)) return error.UnsupportedRemoteAttemptProtocol;
        const self = Context{
            .kind = std.enums.fromInt(Kind, bytes[6]) orelse return error.UnsupportedRemoteAttemptProtocol,
            .id = .{
                .coordinator = std.mem.readInt(u128, bytes[8..24], .little),
                .generation = std.mem.readInt(u64, bytes[24..32], .little),
                .operation = std.mem.readInt(u128, bytes[32..48], .little),
                .sequence = std.mem.readInt(u64, bytes[48..56], .little),
            },
            .worker_incarnation = std.mem.readInt(u128, bytes[56..72], .little),
            .remaining_ns = std.mem.readInt(u64, bytes[72..80], .little),
            .charged_bytes = std.mem.readInt(u64, bytes[80..88], .little),
            .request_digest = bytes[88..120].*,
        };
        try self.validate();
        return self;
    }
    pub fn subject(self: Context) [37]u8 {
        var bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &bytes, self.id.coordinator, .big);
        return "node:".* ++ std.fmt.bytesToHex(bytes, .lower);
    }
    /// The caller measures elapsed time against its ORIGINAL monotonic budget.
    /// Retransmission never gets a fresh duration or a fresh attempt identity.
    pub fn afterElapsed(self: Context, elapsed_ns: u64) !Context {
        var next = self;
        next.remaining_ns -|= elapsed_ns;
        try next.validate();
        return next;
    }
};

pub fn requestDigest(method: []const u8, target: []const u8, body: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ method, target, body }) |part| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, part.len, .little);
        hash.update(&length);
        hash.update(part);
    }
    return hash.finalResult();
}

/// The API authentication adapter fixes this deadline in platform monotonic
/// time, including credential verification. Pure codec callers supply their
/// explicitly chosen receiver clock; no absolute time is serialized.
pub const Verified = struct { context: Context, local_deadline_ns: u64 };

/// Reject attempt-bearing credentials on legacy RPC routes until they have a
/// physical ownership adapter. Decode keys structurally, including JSON escapes.
pub fn carriesContext(allocator: std.mem.Allocator, metadata_json: []const u8) !bool {
    if (metadata_json.len == 0 or std.mem.eql(u8, metadata_json, "{}")) return false;
    if (metadata_json.len > 1024) return error.InvalidRemoteAttempt;
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, metadata_json, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRemoteAttempt;
    defer parsed.deinit();
    return switch (parsed.value) {
        .object => |object| object.get("remote_attempt") != null,
        else => error.InvalidRemoteAttempt,
    };
}

/// All identity fields here must come from an already VERIFIED credential.
/// Membership authorization is a separate requirement; a signed node subject
/// does not prove the coordinator incarnation is current or permitted to run.
pub fn authorize(allocator: std.mem.Allocator, identity: anytype, expected_worker_incarnation: u128, expected_kind: Kind, method: []const u8, target: []const u8, body: []const u8, now_ns: u64, maximum_duration_ns: u64) !Verified {
    if (!identity.is_internal_service or identity.metadata_json.len > 1024) return error.Unauthorized;
    const Metadata = struct { remote_attempt: []const u8 };
    var metadata = std.json.parseFromSlice(Metadata, allocator, identity.metadata_json, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRemoteAttempt;
    defer metadata.deinit();
    const context = try Context.decode(metadata.value.remote_attempt);
    const expected_subject = context.subject();
    if (!std.mem.eql(u8, identity.username, &expected_subject) or context.worker_incarnation != expected_worker_incarnation or context.kind != expected_kind) return error.Unauthorized;
    if (!std.mem.eql(u8, &context.request_digest, &requestDigest(method, target, body))) return error.InvalidRemoteAttempt;
    if (maximum_duration_ns == 0) return error.InvalidRemoteAttempt;
    const duration = @min(context.remaining_ns, maximum_duration_ns);
    const deadline = std.math.add(u64, now_ns, duration) catch return error.InvalidRemoteAttempt;
    return .{ .context = context, .local_deadline_ns = deadline };
}

fn fixture() Context {
    return .{
        .kind = .read,
        .id = .{ .coordinator = 7, .generation = 9, .operation = std.math.maxInt(u128), .sequence = 4 },
        .worker_incarnation = 11,
        .remaining_ns = 100,
        .charged_bytes = 4096,
        .request_digest = requestDigest("POST", "/internal/v1/read", "{}"),
    };
}

test "remote attempt wire preserves exact identities and rejects unknown versions" {
    const context = fixture();
    var encoded = try context.encode();
    try std.testing.expectEqualDeep(context, try Context.decode(&encoded));
    encoded[8] = '2';
    try std.testing.expectError(error.UnsupportedRemoteAttemptProtocol, Context.decode(&encoded));
    try std.testing.expectError(error.InvalidRemoteAttempt, Context.decode("client hint"));
    const shortened = try context.afterElapsed(40);
    try std.testing.expectEqual(@as(u64, 60), shortened.remaining_ns);
    try std.testing.expectEqualDeep(context.id, shortened.id);
    try std.testing.expectError(error.DeadlineExceeded, shortened.afterElapsed(60));
}

test "remote attempt wire binds verified service identity target body and local duration" {
    const context = fixture();
    const encoded = try context.encode();
    const metadata = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .remote_attempt = @as([]const u8, &encoded) }, .{});
    defer std.testing.allocator.free(metadata);
    const subject = context.subject();
    const Identity = struct { is_internal_service: bool, username: []const u8, metadata_json: []const u8 };
    var identity: Identity = .{ .is_internal_service = true, .username = &subject, .metadata_json = metadata };
    const verified = try authorize(std.testing.allocator, identity, 11, .read, "POST", "/internal/v1/read", "{}", 9_000_000, 70);
    try std.testing.expectEqual(@as(u64, 9_000_070), verified.local_deadline_ns);
    try std.testing.expectError(error.Unauthorized, authorize(std.testing.allocator, identity, 22, .read, "POST", "/internal/v1/read", "{}", 1, 70));
    try std.testing.expectError(error.Unauthorized, authorize(std.testing.allocator, identity, 11, .fence, "POST", "/internal/v1/read", "{}", 1, 70));
    try std.testing.expectError(error.InvalidRemoteAttempt, authorize(std.testing.allocator, identity, 11, .read, "POST", "/internal/v1/write", "{}", 1, 70));
    try std.testing.expectError(error.InvalidRemoteAttempt, authorize(std.testing.allocator, identity, 11, .read, "POST", "/internal/v1/read", "changed", 1, 70));
    try std.testing.expectError(error.InvalidRemoteAttempt, authorize(std.testing.allocator, identity, 11, .read, "POST", "/internal/v1/read", "{}", std.math.maxInt(u64), 70));
    identity.username = "antfly-node";
    try std.testing.expectError(error.Unauthorized, authorize(std.testing.allocator, identity, 11, .read, "POST", "/internal/v1/read", "{}", 1, 70));
    identity.username = &subject;
    identity.is_internal_service = false;
    try std.testing.expectError(error.Unauthorized, authorize(std.testing.allocator, identity, 11, .read, "POST", "/internal/v1/read", "{}", 1, 70));
}

test "remote attempt wire detects context on legacy routes without string-key bypass" {
    try std.testing.expect(!try carriesContext(std.testing.allocator, "{}"));
    try std.testing.expect(try carriesContext(std.testing.allocator, "{\"remote_attempt\":null}"));
    try std.testing.expect(try carriesContext(std.testing.allocator, "{\"remote_\\u0061ttempt\":\"payload\"}"));
    try std.testing.expect(!try carriesContext(std.testing.allocator, "{\"description\":\"remote_attempt\"}"));
    try std.testing.expectError(error.InvalidRemoteAttempt, carriesContext(std.testing.allocator, "broken"));
}

test "remote attempt wire cancellation is exact and cannot cancel generation probes" {
    var context = fixture();
    context.kind = .cancel;
    context.charged_bytes = 0;
    const encoded = try context.encode();
    try std.testing.expectEqualDeep(context, try Context.decode(&encoded));
    context.id.sequence = std.math.maxInt(u64);
    try std.testing.expectError(error.InvalidRemoteAttempt, context.encode());
    context.kind = .status;
    _ = try context.encode();
}
