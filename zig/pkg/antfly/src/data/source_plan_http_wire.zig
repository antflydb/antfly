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

//! Private source-plan ingress. Submission and receipt recovery are distinct;
//! neither a response status nor an unsealed bitmap grants checkpoint authority.
const std = @import("std");
pub const entry = @import("source_plan_entry.zig");
pub const native = entry.wire;
pub const control = native.control;
pub const Owner = @FieldType(entry.Entry, "source_owner");
const A = std.mem.Allocator;
pub const max_bytes = native.max_bytes;
pub const max_control_reply_bytes = 4096;
pub fn requireRoot(owner: Owner, root: control.storage.Root) !void {
    if (!std.meta.eql(try entry.rootFor(owner), root)) return error.InvalidSourcePlanWire;
}
pub const ControlRequest = struct {
    version: u8,
    operation: enum { submit, recover },
    source_owner: Owner,
    command: native.Command,
    receipt: ?control.Entry = null,
    pub fn validate(self: ControlRequest, a: A) !void {
        if (self.version != 1) return error.InvalidSourcePlanWire;
        try requireRoot(self.source_owner, self.command.root);
        var decoded = try native.fromCommand(a, self.command);
        defer decoded.deinit();
        switch (self.operation) {
            .submit => if (self.receipt != null) return error.InvalidSourcePlanWire,
            .recover => if (self.receipt) |receipt| {
                if (receipt.term == 0 or receipt.index == 0) return error.InvalidSourcePlanWire;
            } else return error.InvalidSourcePlanWire,
        }
    }
    pub fn digest(self: ControlRequest, a: A) !control.Digest {
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        defer a.free(bytes);
        if (bytes.len > max_bytes) return error.InvalidSourcePlanWire;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-plan-http-control:v1");
        hash.update(bytes);
        return hash.finalResult();
    }
};
pub const ControlReply = struct {
    version: u8 = 1,
    request_digest: control.Digest,
    receipt: ?control.Entry = null,
    outcome: ?control.Outcome = null,
    pub fn confirmed(self: ControlReply) bool {
        return self.version == 1 and self.receipt != null and self.receipt.?.term != 0 and self.receipt.?.index != 0 and self.outcome != null and self.outcome.? != .superseded;
    }
    pub fn validate(self: ControlReply, request: ControlRequest, expected_digest: control.Digest) !void {
        if (self.version != 1 or !std.mem.eql(u8, &self.request_digest, &expected_digest)) return error.InvalidSourcePlanWire;
        if (self.receipt) |receipt| {
            if (receipt.term == 0 or receipt.index == 0) return error.InvalidSourcePlanWire;
        } else if (self.outcome != null) return error.InvalidSourcePlanWire;
        if (request.operation == .recover and !std.meta.eql(request.receipt, self.receipt)) return error.InvalidSourcePlanWire;
        if (self.outcome) |outcome| if (outcome == .applied) {
            const receipt = self.receipt.?;
            var cut: control.Digest = @splat(0);
            std.mem.writeInt(u64, cut[0..8], receipt.term, .little);
            std.mem.writeInt(u64, cut[8..16], receipt.index, .little);
            cut[31] = 1;
            const status = outcome.applied;
            status.validateEvidence(request.command.root, request.command.spec, cut) catch return error.InvalidSourcePlanWire;
            switch (request.command.action) {
                .begin => if (!status.present or status.fence != null) return error.InvalidSourcePlanWire,
                .chunk => {
                    _ = request.command.spec.chunkLen(request.command.ordinal) catch return error.InvalidSourcePlanWire;
                    const ordinal = request.command.ordinal;
                    if (!status.present or status.fence != null or status.received[ordinal / 8] & (@as(u8, 1) << @as(u3, @intCast(ordinal % 8))) == 0) return error.InvalidSourcePlanWire;
                },
                .seal => if (!status.present or status.fence != null or status.seal == null) return error.InvalidSourcePlanWire,
                .fence, .reclaim => if (status.fence == null and status.reclamation == null) return error.InvalidSourcePlanWire,
            }
        };
    }
};
pub fn decodeControlReply(a: A, request: ControlRequest, bytes: []const u8) !ControlReply {
    try request.validate(a);
    if (bytes.len > max_control_reply_bytes) return error.InvalidSourcePlanWire;
    var parsed = try std.json.parseFromSlice(ControlReply, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    try parsed.value.validate(request, try request.digest(a));
    return parsed.value;
}
pub const ObserveRequest = struct {
    version: u8,
    source_owner: Owner,
    query: control.Query,
    pub fn validate(self: ObserveRequest) !void {
        if (self.version != 1) return error.InvalidSourcePlanWire;
        try requireRoot(self.source_owner, self.query.root);
        try self.query.validate();
    }
};
pub const ChunkRequest = struct {
    version: u8,
    source_owner: Owner,
    request: control.ReadRequest,
    pub fn validate(self: ChunkRequest) !void {
        if (self.version != 1) return error.InvalidSourcePlanWire;
        try requireRoot(self.source_owner, self.request.query.root);
        try self.request.validate();
    }
};
pub fn parse(comptime T: type, a: A, bytes: []const u8) !std.json.Parsed(T) {
    if (bytes.len > max_bytes) return error.InvalidSourcePlanWire;
    return std.json.parseFromSlice(T, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" });
}

pub const test_support = if (@import("builtin").is_test) struct {
    pub const Fixture = struct {
        source_owner: Owner,
        frame: [128]u8,
        hashes: [1]control.Digest,
        checksum: control.Digest,
        plan_digest: control.Digest,
        pub fn init() !Fixture {
            var namespace: [24]u8 = undefined;
            std.mem.writeInt(u64, namespace[0..8], 7, .big);
            std.mem.writeInt(u64, namespace[8..16], 17, .big);
            std.mem.writeInt(u64, namespace[16..24], 19, .big);
            var frame: [128]u8 = @splat(255);
            @memcpy(frame[0..4], "AFP1");
            std.mem.writeInt(u32, frame[4..8], 1, .little);
            std.mem.writeInt(u64, frame[8..16], 10, .little);
            std.mem.writeInt(u64, frame[16..24], 11, .little);
            std.mem.writeInt(u64, frame[24..32], 1, .little);
            const digest = try control.storage.frozenDigest(&frame);
            @memcpy(frame[32..64], &digest);
            var checksum: control.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(&frame, &checksum, .{});
            return .{ .source_owner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = namespace, .table_binding = @splat(3), .root_generation = 0, .group_id = 17, .topology_epoch = 1 }, .frame = frame, .hashes = .{control.storage.chunkDigest(0, &frame)}, .checksum = checksum, .plan_digest = digest };
        }
        pub fn spec(self: *const Fixture) !control.storage.Spec {
            const root = try entry.rootFor(self.source_owner);
            var attempt: [16]u8 = @splat(1);
            std.mem.writeInt(u64, attempt[0..8], 1, .big);
            return .{ .namespace = control.storage.namespaceFor(root, 1), .storage_epoch = 1, .binding = @splat(2), .domain = control.storage.domainFor(@splat(2)), .attempt = attempt, .bytes = self.frame.len, .checksum = self.checksum, .plan_digest = self.plan_digest, .chunk_hashes = &self.hashes };
        }
        pub fn begin(self: *const Fixture) !ControlRequest {
            return .{ .version = 1, .operation = .submit, .source_owner = self.source_owner, .command = .{ .version = 1, .root = try entry.rootFor(self.source_owner), .spec = try self.spec(), .action = .begin } };
        }
        pub fn cut() control.Digest {
            var value: control.Digest = @splat(0);
            std.mem.writeInt(u64, value[0..8], 1, .little);
            std.mem.writeInt(u64, value[8..16], 2, .little);
            value[31] = 1;
            return value;
        }
        pub fn sealed(self: *const Fixture, a: A) !control.Observation {
            var txn: control.storage.test_support.Txn = .{};
            defer txn.deinit();
            const request = try self.begin();
            try control.storage.stageBegin(a, &txn, request.command.root, request.command.spec);
            try control.storage.stageChunk(a, &txn, request.command.root, request.command.spec, 0, &self.frame);
            _ = try control.storage.stageSeal(a, &txn, request.command.root, request.command.spec, cut());
            return .{ .root = request.command.root, .cut = cut(), .quota = try control.storage.readQuota(&txn, request.command.root), .status = try control.storage.inspect(a, &txn, request.command.root, request.command.spec) };
        }
    };
} else struct {};

test "source plan HTTP control binds receipt operation and phase evidence" {
    const a = std.testing.allocator;
    var fixture = try test_support.Fixture.init();
    var request = try fixture.begin();
    try request.validate(a);
    const digest = try request.digest(a);
    var reply: ControlReply = .{ .request_digest = digest, .receipt = .{ .term = 1, .index = 2 }, .outcome = .{ .applied = .{ .present = true } } };
    try reply.validate(request, digest);
    try std.testing.expect(reply.confirmed());
    request.operation = .recover;
    try std.testing.expectError(error.InvalidSourcePlanWire, request.validate(a));
    request.receipt = reply.receipt;
    try request.validate(a);
    const recovery_digest = try request.digest(a);
    try std.testing.expect(!std.mem.eql(u8, &digest, &recovery_digest));
    try std.testing.expectError(error.InvalidSourcePlanWire, reply.validate(request, recovery_digest));
    reply.request_digest = recovery_digest;
    try reply.validate(request, recovery_digest);
    reply.receipt.?.index += 1;
    try std.testing.expectError(error.InvalidSourcePlanWire, reply.validate(request, recovery_digest));
    reply.receipt = request.receipt;
    request.command.action = .seal;
    reply.request_digest = try request.digest(a);
    try std.testing.expectError(error.InvalidSourcePlanWire, reply.validate(request, reply.request_digest));
    const sealed = try fixture.sealed(a);
    reply.outcome = .{ .applied = sealed.status.? };
    try reply.validate(request, reply.request_digest);
    reply.outcome.?.applied.seal.?[0] ^= 1;
    try std.testing.expectError(error.InvalidSourcePlanWire, reply.validate(request, reply.request_digest));
}

test "source plan HTTP reply validation bounds untrusted chunk ordinals" {
    const a = std.testing.allocator;
    var fixture = try test_support.Fixture.init();
    var request = try fixture.begin();
    request.command.action = .chunk;
    request.command.ordinal = std.math.maxInt(u16);
    const digest = try request.digest(a);
    const reply: ControlReply = .{ .request_digest = digest, .receipt = .{ .term = 1, .index = 2 }, .outcome = .{ .applied = .{ .present = true, .received = @splat(0) } } };
    try std.testing.expectError(error.InvalidSourcePlanWire, reply.validate(request, digest));
    const encoded = try std.json.Stringify.valueAlloc(a, reply, .{});
    defer a.free(encoded);
    try std.testing.expectError(error.InvalidSourceAdmission, decodeControlReply(a, request, encoded));
}
