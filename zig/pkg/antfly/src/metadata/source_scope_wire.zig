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

//! Correlated, bounded metadata source-scope capture transport. A reply is an
//! observation, not caller-supplied authority. Serving adapters must establish
//! metadata ReadIndex and authenticate/revalidate the service and incarnation.
const std = @import("std");
pub const capture = @import("source_scope_capture.zig");
const A = std.mem.Allocator;
pub const max_request_bytes = 2048;
pub const max_reply_bytes = 8192;
pub const Request = struct {
    version: u8,
    query: capture.Query,
    pub fn validate(self: Request) !void {
        if (self.version != 1) return error.InvalidSourceControl;
        try self.query.validate();
    }
    pub fn digest(self: Request) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:metadata-source-scope-query:v1");
        hash.update(&.{self.version});
        var bytes: [8]u8 = undefined;
        for ([_]u64{ self.query.metadata_group_id, self.query.actor_table_id, self.query.minimum_applied_index }) |value| {
            std.mem.writeInt(u64, &bytes, value, .little);
            hash.update(&bytes);
        }
        hash.update(&self.query.metadata_incarnation);
        hash.update(&self.query.source_id);
        return hash.finalResult();
    }
};
pub const Reply = struct {
    version: u8,
    request_digest: [32]u8,
    snapshot: capture.Snapshot,
    pub fn validate(self: Reply, request: Request) !void {
        try request.validate();
        const query = request.query;
        const value = self.snapshot;
        if (self.version != 1 or !std.meta.eql(self.request_digest, request.digest()) or value.cut.group_id != query.metadata_group_id or !std.meta.eql(value.cut.incarnation, query.metadata_incarnation) or value.cut.index < query.minimum_applied_index or value.actor_revision == 0) return error.SourceControlCorrupt;
        value.spec.validate() catch return error.SourceControlCorrupt;
        if (value.spec.actor_table_id != query.actor_table_id or !std.meta.eql(value.spec.source_id, query.source_id) or value.spec.checkpoint_owner.metadata_group_id != query.metadata_group_id or !std.meta.eql(value.spec.checkpoint_owner.metadata_incarnation, query.metadata_incarnation)) return error.SourceControlCorrupt;
        if (value.checkpoint_table.len == 0 or value.checkpoint_table.len > 255 or !std.unicode.utf8ValidateSlice(value.checkpoint_table) or std.mem.indexOfScalar(u8, value.checkpoint_table, 0) != null) return error.SourceControlCorrupt;
        if (value.pending) |pending| {
            pending.reference.validate() catch return error.SourceControlCorrupt;
            if (value.phase == .retired or !std.meta.eql(pending.reference.domain, value.spec.domain()) or !std.meta.eql(pending.reference.binding, value.spec.binding) or pending.reference.generation != value.spec.generation or std.mem.allEqual(u8, &pending.transaction_id, 0) or std.mem.allEqual(u8, &pending.purpose, 0) or std.mem.allEqual(u8, &pending.reservation, 0) or std.mem.allEqual(u8, &pending.plan_digest, 0) or std.mem.allEqual(u8, &pending.artifact_plan_digest, 0)) return error.SourceControlCorrupt;
        }
    }
};
pub fn decodeRequest(a: A, bytes: []const u8) !Request {
    if (bytes.len == 0 or bytes.len > max_request_bytes) return error.InvalidSourceControl;
    var parsed = std.json.parseFromSlice(Request, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceControl;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn encodeRequest(a: A, request: Request) ![]u8 {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_request_bytes) return error.InvalidSourceControl;
    return bytes;
}
pub fn encodeReply(a: A, request: Request, snapshot: capture.Snapshot) ![]u8 {
    const reply: Reply = .{ .version = 1, .request_digest = request.digest(), .snapshot = snapshot };
    try reply.validate(request);
    const bytes = try std.json.Stringify.valueAlloc(a, reply, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_reply_bytes) return error.SourceControlCorrupt;
    return bytes;
}
/// Returns an independently owned snapshot. No parsed response buffers escape;
/// callers use Snapshot.deinit, exactly as with local applied-store capture.
pub fn decodeReply(a: A, request: Request, bytes: []const u8) !capture.Snapshot {
    if (bytes.len == 0 or bytes.len > max_reply_bytes) return error.SourceControlCorrupt;
    var parsed = std.json.parseFromSlice(Reply, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceControlCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(request);
    var value = parsed.value.snapshot;
    value.checkpoint_table = try a.dupe(u8, value.checkpoint_table);
    return value;
}
fn fixture() Request {
    return .{ .version = 1, .query = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .actor_table_id = 7, .source_id = @splat(2), .minimum_applied_index = 10 } };
}
fn observation() capture.Snapshot {
    const request = fixture();
    return .{ .cut = .{ .group_id = request.query.metadata_group_id, .incarnation = request.query.metadata_incarnation, .index = 11 }, .spec = .{ .actor_table_id = request.query.actor_table_id, .source_id = request.query.source_id, .generation = 3, .binding = @splat(4), .checkpoint_owner = .{ .metadata_group_id = 1, .metadata_incarnation = request.query.metadata_incarnation, .namespace = @splat(5), .root_generation = 0, .group_id = 17 } }, .phase = .active, .actor_revision = 4, .live_definition = true, .checkpoint_table = "checkpoint" };
}
test "metadata source scope wire owns decoded capture and binds every requested cut" {
    const a = std.testing.allocator;
    const request = fixture();
    const bytes = try encodeRequest(a, request);
    defer a.free(bytes);
    try std.testing.expectEqualDeep(request, try decodeRequest(a, bytes));
    const raw = try encodeReply(a, request, observation());
    var decoded = try decodeReply(a, request, raw);
    defer decoded.deinit(a);
    a.free(raw);
    try std.testing.expectEqualStrings("checkpoint", decoded.checkpoint_table);
    try std.testing.expectEqualDeep(observation().spec, decoded.spec);
    var reply: Reply = .{ .version = 1, .request_digest = request.digest(), .snapshot = observation() };
    var changed = request;
    changed.query.minimum_applied_index += 1;
    try std.testing.expectError(error.SourceControlCorrupt, reply.validate(changed));
    reply.snapshot.cut.index = 9;
    try std.testing.expectError(error.SourceControlCorrupt, reply.validate(request));
    reply.snapshot = observation();
    reply.snapshot.spec.source_id[0] ^= 1;
    try std.testing.expectError(error.SourceControlCorrupt, reply.validate(request));
    try std.testing.expectError(error.InvalidSourceControl, decodeRequest(a, "{\"version\":1,\"version\":1}"));
}
test "metadata source scope wire never turns inconsistent pending identity into authority" {
    const request = fixture();
    var reply: Reply = .{ .version = 1, .request_digest = request.digest(), .snapshot = observation() };
    reply.snapshot.pending = .{ .reference = .{ .domain = reply.snapshot.spec.domain(), .binding = reply.snapshot.spec.binding, .generation = reply.snapshot.spec.generation, .token = 1, .sequence = 1 }, .transaction_id = @splat(6), .purpose = @splat(7), .reservation = @splat(8), .plan_digest = @splat(9), .artifact_plan_digest = @splat(10) };
    try reply.validate(request);
    reply.snapshot.phase = .retired;
    try std.testing.expectError(error.SourceControlCorrupt, reply.validate(request));
    reply.snapshot.phase = .closing;
    try reply.validate(request);
    reply.snapshot.pending.?.reference.generation += 1;
    try std.testing.expectError(error.SourceControlCorrupt, reply.validate(request));
}

test "metadata source scope wire authorizes ordered first bind from certified bootstrap" {
    const a = std.testing.allocator;
    const request = fixture();
    var snapshot = observation();
    const control = @import("antfly_local_sources").storage_source_scope_control;
    var intent: control.Intent = .{
        .version = 1,
        .owner = .{
            .metadata_group_id = 1,
            .metadata_incarnation = request.query.metadata_incarnation,
            .namespace = snapshot.spec.checkpoint_owner.namespace,
            .table_binding = @splat(6),
            .root_generation = snapshot.spec.checkpoint_owner.root_generation,
            .group_id = 17,
            .topology_epoch = 1,
        },
        .spec = snapshot.spec,
        .minimum_metadata_index = request.query.minimum_applied_index,
        .action = .bind,
    };
    const legacy = try encodeReply(a, request, snapshot);
    defer a.free(legacy);
    try std.testing.expect(std.mem.indexOf(u8, legacy, "native_bootstrap_certified") == null);
    try std.testing.expectEqual(@as(u8, 1), (try snapshot.authorize(intent)).version);
    snapshot.native_bootstrap_certified = true;
    const raw = try encodeReply(a, request, snapshot);
    defer a.free(raw);
    var captured = try decodeReply(a, request, raw);
    defer captured.deinit(a);
    try std.testing.expectEqual(@as(u8, 2), (try captured.authorize(intent)).version);
    captured.phase = .closing;
    try std.testing.expectError(error.SourceBindingConflict, captured.authorize(intent));
    intent.action = .close;
    try std.testing.expectEqual(@as(u8, 1), (try captured.authorize(intent)).version);
}
