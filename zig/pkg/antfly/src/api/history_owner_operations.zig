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

//! Current-owner qualification, bound to the original catalog route.
const std = @import("std");
const local = @import("antfly_local_sources");
const operation = local.api_operation;
const metadata = @import("../metadata/api.zig");
pub const wire = local.storage_history_admission_wire;
pub const max_bytes = wire.max_bytes;
pub const Scope = @import("history_publication_operations.zig").Scope;
pub const ErrorResponse = @import("history_publication_operations.zig").ErrorResponse;
pub const Request = struct {
    version: u8,
    expected_table_binding: [32]u8,
    pub fn validate(self: Request, _: [24]u8) !void {
        if (self.version != 1 or std.mem.allEqual(u8, &self.expected_table_binding, 0)) return error.InvalidHistoryAdmission;
    }
};
pub const Reply = struct {
    version: u8 = 1,
    owner: @FieldType(wire.Query, "owner"),
    observation: wire.Reply,
    pub fn validate(self: Reply, input: Request, fence: metadata.CatalogRouteFence) !void {
        try input.validate(self.owner.namespace);
        try fence.validate();
        try self.owner.validate();
        var namespace: [24]u8 = undefined;
        std.mem.writeInt(u64, namespace[0..8], fence.route.identity_namespace.table_id, .big);
        std.mem.writeInt(u64, namespace[8..16], fence.route.identity_namespace.shard_id, .big);
        std.mem.writeInt(u64, namespace[16..24], fence.route.identity_namespace.range_id, .big);
        const incarnation = fence.metadata_incarnation orelse return error.ChangefeedCapabilityUnsupported;
        if (self.version != 1 or self.owner.metadata_group_id != fence.metadata_group_id or
            !std.mem.eql(u8, &self.owner.metadata_incarnation, &incarnation) or
            !std.mem.eql(u8, &self.owner.namespace, &namespace) or
            !std.mem.eql(u8, &self.owner.table_binding, &input.expected_table_binding) or
            self.owner.group_id != fence.route.group_id or self.owner.topology_epoch != fence.topology_epoch)
            return error.TopologyChanged;
        try self.observation.validate(.{ .owner = self.owner, .minimum_applied_index = 1 });
    }
};
pub fn decodeReply(a: std.mem.Allocator, body: []const u8, input: Request, fence: metadata.CatalogRouteFence) !Reply {
    if (body.len > max_bytes) return error.InvalidRemoteResponse;
    var parsed = std.json.parseFromSlice(Reply, a, body, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRemoteResponse;
    defer parsed.deinit();
    try parsed.value.validate(input, fence);
    return parsed.value;
}
pub const Port = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, std.mem.Allocator, metadata.CatalogRouteFence, []const u8, Request, operation.RequestContext) anyerror!Reply,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { read: @FieldType(Port, "read_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    pub fn read(self: Port, a: std.mem.Allocator, fence: metadata.CatalogRouteFence, table: []const u8, input: Request, context: operation.RequestContext) !Reply {
        var scope = try Scope.init(fence, context);
        const bounded = scope.context();
        try bounded.ensureActive();
        try fence.validate();
        try input.validate(@splat(0));
        if (table.len == 0 or table.len > 1024) return error.InvalidCatalogRouteFence;
        const result = try Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, fence, table, input, bounded });
        try bounded.ensureActive();
        try result.validate(input, fence);
        return result;
    }
};
pub fn decode(a: std.mem.Allocator, body: []const u8, namespace: [24]u8) !Request {
    if (body.len > max_bytes) return error.InvalidHistoryAdmission;
    var parsed = std.json.parseFromSlice(Request, a, body, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidHistoryAdmission;
    defer parsed.deinit();
    try parsed.value.validate(namespace);
    return parsed.value;
}
pub fn execute(port: ?Port, a: std.mem.Allocator, group: u64, table: []const u8, body: []const u8, context: operation.RequestContext) !Reply {
    try context.ensureActive();
    if (group == 0 or table.len == 0 or table.len > 1024 or context.catalog_route_fence_json.len == 0 or context.catalog_route_fence_json.len > 16384) return error.InvalidCatalogRouteFence;
    var parsed = std.json.parseFromSlice(metadata.CatalogRouteFence, a, context.catalog_route_fence_json, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidCatalogRouteFence;
    defer parsed.deinit();
    var fence = parsed.value;
    try fence.validate();
    if (fence.route.group_id != group) return error.InvalidCatalogRouteFence;
    fence.admission_deadline_ns = context.deadline_ns;
    fence.admission_deadline_io = context.deadline_io;
    fence.admission_cancellation = context.cancellation;
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], fence.route.identity_namespace.table_id, .big);
    std.mem.writeInt(u64, namespace[8..16], fence.route.identity_namespace.shard_id, .big);
    std.mem.writeInt(u64, namespace[16..24], fence.route.identity_namespace.range_id, .big);
    const input = try decode(a, body, namespace);
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    const result = try target.read(a, fence, table, input, context);
    try context.ensureActive();
    try result.validate(input, fence);
    return result;
}

test "history owner ingress binds catalog identity and native observation" {
    const a = std.testing.allocator;
    const Fixture = struct {
        owner: @FieldType(Reply, "owner"),
        changed: bool = false,
        cancelled: std.atomic.Value(bool) = .init(false),
        cancel_after: bool = false,
        calls: usize = 0,
        fn read(ptr: *anyopaque, _: std.mem.Allocator, _: metadata.CatalogRouteFence, _: []const u8, _: Request, _: operation.RequestContext) !Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            var result: Reply = .{ .owner = self.owner, .observation = try wire.Reply.init(.{ .owner = self.owner }, .{ .term = 2, .index = 3 }, 1024) };
            if (self.changed) result.owner.table_binding[0] ^= 1;
            if (self.cancel_after) self.cancelled.store(true, .release);
            return result;
        }
    };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 17, .big);
    std.mem.writeInt(u64, namespace[16..24], 19, .big);
    var fixture: Fixture = .{ .owner = .{ .metadata_group_id = 1, .metadata_incarnation = fence.metadata_incarnation.?, .namespace = namespace, .table_binding = @splat(2), .root_generation = 0, .group_id = 17, .topology_epoch = 1 } };
    const input: Request = .{ .version = 1, .expected_table_binding = @splat(2) };
    const body = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(body);
    const encoded_fence = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(encoded_fence);
    const context: operation.RequestContext = .{ .catalog_route_fence_json = encoded_fence, .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&fixture.cancelled) };
    const port: Port = .{ .ptr = &fixture, .read_fn = Fixture.read };
    const reply = try execute(port, a, 17, "docs", body, context);
    try std.testing.expectEqual(fixture.owner, reply.owner);
    const encoded = try std.json.Stringify.valueAlloc(a, reply, .{});
    defer a.free(encoded);
    try std.testing.expectEqualDeep(reply, try decodeReply(a, encoded, input, fence));
    fixture.changed = true;
    try std.testing.expectError(error.TopologyChanged, execute(port, a, 17, "docs", body, context));
    fixture.changed = false;
    fixture.cancel_after = true;
    try std.testing.expectError(error.Canceled, execute(port, a, 17, "docs", body, context));
    fixture.cancelled.store(false, .release);
    fixture.cancel_after = false;
    const calls = fixture.calls;
    try std.testing.expectError(error.InvalidCatalogRouteFence, execute(port, a, 18, "docs", body, context));
    try std.testing.expectEqual(calls, fixture.calls);
    try std.testing.expectError(error.InvalidHistoryAdmission, decode(a, "{\"version\":1,\"version\":1}", namespace));
    const unknown = try std.fmt.allocPrint(a, "{s},\"unexpected\":true}}", .{body[0 .. body.len - 1]});
    defer a.free(unknown);
    try std.testing.expectError(error.InvalidHistoryAdmission, decode(a, unknown, namespace));
    const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{encoded[0 .. encoded.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.InvalidRemoteResponse, decodeReply(a, duplicate, input, fence));
    var replaced = fence;
    replaced.metadata_incarnation = "1123456789abcdef0123456789abcdef".*;
    try std.testing.expectError(error.TopologyChanged, reply.validate(input, replaced));
    var changed = reply;
    changed.owner.root_generation += 1;
    try std.testing.expectError(error.HistoryCorrupt, changed.validate(input, fence));
    changed = reply;
    changed.observation.retention_limit_bytes = 0;
    try std.testing.expectError(error.HistoryCorrupt, changed.validate(input, fence));
}
