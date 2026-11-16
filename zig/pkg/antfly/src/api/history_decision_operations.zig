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

//! Signed private observation of the retained coordinator decision/admission.
const std = @import("std");
const local = @import("antfly_local_sources");
const operation = local.api_operation;
const metadata = @import("../metadata/api.zig");
pub const wire = local.storage_history_decision_wire;
pub const max_bytes = wire.max_request_bytes;
pub const max_reply_bytes = wire.max_reply_bytes;
pub const Reply = wire.OwnedReply;
pub const Scope = @import("history_publication_operations.zig").Scope;
pub const Request = struct {
    version: u8,
    owner: @FieldType(wire.Query, "owner"),
    transaction_id: [16]u8,
    minimum_applied_index: u64,
    pub fn validate(self: Request, namespace: [24]u8) !void {
        try self.binding().validate();
        if (!std.mem.eql(u8, &namespace, &self.owner.namespace)) return error.InvalidHistoryAdmission;
    }
    pub fn validateFence(self: Request, fence: metadata.CatalogRouteFence) !void {
        try fence.validate();
        try self.binding().validate();
        const incarnation = fence.metadata_incarnation orelse return error.ChangefeedCapabilityUnsupported;
        var namespace: [24]u8 = undefined;
        std.mem.writeInt(u64, namespace[0..8], fence.route.identity_namespace.table_id, .big);
        std.mem.writeInt(u64, namespace[8..16], fence.route.identity_namespace.shard_id, .big);
        std.mem.writeInt(u64, namespace[16..24], fence.route.identity_namespace.range_id, .big);
        if (self.owner.metadata_group_id != fence.metadata_group_id or !std.mem.eql(u8, &self.owner.metadata_incarnation, &incarnation) or
            !std.mem.eql(u8, &self.owner.namespace, &namespace) or self.owner.group_id != fence.route.group_id or self.owner.topology_epoch != fence.topology_epoch) return error.TopologyChanged;
    }
    pub fn binding(self: Request) wire.Query {
        return .{ .version = self.version, .owner = self.owner, .transaction_id = self.transaction_id, .minimum_applied_index = self.minimum_applied_index };
    }
};
/// HTTP requires an explicit envelope version even though the native helper
/// permits a default for trusted in-process query construction.
pub fn decodeReply(a: std.mem.Allocator, raw: []const u8, input: Request) !Reply {
    if (raw.len == 0 or raw.len > max_reply_bytes) return error.HistoryCorrupt;
    const Payload = struct {
        version: u8,
        request_digest: @FieldType(wire.Reply, "request_digest"),
        cut: @FieldType(wire.Reply, "cut"),
        decision: @FieldType(wire.Reply, "decision"),
    };
    var parsed = std.json.parseFromSlice(Payload, a, raw, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.HistoryCorrupt;
    errdefer parsed.deinit();
    const result: Reply = .{ .arena = parsed.arena, .value = .{ .version = parsed.value.version, .request_digest = parsed.value.request_digest, .cut = parsed.value.cut, .decision = parsed.value.decision } };
    result.value.validate(input.binding()) catch |err| return if (err == error.ChangefeedCapabilityUnsupported) err else error.HistoryCorrupt;
    return result;
}
pub const ErrorResponse = struct {
    version: u8,
    code: enum { invalid_request, resynchronization_required, topology_changed, unsupported, unavailable, corrupt_decision, internal_error },
    pub fn status(self: ErrorResponse) u16 {
        return switch (self.code) {
            .invalid_request => 400,
            .resynchronization_required, .topology_changed => 409,
            .unsupported => 501,
            .unavailable => 503,
            .corrupt_decision, .internal_error => 500,
        };
    }
    pub fn fromError(err: anyerror) ErrorResponse {
        return .{ .version = 1, .code = switch (err) {
            error.InvalidHistoryAdmission, error.InvalidCatalogRouteFence, error.UnsupportedCatalogRouteFence => .invalid_request,
            error.HistoryResynchronizationRequired, error.HistoryAuthorityRequired => .resynchronization_required,
            error.TopologyChanged, error.DocIdentityNamespaceMismatch, error.IdentityReadGenerationChanged => .topology_changed,
            error.ChangefeedCapabilityUnsupported => .unsupported,
            error.GroupLeaderUnavailable, error.LeaderUnavailable, error.NotLeader, error.ReadIndexTimeout, error.Timeout, error.ReadUnavailable, error.StorageBusy, error.StorageReadTemporarilyUnavailable, error.StorageKernelOwnerTransitionRequired, error.StorageOwnerAdmissionPending, error.ResourceBudgetExceeded, error.OutOfMemory, error.Canceled, error.Cancelled, error.DeadlineExceeded, error.CatalogRoutingUnavailable, error.CatalogRoutingSnapshotTimeout => .unavailable,
            error.InvalidTxnRecord, error.HistoryCorrupt, error.CorruptOrderedApplyReceipt, error.CorruptRaftAppliedEntry => .corrupt_decision,
            else => .internal_error,
        } };
    }
};
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
        try input.validateFence(fence);
        if (table.len == 0 or table.len > 1024) return error.InvalidCatalogRouteFence;
        var result = try Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, fence, table, input, bounded });
        errdefer result.deinit();
        try bounded.ensureActive();
        try result.value.validate(input.binding());
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
    if (input.owner.group_id != group) return error.InvalidHistoryAdmission;
    try input.validateFence(fence);
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    var result = try target.read(a, fence, table, input, context);
    errdefer result.deinit();
    try context.ensureActive();
    try result.value.validate(input.binding());
    return result;
}

test "history decision ingress frees owned replies on forged observations and cancellation" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        forge: bool = false,
        cancel_after: bool = false,
        cancelled: std.atomic.Value(bool) = .init(false),
        fn read(ptr: *anyopaque, alloc: std.mem.Allocator, _: metadata.CatalogRouteFence, _: []const u8, input: Request, _: operation.RequestContext) !Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const raw = try wire.encodeReply(alloc, input.binding(), .{ .request_digest = input.binding().digest(), .cut = .{ .term = 2, .index = 10 }, .decision = null });
            defer alloc.free(raw);
            var parsed = try decodeReply(alloc, raw, input);
            if (self.forge) parsed.value.request_digest[0] ^= 1;
            if (self.cancel_after) self.cancelled.store(true, .release);
            return parsed;
        }
    };
    var fake: Fake = .{};
    const port: Port = .{ .ptr = &fake, .read_fn = Fake.read };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 17, .big);
    std.mem.writeInt(u64, namespace[16..24], 19, .big);
    const input: Request = .{ .version = 1, .owner = .{ .metadata_group_id = 1, .metadata_incarnation = fence.metadata_incarnation.?, .namespace = namespace, .table_binding = @splat(2), .root_generation = 0, .group_id = 17, .topology_epoch = 1 }, .transaction_id = @splat(3), .minimum_applied_index = 1 };
    const body = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(body);
    const encoded_fence = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(encoded_fence);
    const context: operation.RequestContext = .{ .catalog_route_fence_json = encoded_fence, .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&fake.cancelled) };
    var reply = try execute(port, a, 17, "docs", body, context);
    defer reply.deinit();
    try std.testing.expect(reply.value.decision == null);
    fake.forge = true;
    try std.testing.expectError(error.HistoryCorrupt, execute(port, a, 17, "docs", body, context));
    fake.forge = false;
    fake.cancel_after = true;
    try std.testing.expectError(error.Canceled, execute(port, a, 17, "docs", body, context));
    fake.cancel_after = false;
    fake.cancelled.store(false, .release);
    const calls = fake.calls;
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, execute(null, a, 17, "docs", body, context));
    try std.testing.expectError(error.InvalidCatalogRouteFence, execute(port, a, 18, "docs", body, context));
    var replaced = fence;
    replaced.metadata_incarnation = "1123456789abcdef0123456789abcdef".*;
    try std.testing.expectError(error.TopologyChanged, port.read(a, replaced, "docs", input, context));
    try std.testing.expectEqual(calls, fake.calls);
    const oversized: [max_bytes + 1]u8 = @splat(' ');
    try std.testing.expectError(error.InvalidHistoryAdmission, execute(port, a, 17, "docs", &oversized, context));
    const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{body[0 .. body.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.InvalidHistoryAdmission, execute(port, a, 17, "docs", duplicate, context));
    const unknown = try std.fmt.allocPrint(a, "{s},\"unexpected\":true}}", .{body[0 .. body.len - 1]});
    defer a.free(unknown);
    try std.testing.expectError(error.InvalidHistoryAdmission, execute(port, a, 17, "docs", unknown, context));
    try std.testing.expectEqual(calls, fake.calls);
}
