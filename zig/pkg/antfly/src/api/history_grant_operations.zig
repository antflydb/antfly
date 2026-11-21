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

//! Private read-only grant observation, not a command receipt or ACK grant.
const std = @import("std");
const local = @import("antfly_local_sources");
const control = local.api_local_table_writes.table_write_source.history_consumer;
pub const wire = local.api_local_table_writes.table_write_source.history_grant_wire;
const operation = local.api_operation;
const metadata = @import("../metadata/api.zig");
pub const max_bytes = wire.max_bytes;
pub const Request = struct {
    version: u8,
    identity: control.GrantIdentity,
    pub fn validate(self: Request, namespace: [24]u8) !void {
        try self.identity.validate();
        if (self.version != 1 or !std.mem.eql(u8, &namespace, &self.identity.namespace)) return error.InvalidHistoryAdmission;
    }
    /// HTTP always requires a current leader ReadIndex, stronger than this
    /// positive lower bound. A caller cannot select native/stale consistency.
    pub fn binding(self: Request) wire.Request {
        return .{ .version = self.version, .authority = .raft, .identity = self.identity, .minimum_applied_index = 1 };
    }
};
pub const ErrorResponse = struct {
    version: u8,
    code: enum { invalid_request, resynchronization_required, consumer_conflict, topology_changed, unsupported, unavailable, corrupt_observation, internal_error },
    pub fn status(self: ErrorResponse) u16 {
        return switch (self.code) {
            .invalid_request => 400,
            .resynchronization_required, .consumer_conflict, .topology_changed => 409,
            .unsupported => 501,
            .unavailable => 503,
            .corrupt_observation, .internal_error => 500,
        };
    }
    pub fn fromError(err: anyerror) ErrorResponse {
        return .{ .version = 1, .code = switch (err) {
            error.InvalidHistoryAdmission, error.InvalidCatalogRouteFence, error.UnsupportedCatalogRouteFence => .invalid_request,
            error.HistoryResynchronizationRequired, error.HistoryAuthorityRequired => .resynchronization_required,
            error.HistoryConsumerConflict => .consumer_conflict,
            error.TopologyChanged, error.DocIdentityNamespaceMismatch, error.IdentityReadGenerationChanged => .topology_changed,
            error.ChangefeedCapabilityUnsupported => .unsupported,
            error.GroupLeaderUnavailable, error.LeaderUnavailable, error.NotLeader, error.ReadIndexTimeout, error.Timeout, error.ReadUnavailable, error.StorageBusy, error.StorageReadTemporarilyUnavailable, error.StorageKernelOwnerTransitionRequired, error.StorageOwnerAdmissionPending, error.ResourceBudgetExceeded, error.OutOfMemory, error.Canceled, error.Cancelled, error.DeadlineExceeded, error.CatalogRoutingUnavailable, error.CatalogRoutingSnapshotTimeout => .unavailable,
            error.HistoryCorrupt, error.CorruptOrderedApplyReceipt, error.CorruptRaftAppliedEntry => .corrupt_observation,
            else => .internal_error,
        } };
    }
};
pub const Port = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, std.mem.Allocator, metadata.CatalogRouteFence, []const u8, Request, operation.RequestContext) anyerror!control.GrantObservation,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { read: @FieldType(Port, "read_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    pub fn read(self: Port, a: std.mem.Allocator, fence: metadata.CatalogRouteFence, table: []const u8, input: Request, context: operation.RequestContext) !control.GrantObservation {
        return Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, fence, table, input, context });
    }
};
pub fn decode(a: std.mem.Allocator, body: []const u8, namespace: [24]u8) !Request {
    if (body.len > max_bytes) return error.InvalidHistoryAdmission;
    var parsed = std.json.parseFromSlice(Request, a, body, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidHistoryAdmission;
    defer parsed.deinit();
    try parsed.value.validate(namespace);
    return parsed.value;
}
pub fn execute(port: ?Port, a: std.mem.Allocator, group: u64, table: []const u8, body: []const u8, context: operation.RequestContext) !wire.Reply {
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
    const observation = try target.read(a, fence, table, input, context);
    try context.ensureActive();
    return wire.Reply.init(input.binding(), observation);
}

test "history grant ingress binds scope and withholds invalid or cancelled observations" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        forged: bool = false,
        cancel_after: bool = false,
        cancelled: std.atomic.Value(bool) = .init(false),
        fn read(ptr: *anyopaque, _: std.mem.Allocator, _: metadata.CatalogRouteFence, table: []const u8, input: Request, _: operation.RequestContext) !control.GrantObservation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqualStrings("docs", table);
            if (self.cancel_after) self.cancelled.store(true, .release);
            return .{ .active = .{ .grant = input.identity.sequence + @as(u64, if (self.forged) 1 else 0), .limit = 1024 * 1024, .consumer = .{ .id = input.identity.consumer, .epoch = 2, .start = 3, .acknowledged = 3, .expires_at_seconds = 1000, .policy_revision = input.identity.policy_revision } } };
        }
    };
    var fake: Fake = .{};
    const port: Port = .{ .ptr = &fake, .read_fn = Fake.read };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    const fence_json = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(fence_json);
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 17, .big);
    std.mem.writeInt(u64, namespace[16..24], 19, .big);
    var input: Request = .{ .version = 1, .identity = .{ .namespace = namespace, .authority = @splat(1), .consumer = @splat(2), .definition = @splat(3), .sequence = 1, .policy_revision = 1 } };
    const body = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(body);
    const context: operation.RequestContext = .{ .catalog_route_fence_json = fence_json };
    try std.testing.expectError(error.InvalidCatalogRouteFence, execute(port, a, 18, "docs", body, context));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, execute(null, a, 17, "docs", body, context));
    const result = try execute(port, a, 17, "docs", body, context);
    try result.validate(input.binding());
    try std.testing.expectEqual(@as(u64, 2), result.observation.active.consumer.epoch);
    fake.forged = true;
    try std.testing.expectError(error.HistoryCorrupt, execute(port, a, 17, "docs", body, context));
    fake.forged = false;
    fake.cancel_after = true;
    var cancelled = context;
    cancelled.cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&fake.cancelled);
    try std.testing.expectError(error.Canceled, execute(port, a, 17, "docs", body, cancelled));
    const calls = fake.calls;
    try std.testing.expectError(error.Canceled, execute(port, a, 17, "docs", body, cancelled));
    try std.testing.expectEqual(calls, fake.calls);
    input.identity.namespace[0] ^= 1;
    const wrong = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(wrong);
    try std.testing.expectError(error.InvalidHistoryAdmission, execute(port, a, 17, "docs", wrong, context));
    try std.testing.expectEqual(calls, fake.calls);
    const oversized: [max_bytes + 1]u8 = @splat(' ');
    try std.testing.expectError(error.InvalidHistoryAdmission, decode(a, &oversized, namespace));
    try std.testing.expectEqual(.consumer_conflict, ErrorResponse.fromError(error.HistoryConsumerConflict).code);
}
