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

//! Private original-owner publication observations; absence remains unproven.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const wire = local.storage_history_publication_wire;
const operation = local.api_operation;
const metadata = @import("../metadata/api.zig");
pub const max_bytes = wire.publication_read_max_bytes;
pub const Reply = wire.CompletionPublicationObservation;
pub const Request = struct {
    version: u8,
    owner: @FieldType(wire.CompletionPublicationQuery, "owner"),
    transaction_id: [16]u8,
    manifest_digest: [32]u8,
    commit_version: u64,
    minimum_applied_index: u64,
    pub fn validate(self: Request, namespace: [24]u8) !void {
        try self.binding().validate();
        if (!std.mem.eql(u8, &namespace, &self.owner.namespace)) return error.InvalidHistoryAdmission;
    }
    pub fn binding(self: Request) wire.CompletionPublicationQuery {
        return .{ .version = self.version, .owner = self.owner, .transaction_id = self.transaction_id, .manifest_digest = self.manifest_digest, .commit_version = self.commit_version, .minimum_applied_index = self.minimum_applied_index };
    }
};
pub const ErrorResponse = struct {
    version: u8,
    code: enum { invalid_request, resynchronization_required, topology_changed, unsupported, unavailable, corrupt_publication, internal_error },
    pub fn status(self: ErrorResponse) u16 {
        return switch (self.code) {
            .invalid_request => 400,
            .resynchronization_required, .topology_changed => 409,
            .unsupported => 501,
            .unavailable => 503,
            .corrupt_publication, .internal_error => 500,
        };
    }
    pub fn fromError(err: anyerror) ErrorResponse {
        return .{ .version = 1, .code = switch (err) {
            error.InvalidHistoryAdmission, error.InvalidCatalogRouteFence, error.UnsupportedCatalogRouteFence => .invalid_request,
            error.HistoryResynchronizationRequired, error.HistoryAuthorityRequired => .resynchronization_required,
            error.TopologyChanged, error.DocIdentityNamespaceMismatch, error.IdentityReadGenerationChanged => .topology_changed,
            error.ChangefeedCapabilityUnsupported => .unsupported,
            error.GroupLeaderUnavailable, error.LeaderUnavailable, error.NotLeader, error.ReadIndexTimeout, error.Timeout, error.ReadUnavailable, error.StorageBusy, error.StorageReadTemporarilyUnavailable, error.StorageKernelOwnerTransitionRequired, error.StorageOwnerAdmissionPending, error.ResourceBudgetExceeded, error.OutOfMemory, error.Canceled, error.Cancelled, error.DeadlineExceeded, error.CatalogRoutingUnavailable, error.CatalogRoutingSnapshotTimeout => .unavailable,
            error.InvalidTxnRecord, error.HistoryCorrupt, error.CorruptOrderedApplyReceipt, error.CorruptRaftAppliedEntry => .corrupt_publication,
            else => .internal_error,
        } };
    }
};
/// Owns the combined cancellation borrow for one synchronous read. Normalize
/// both original clocks once and count validation work against the same budget.
pub const Scope = struct {
    base: operation.RequestContext,
    first: @import("antfly_cancellation").CancellationToken,
    second: @import("antfly_cancellation").CancellationToken,
    deadline_ns: u64,
    pub fn init(fence: metadata.CatalogRouteFence, original: operation.RequestContext) !Scope {
        const started = @import("antfly_platform").time.monotonicNs();
        const base = try original.platformDeadline();
        const route = try (operation.RequestContext{ .deadline_ns = fence.admission_deadline_ns, .deadline_io = fence.admission_deadline_io, .cancellation = fence.admission_cancellation }).platformDeadline();
        const deadline = @min(@min(base.deadline_ns orelse std.math.maxInt(u64), route.deadline_ns orelse std.math.maxInt(u64)), started +| 5 * std.time.ns_per_s);
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        return .{ .base = base, .first = base.cancellation, .second = if (std.meta.eql(base.cancellation, route.cancellation)) .none else route.cancellation, .deadline_ns = deadline };
    }
    pub fn context(self: *Scope) operation.RequestContext {
        var result = self.base;
        result.deadline_ns = self.deadline_ns;
        result.deadline_io = null;
        result.cancellation = .{ .ptr = self, .check_fn = check, .is_cancelled_fn = cancelled };
        return result;
    }
    fn check(ptr: *const anyopaque) !void {
        const self: *const Scope = @ptrCast(@alignCast(ptr));
        try self.first.check();
        try self.second.check();
        if (@import("antfly_platform").time.monotonicNs() >= self.deadline_ns) return error.DeadlineExceeded;
    }
    fn cancelled(ptr: *const anyopaque) bool {
        const self: *const Scope = @ptrCast(@alignCast(ptr));
        return self.first.isCancelled() or self.second.isCancelled();
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
        const result = try Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, fence, table, input, bounded });
        try bounded.ensureActive();
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
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    const result = try target.read(a, fence, table, input, context);
    try context.ensureActive();
    try result.validate(input.binding());
    return result;
}

test "history publication ingress binds receipts and withholds forged or cancelled results" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        forge: bool = false,
        cancel_after: bool = false,
        cancelled: std.atomic.Value(bool) = .init(false),
        fn read(ptr: *anyopaque, _: std.mem.Allocator, _: metadata.CatalogRouteFence, _: []const u8, input: Request, _: operation.RequestContext) !Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            var result = try Reply.init(input.binding(), .{ .term = 2, .index = 10 }, null);
            if (self.forge) result.request_digest[0] ^= 1;
            if (self.cancel_after) self.cancelled.store(true, .release);
            return result;
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
    var input: Request = .{ .version = 1, .owner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = namespace, .table_binding = @splat(2), .root_generation = 0, .group_id = 17, .topology_epoch = 1 }, .transaction_id = @splat(1), .manifest_digest = @splat(3), .commit_version = 4, .minimum_applied_index = 5 };
    const body = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(body);
    const context: operation.RequestContext = .{ .catalog_route_fence_json = fence_json };
    try std.testing.expectError(error.InvalidCatalogRouteFence, execute(port, a, 18, "docs", body, context));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, execute(null, a, 17, "docs", body, context));
    const result = try execute(port, a, 17, "docs", body, context);
    try result.validate(input.binding());
    try std.testing.expect(result.publication == null);
    fake.forge = true;
    try std.testing.expectError(error.HistoryCorrupt, execute(port, a, 17, "docs", body, context));
    fake.forge = false;
    fake.cancel_after = true;
    var cancelled = context;
    cancelled.cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&fake.cancelled);
    try std.testing.expectError(error.Canceled, execute(port, a, 17, "docs", body, cancelled));
    const calls = fake.calls;
    try std.testing.expectError(error.Canceled, execute(port, a, 17, "docs", body, cancelled));
    try std.testing.expectEqual(calls, fake.calls);
    input.owner.namespace[0] ^= 1;
    const wrong = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(wrong);
    try std.testing.expectError(error.InvalidHistoryAdmission, decode(a, wrong, namespace));
    input.owner.namespace = namespace;
    input.minimum_applied_index = 0;
    const native = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(native);
    try std.testing.expectError(error.InvalidHistoryAdmission, decode(a, native, namespace));
    try std.testing.expectError(error.InvalidHistoryAdmission, decode(a, "{}", namespace));
    const oversized: [max_bytes + 1]u8 = @splat(' ');
    try std.testing.expectError(error.InvalidHistoryAdmission, decode(a, &oversized, namespace));
}

test "history publication ingress preserves both original budget and fallible route scope" {
    const Token = @import("antfly_cancellation").CancellationToken;
    const platform = @import("antfly_platform").time;
    const Checks = struct {
        fn expired(_: *const anyopaque) !void {
            return error.DeadlineExceeded;
        }
        fn live(_: *const anyopaque) bool {
            return false;
        }
        fn count(ptr: *const anyopaque) !void {
            const counter: *usize = @ptrCast(@alignCast(@constCast(ptr)));
            counter.* += 1;
        }
    };
    var check_count: usize = 0;
    var fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    fence.admission_cancellation = .{ .ptr = &check_count, .check_fn = Checks.expired, .is_cancelled_fn = Checks.live };
    try std.testing.expectError(error.DeadlineExceeded, Scope.init(fence, .{}));
    fence.admission_cancellation = .none;
    fence.admission_deadline_ns = platform.monotonicNs() -| 1;
    try std.testing.expectError(error.DeadlineExceeded, Scope.init(fence, .{}));
    const now = platform.monotonicNs();
    fence.admission_deadline_ns = now + std.time.ns_per_s;
    const token: Token = .{ .ptr = &check_count, .check_fn = Checks.count, .is_cancelled_fn = Checks.live };
    fence.admission_cancellation = token;
    var scope = try Scope.init(fence, .{ .deadline_ns = now + 3 * std.time.ns_per_s, .cancellation = token });
    try std.testing.expectEqual(fence.admission_deadline_ns, scope.context().deadline_ns);
    const before = check_count;
    try scope.context().ensureActive();
    try std.testing.expectEqual(before + 1, check_count);
}
