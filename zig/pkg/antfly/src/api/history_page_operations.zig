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

//! Private retained-page ingress. Raw history is service-only; feed owners
//! still owe public authorization, projection and global completion proofs.
const std = @import("std");
const local = @import("antfly_local_sources");
const source = local.api_local_table_writes.table_write_source;
const operation = local.api_operation;
const metadata = @import("../metadata/api.zig");
pub const pages = source.history_page;
pub const wire = source.history_page_wire;
pub const max_request_bytes = wire.max_request_bytes;
pub const max_bootstrap_request_bytes = 4096;
pub const Request = struct {
    version: u8,
    request: pages.Request,
    pub fn validate(self: Request, namespace: [24]u8) !void {
        const value = self.request;
        value.validateCompletionRoots() catch return error.InvalidHistoryPageRequest;
        if (self.version != 1 or std.mem.allEqual(u8, &namespace, 0) or !std.mem.eql(u8, &namespace, &value.reference.namespace) or
            value.reference.epoch == 0 or value.reference.grant == 0 or value.now_seconds == 0 or
            value.max_events == 0 or value.max_events > 1024 or value.max_bytes == 0 or value.max_bytes > pages.max_source_bytes) return error.InvalidHistoryPageRequest;
    }
};
pub const ErrorResponse = struct {
    version: u8,
    code: enum { invalid_request, resynchronization_required, topology_changed, page_budget_exceeded, unsupported, unavailable, corrupt_page, internal_error },
    pub fn status(self: ErrorResponse) u16 {
        return switch (self.code) {
            .invalid_request => 400,
            .resynchronization_required, .topology_changed => 409,
            .page_budget_exceeded => 422,
            .unsupported => 501,
            .unavailable => 503,
            .corrupt_page, .internal_error => 500,
        };
    }
    pub fn fromError(err: anyerror) ErrorResponse {
        return .{ .version = 1, .code = switch (err) {
            error.InvalidHistoryPageRequest, error.InvalidHistoryAdmission, error.InvalidCatalogRouteFence, error.UnsupportedCatalogRouteFence, error.UnknownField, error.DuplicateField, error.MissingField, error.UnexpectedToken, error.SyntaxError, error.Overflow => .invalid_request,
            error.HistoryResynchronizationRequired, error.HistoryConsumerConflict, error.HistoryCheckpointConflict => .resynchronization_required,
            error.TopologyChanged, error.DocIdentityNamespaceMismatch, error.IdentityReadGenerationChanged => .topology_changed,
            error.HistoryTooLarge => .page_budget_exceeded,
            error.ChangefeedCapabilityUnsupported => .unsupported,
            error.GroupLeaderUnavailable, error.LeaderUnavailable, error.NotLeader, error.ReadIndexTimeout, error.Timeout, error.ReadUnavailable, error.StorageBusy, error.IntegrityTopologyBusy, error.TransactionTopologyBusy, error.StorageReadTemporarilyUnavailable, error.StorageKernelOwnerTransitionRequired, error.StorageOwnerAdmissionPending, error.ResourceBudgetExceeded, error.OutOfMemory, error.Canceled, error.Cancelled, error.DeadlineExceeded, error.CatalogRoutingUnavailable, error.CatalogRoutingSnapshotTimeout => .unavailable,
            error.HistoryCorrupt, error.CorruptOrderedApplyReceipt, error.CorruptRaftAppliedEntry => .corrupt_page,
            else => .internal_error,
        } };
    }
};
pub const Port = PagePort(Request, pages.Page);

pub const Bootstrap = struct {
    pub const wire = source.history_bootstrap_wire;
    pub const Request = struct {
        version: u8,
        request: pages.BootstrapRequest,
        pub fn validate(self: @This(), namespace: [24]u8) !void {
            if (self.version != 1 or !std.mem.eql(u8, &namespace, &self.request.reference.namespace)) return error.InvalidHistoryPageRequest;
            try self.request.validate();
        }
    };
    pub const Port = PagePort(Bootstrap.Request, pages.BootstrapPage);

    pub fn execute(port: ?Bootstrap.Port, a: std.mem.Allocator, group: u64, table: []const u8, body: []const u8, context: operation.RequestContext) ![]u8 {
        return executePage(Bootstrap.Request, Bootstrap.wire, port, a, group, table, body, context);
    }
};

fn PagePort(comptime Input: type, comptime Page: type) type {
    return struct {
        ptr: *anyopaque,
        read_fn: *const fn (*anyopaque, std.mem.Allocator, metadata.CatalogRouteFence, []const u8, Input, operation.RequestContext) anyerror!Page,
        boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
        const Self = @This();
        pub const VTable = struct { read: @FieldType(Self, "read_fn") };
        const Boundary = local.runtime_callback_abi.Boundary(VTable);
        pub fn read(self: Self, a: std.mem.Allocator, fence: metadata.CatalogRouteFence, table: []const u8, input: Input, context: operation.RequestContext) !Page {
            if (comptime Input == Request) {
                const group = input.request.completion_local_group;
                if (group != 0 and group != fence.route.group_id) return error.InvalidHistoryPageRequest;
            }
            return Boundary.call("read", self.boundary_dispatch, self.read_fn, .{ self.ptr, a, fence, table, input, context });
        }
    };
}

/// The request may borrow allocations from the caller-owned arena.
pub fn decode(a: std.mem.Allocator, body: []const u8, namespace: [24]u8) !Request {
    return decodePage(Request, a, body, namespace);
}

fn decodePage(comptime Input: type, a: std.mem.Allocator, body: []const u8, namespace: [24]u8) !Input {
    if (body.len > (if (Input == Request) max_request_bytes else max_bootstrap_request_bytes)) return error.InvalidHistoryPageRequest;
    const parsed = std.json.parseFromSliceLeaky(Input, a, body, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidHistoryPageRequest;
    try parsed.validate(namespace);
    return parsed;
}

/// The caller owns returned HPG1 bytes. No result escapes after cancellation,
/// and encoding validates all page metadata against the original request.
pub fn execute(port: ?Port, a: std.mem.Allocator, group: u64, table: []const u8, body: []const u8, context: operation.RequestContext) ![]u8 {
    return executePage(Request, wire, port, a, group, table, body, context);
}

fn executePage(comptime Input: type, comptime Codec: type, port: anytype, a: std.mem.Allocator, group: u64, table: []const u8, body: []const u8, context: operation.RequestContext) ![]u8 {
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
    var request_arena = std.heap.ArenaAllocator.init(a);
    defer request_arena.deinit();
    const input = try decodePage(Input, request_arena.allocator(), body, namespace);
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    var page = try target.read(a, fence, table, input, context);
    defer page.deinit();
    try context.ensureActive();
    const encoded = try Codec.encode(a, page, input.request);
    errdefer a.free(encoded);
    try context.ensureActive();
    return encoded;
}

test "history page ingress rejects scope mismatches forged pages and late cancellation" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        forged: bool = false,
        cancel_after: bool = false,
        cancelled: std.atomic.Value(bool) = .init(false),
        fn read(ptr: *anyopaque, alloc: std.mem.Allocator, _: metadata.CatalogRouteFence, table: []const u8, input: Request, _: operation.RequestContext) !pages.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqualStrings("docs", table);
            if (self.cancel_after) self.cancelled.store(true, .release);
            return .{ .arena = .init(alloc), .next = .{ .sequence = input.request.after.sequence + @as(u64, if (self.forged) 1 else 0) }, .observed_latest = input.request.after.sequence, .caught_up = true };
        }
    };
    var fake: Fake = .{};
    const port: Port = .{ .ptr = &fake, .read_fn = Fake.read };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    const encoded_fence = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(encoded_fence);
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 17, .big);
    std.mem.writeInt(u64, namespace[16..24], 19, .big);
    var input: Request = .{ .version = 1, .request = .{ .reference = .{ .namespace = namespace, .authority = @splat(1), .definition = @splat(2), .consumer = @splat(3), .epoch = 1, .grant = 1 }, .after = .{}, .now_seconds = 100 } };
    const body = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(body);
    const context: operation.RequestContext = .{ .catalog_route_fence_json = encoded_fence };
    try std.testing.expectError(error.InvalidCatalogRouteFence, execute(port, a, 18, "docs", body, context));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, execute(null, a, 17, "docs", body, context));
    const encoded = try execute(port, a, 17, "docs", body, context);
    defer a.free(encoded);
    var page = try wire.decode(a, encoded, input.request);
    defer page.deinit();
    try std.testing.expect(page.caught_up);
    var projected = input;
    projected.request.include_completion = true;
    projected.request.completion_roots = &.{ namespace, @splat(8) };
    const projected_body = try std.json.Stringify.valueAlloc(a, projected, .{});
    defer a.free(projected_body);
    const projected_reply = try execute(port, a, 17, "docs", projected_body, context);
    defer a.free(projected_reply);
    var projected_page = try wire.decode(a, projected_reply, projected.request);
    defer projected_page.deinit();
    try std.testing.expect(projected_page.caught_up);
    projected.request.completion_roots = &.{ namespace, namespace };
    projected.request.completion_groups = &.{ 17, 18 };
    projected.request.completion_local_group = 18;
    const wrong_projection = try std.json.Stringify.valueAlloc(a, projected, .{});
    defer a.free(wrong_projection);
    const before_wrong_projection = fake.calls;
    try std.testing.expectError(error.InvalidHistoryPageRequest, execute(port, a, 17, "docs", wrong_projection, context));
    try std.testing.expectEqual(before_wrong_projection, fake.calls);
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
    input.request.reference.namespace[0] ^= 1;
    const wrong_scope = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(wrong_scope);
    try std.testing.expectError(error.InvalidHistoryPageRequest, execute(port, a, 17, "docs", wrong_scope, context));
    try std.testing.expectEqual(calls, fake.calls);
    const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{body[0 .. body.len - 1]});
    defer a.free(duplicate);
    var request_arena = std.heap.ArenaAllocator.init(a);
    defer request_arena.deinit();
    try std.testing.expectError(error.InvalidHistoryPageRequest, decode(request_arena.allocator(), duplicate, namespace));
    const oversized: [max_request_bytes + 1]u8 = @splat(' ');
    try std.testing.expectError(error.InvalidHistoryPageRequest, decode(request_arena.allocator(), &oversized, namespace));
    try std.testing.expectEqual(@as(u16, 409), ErrorResponse.fromError(error.TopologyChanged).status());
    try std.testing.expectEqual(.resynchronization_required, ErrorResponse.fromError(error.HistoryConsumerConflict).code);
    try std.testing.expectEqual(.page_budget_exceeded, ErrorResponse.fromError(error.HistoryTooLarge).code);
}

test "history page ingress bootstrap binds the pin and row position" {
    const a = std.testing.allocator;
    const Fixture = struct {
        fn read(_: *anyopaque, alloc: std.mem.Allocator, _: metadata.CatalogRouteFence, _: []const u8, input: Bootstrap.Request, _: operation.RequestContext) !pages.BootstrapPage {
            return .{ .arena = .init(alloc), .schema = "{}", .rows = &.{}, .next_row = input.request.after_rows, .done = true };
        }
    };
    var token: u8 = 0;
    const port: Bootstrap.Port = .{ .ptr = &token, .read_fn = Fixture.read };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    const encoded_fence = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(encoded_fence);
    const context: operation.RequestContext = .{ .catalog_route_fence_json = encoded_fence };
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 17, .big);
    std.mem.writeInt(u64, namespace[16..24], 19, .big);
    var input: Bootstrap.Request = .{ .version = 1, .request = .{ .reference = .{ .namespace = namespace, .authority = @splat(1), .definition = @splat(2), .consumer = @splat(3), .epoch = 1, .grant = 1 }, .digest = @splat(4), .now_seconds = 100, .after_rows = 12 } };
    const body = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(body);
    const encoded = try Bootstrap.execute(port, a, 17, "docs", body, context);
    defer a.free(encoded);
    var page = try Bootstrap.wire.decode(a, encoded, input.request);
    defer page.deinit();
    try std.testing.expectEqual(@as(u32, 12), page.next_row);
    try std.testing.expect(page.done);
    input.request.digest[0] ^= 1;
    try std.testing.expectError(error.HistoryCorrupt, Bootstrap.wire.decode(a, encoded, input.request));
}
