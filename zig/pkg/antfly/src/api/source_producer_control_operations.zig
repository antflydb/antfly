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

//! Transport adapters authenticate service authority. This callback boundary
//! binds the complete route and qualified closure state. Accepted proposal
//! receipts survive late cancellation without becoming commit/drain proofs.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const wire = @import("../data/source_producer_control_http_wire.zig");
const operation = local.api_operation;
const metadata = @import("../metadata/api.zig");
const A = std.mem.Allocator;
pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, A, metadata.CatalogRouteFence, []const u8, wire.Request, operation.RequestContext) anyerror!wire.Reply,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { execute: @FieldType(Port, "execute_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    pub fn execute(self: Port, a: A, fence: metadata.CatalogRouteFence, table: []const u8, request: wire.Request, context: operation.RequestContext) !wire.Reply {
        return Boundary.call("execute", self.boundary_dispatch, self.execute_fn, .{ self.ptr, a, fence, table, request, context });
    }
};
pub fn execute(port: ?Port, a: A, group: u64, table: []const u8, body: []const u8, original: operation.RequestContext) !wire.Reply {
    try original.ensureActive();
    const request = try wire.decodeRequest(a, body);
    var route = try @import("source_owner_scope.zig").Scope.init(a, group, table, request.owner, original);
    defer route.deinit();
    var scope = try @import("history_publication_operations.zig").Scope.init(route.fence, original);
    const context = scope.context();
    try context.ensureActive();
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    var reply = target.execute(a, route.fence, table, request, context) catch |err| {
        if (err == error.RaftBatchWriteOutcomeUnknown) return wire.unknown(a, request);
        return err;
    };
    reply.validate(request, try request.digest(a)) catch return error.SourceOutcomeUnresolved;
    context.ensureActive() catch {
        reply.outcome = null;
        return reply;
    };
    return reply;
}

test "source producer control ingress preserves receipts and rejects forged authority" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        mode: enum { known, unknown, forged, cancel } = .known,
        cancelled: std.atomic.Value(bool) = .init(false),
        fn run(ptr: *anyopaque, alloc: A, _: metadata.CatalogRouteFence, _: []const u8, input: wire.Request, context: operation.RequestContext) !wire.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            self.calls += 1;
            if (self.mode == .unknown) return error.RaftBatchWriteOutcomeUnknown;
            var reply = try wire.test_support.closedReply(alloc, input);
            if (self.mode == .forged) reply.request_digest[0] ^= 1;
            if (self.mode == .cancel) self.cancelled.store(true, .release);
            return reply;
        }
    };
    const request = wire.test_support.fixture();
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = request.owner.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    const fence_json = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(fence_json);
    const body = try wire.encodeRequest(a, request);
    defer a.free(body);
    const context: operation.RequestContext = .{ .catalog_route_fence_json = fence_json };
    var fake: Fake = .{};
    const port: Port = .{ .ptr = &fake, .execute_fn = Fake.run };
    try std.testing.expectError(error.InvalidCatalogRouteFence, execute(port, a, 18, "docs", body, context));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, execute(null, a, 17, "docs", body, context));
    try std.testing.expect((try execute(port, a, 17, "docs", body, context)).closed());
    fake.mode = .unknown;
    try std.testing.expect((try execute(port, a, 17, "docs", body, context)).receipt == null);
    fake.mode = .forged;
    try std.testing.expectError(error.SourceOutcomeUnresolved, execute(port, a, 17, "docs", body, context));
    fake.mode = .cancel;
    var cancellable = context;
    cancellable.cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&fake.cancelled);
    const late = try execute(port, a, 17, "docs", body, cancellable);
    try std.testing.expect(late.outcome == null and late.receipt != null);
    const before = fake.calls;
    try std.testing.expectError(error.Canceled, execute(port, a, 17, "docs", body, cancellable));
    try std.testing.expectEqual(before, fake.calls);
}
