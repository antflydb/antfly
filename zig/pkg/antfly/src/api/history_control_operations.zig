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

//! Transport-neutral private history control. Service authentication belongs
//! to ingress; route and result binding remain mandatory at this boundary.
const std = @import("std");
const operation = @import("antfly_local_sources").api_operation;
const metadata = @import("../metadata/api.zig");
pub const wire = @import("../data/history_control_wire.zig");
pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, metadata.CatalogRouteFence, []const u8, wire.Request, operation.RequestContext) anyerror!wire.Response,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct { execute: @FieldType(Port, "execute_fn") };
    const Boundary = @import("antfly_local_sources").runtime_callback_abi.Boundary(VTable);
    pub fn execute(self: Port, a: std.mem.Allocator, fence: metadata.CatalogRouteFence, table: []const u8, input: wire.Request, context: operation.RequestContext) !wire.Response {
        return Boundary.call("execute", self.boundary_dispatch, self.execute_fn, .{ self.ptr, a, fence, table, input, context });
    }
};

pub fn execute(port: ?Port, a: std.mem.Allocator, group: u64, table: []const u8, body: []const u8, context: operation.RequestContext) !wire.Response {
    try context.ensureActive();
    if (group == 0 or table.len == 0 or table.len > 1024) return error.InvalidCatalogRouteFence;
    if (context.catalog_route_fence_json.len == 0 or context.catalog_route_fence_json.len > 16384) return error.InvalidCatalogRouteFence;
    var parsed = try std.json.parseFromSlice(metadata.CatalogRouteFence, a, context.catalog_route_fence_json, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" });
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
    const input = try wire.decodeRequest(a, body, namespace);
    const target = port orelse return error.ChangefeedCapabilityUnsupported;
    const result = target.execute(a, fence, table, input, context) catch |err| {
        if (err == error.RaftBatchWriteOutcomeUnknown) return .{ .version = 1, .receipt = input.receipt };
        return err;
    };
    result.validate(input) catch return error.InvalidHistoryControlResponse;
    return result;
}

test "history control ingress validates route before dispatch and preserves uncertainty" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        mode: enum { applied, unknown, malformed } = .applied,
        fn run(ptr: *anyopaque, _: std.mem.Allocator, fence: metadata.CatalogRouteFence, table: []const u8, request: wire.Request, _: operation.RequestContext) !wire.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(@as(u64, 17), fence.route.group_id);
            try std.testing.expectEqualStrings("docs", table);
            return switch (self.mode) {
                .applied => .{ .version = 1, .receipt = request.receipt orelse .{ .term = 2, .index = 3 }, .outcome = .{ .applied = .{} } },
                .unknown => error.RaftBatchWriteOutcomeUnknown,
                .malformed => .{ .version = 1, .outcome = .{ .applied = .{} } },
            };
        }
    };
    var fake: Fake = .{};
    const port: Port = .{ .ptr = &fake, .execute_fn = Fake.run };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    const encoded_fence = try std.json.Stringify.valueAlloc(a, fence, .{});
    defer a.free(encoded_fence);
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 17, .big);
    std.mem.writeInt(u64, namespace[16..24], 19, .big);
    var input: wire.Request = .{ .version = 1, .operation = .submit, .command = .{ .collect = .{ .namespace = namespace, .authority = @splat(2) } } };
    const body = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(body);
    try std.testing.expectError(error.InvalidCatalogRouteFence, execute(port, a, 17, "docs", body, .{}));
    const context: operation.RequestContext = .{ .catalog_route_fence_json = encoded_fence };
    try std.testing.expectError(error.InvalidCatalogRouteFence, execute(port, a, 18, "docs", body, context));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, execute(null, a, 17, "docs", body, context));
    const accepted = try execute(port, a, 17, "docs", body, context);
    try std.testing.expect(accepted.confirmed());
    fake.mode = .unknown;
    const unknown = try execute(port, a, 17, "docs", body, context);
    try std.testing.expect(!unknown.confirmed());
    try std.testing.expectEqual(null, unknown.receipt);
    input.operation = .recover;
    input.receipt = accepted.receipt;
    const recovery = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(recovery);
    const recovered_unknown = try execute(port, a, 17, "docs", recovery, context);
    try std.testing.expectEqualDeep(accepted.receipt, recovered_unknown.receipt);
    try std.testing.expect(!recovered_unknown.confirmed());
    fake.mode = .malformed;
    try std.testing.expectError(error.InvalidHistoryControlResponse, execute(port, a, 17, "docs", body, context));
    const previous_calls = fake.calls;
    input.command.collect.namespace[0] ^= 1;
    const wrong_namespace = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(wrong_namespace);
    try std.testing.expectError(error.InvalidHistoryControlWire, execute(port, a, 17, "docs", wrong_namespace, context));
    try std.testing.expectEqual(previous_calls, fake.calls);
}
