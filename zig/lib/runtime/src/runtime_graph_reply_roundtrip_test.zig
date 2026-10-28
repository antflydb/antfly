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

//! The producer and consumer resolve graph types independently. No compilation-
//! local anonymous type identity may enter the checked callback contract.
const std = @import("std");
const builtin = @import("builtin");
const rpc = @import("cypher").rpc;
const value = @import("cypher").value;
const wire = @import("cypher").wire;
const callbacks = @import("runtime_callback_abi.zig");
const Methods = struct { invoke_typed: *const fn (*anyopaque, std.mem.Allocator, std.mem.Allocator, rpc.Request) anyerror!rpc.Reply };
const Boundary = callbacks.Boundary(Methods);
const Descriptor = extern struct { dispatch: callbacks.CallbackDispatch, invoke: *const anyopaque };
fn invoke(_: *anyopaque, a: std.mem.Allocator, scratch: std.mem.Allocator, request: rpc.Request) !rpc.Reply {
    var owner_arena = std.heap.ArenaAllocator.init(scratch);
    defer owner_arena.deinit();
    const owned = owner_arena.allocator();
    // Total decoding exceeds the caller's fixed scratch capacity many times.
    // Reclamation must use this runtime's arena identity and explicit backing.
    for (0..64) |_| {
        var document = std.heap.ArenaAllocator.init(@import("cypher").backend.reclaimingAllocator(owned));
        defer document.deinit();
        const bytes = try document.allocator().alloc(u8, 128 * 1024);
        bytes[0] = 1;
    }
    if (request.graph_incarnation != 7) return error.CatalogGenerationChanged;
    if (request.operation == .scan_ordered) return .{ .read_failure = .work, .search_failure = .anchors, .work = 2 };
    if (request.order.len != 1 or request.order[0].after.?.value.integer != 9 or request.filters.len != 1 or request.filters[0].op != .eq) return error.InvalidArgument;
    const node = try owned.create(value.Node);
    node.* = .{ .identity = request.identity.? };
    const values = try owned.alloc(wire.Value, 1);
    values[0] = .{ .value = .{ .node = node } };
    const cursors = try owned.alloc(?[]const u8, 2);
    cursors[0] = null;
    cursors[1] = try owned.dupe(u8, "after");
    return (rpc.Reply{ .values = values, .work = 2, .cursors = cursors }).clone(a);
}
fn descriptor() callconv(.c) *const Descriptor {
    const Holder = struct {
        const value: Descriptor = .{ .dispatch = Boundary.local_dispatch, .invoke = @ptrCast(&invoke) };
    };
    return &Holder.value;
}
comptime {
    if (!builtin.is_test) @export(&descriptor, .{ .name = "antfly_test_graph_reply_provider_v1", .visibility = .hidden });
}
extern fn antfly_test_graph_reply_provider_v1() callconv(.c) *const Descriptor;

test "typed graph callback preserves payloads across independently compiled runtimes" {
    const producer = antfly_test_graph_reply_provider_v1();
    const method: @FieldType(Methods, "invoke_typed") = @ptrCast(@alignCast(producer.invoke));
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var marker: u8 = 0;
    const a = arena.allocator();
    const scratch_storage = try std.testing.allocator.alloc(u8, 1 << 20);
    defer std.testing.allocator.free(scratch_storage);
    var scratch = std.heap.FixedBufferAllocator.init(scratch_storage);
    const identity: value.Identity = .{ .table = "docs", .key = "opaque" };
    var request: rpc.Request = .{ .operation = .get, .graph_incarnation = 7, .graph_revision = 1, .identity = identity, .order = &.{.{ .property = "score", .after = .{ .value = .{ .integer = 9 } } }}, .filters = &.{.{ .property = "score", .value = .{ .integer = 9 } }} };
    const reply = try Boundary.call("invoke_typed", producer.dispatch, method, .{ &marker, a, scratch.allocator(), request });
    try std.testing.expectEqual(@as(usize, 0), scratch.end_index);
    @memset(scratch_storage, 0xa5);
    try std.testing.expect(reply.cursors.?[0] == null);
    try std.testing.expectEqualStrings("after", reply.cursors.?[1].?);
    try std.testing.expectEqual(@as(usize, 2), reply.work);
    try std.testing.expect(reply.values[0].value.node.identity.eql(identity));
    request.operation = .scan_ordered;
    const failed = try Boundary.call("invoke_typed", producer.dispatch, method, .{ &marker, a, scratch.allocator(), request });
    try std.testing.expectEqual(rpc.BudgetFailure.work, failed.read_failure.?);
    try std.testing.expectEqual(rpc.BudgetFailure.anchors, failed.search_failure.?);
    request.graph_incarnation = 8;
    try std.testing.expectError(error.CatalogGenerationChanged, Boundary.call("invoke_typed", producer.dispatch, method, .{ &marker, a, scratch.allocator(), request }));
}
