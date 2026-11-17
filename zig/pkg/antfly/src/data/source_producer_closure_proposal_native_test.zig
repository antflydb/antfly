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

//! Producer closure through the real proposer, durable activation and ReadIndex.
//! Accepted proposal identity is not a row decision or legacy-drain proof.
const std = @import("std");
const metadata = @import("../metadata/api.zig");
const local = @import("antfly_local_sources");
const Owner = local.storage_transaction_completion.RoutedOwner;
pub fn run(a: std.mem.Allocator, server: anytype, fence: metadata.CatalogRouteFence, owner: Owner, comptime Event: type, comptime Harness: type) !void {
    const before = try server.sourceProducerClosureObservationPort().read(a, fence, "docs", .{ .version = 1, .owner = owner, .minimum_applied_index = 1 }, .{});
    try std.testing.expect(before.closure == null);
    var changed = owner;
    changed.root_generation += 1;
    const original_last = Harness.last(server, owner.group_id);
    try std.testing.expectError(error.TopologyChanged, server.submitSourceProducerClosureLocal(a, fence, "docs", changed));
    try std.testing.expectEqual(original_last, Harness.last(server, owner.group_id));
    var cancelled: std.atomic.Value(bool) = .init(true);
    var cancelled_fence = fence;
    cancelled_fence.admission_cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled);
    try std.testing.expectError(error.Canceled, server.submitSourceProducerClosureLocal(a, cancelled_fence, "docs", owner));
    try std.testing.expectEqual(original_last, Harness.last(server, owner.group_id));
    const Interrupted = struct {
        accepted_index: ?u64 = null,
        fn reach(ptr: *anyopaque, event: Event) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (event.phase == .proposal_accepted) {
                self.accepted_index = event.log_index;
                return error.InjectedProducerClosureConfirmationLoss;
            }
        }
    };
    var interruption: Interrupted = .{};
    const previous_hook = server.data_request_lifecycle_hook;
    defer server.data_request_lifecycle_hook = previous_hook;
    const before_documents = Harness.documentApplies(server);
    server.data_request_lifecycle_hook = .{ .ptr = &interruption, .reach_fn = Interrupted.reach };
    const uncertain = try Harness.submitRpc(a, server, fence, owner, 181);
    server.data_request_lifecycle_hook = previous_hook;
    try std.testing.expect(uncertain.outcome == null);
    try std.testing.expectEqual(interruption.accepted_index.?, uncertain.entry.index);
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (Harness.applied(server, owner.group_id) < uncertain.entry.index) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    const recovery_last = Harness.last(server, owner.group_id);
    const recovered = try server.recoverSourceProducerClosureLocal(a, fence, "docs", owner, uncertain.entry);
    try std.testing.expectEqual(recovery_last, Harness.last(server, owner.group_id));
    try std.testing.expect(recovered.closure != null);
    try std.testing.expectEqual(uncertain.entry.index, recovered.closure.?.first_closed_at.index);
    try std.testing.expectEqual(uncertain.entry.term, recovered.closure.?.first_closed_at.term);
    const again = try server.submitSourceProducerClosureLocal(a, fence, "docs", owner);
    const observed = again.outcome orelse return error.TestExpectedQualifiedClosure;
    try std.testing.expect(observed.cut.index >= again.entry.index);
    try std.testing.expectEqualDeep(recovered.closure, observed.closure);
    const routed_again = try Harness.submitRpc(a, server, fence, owner, 182);
    const routed_observation = routed_again.outcome orelse return error.TestExpectedRoutedClosureState;
    try std.testing.expect(routed_observation.cut.index >= routed_again.entry.index);
    try std.testing.expectEqualDeep(recovered.closure, routed_observation.closure);

    try std.testing.expectEqual(before_documents, Harness.documentApplies(server));
    const registry = try server.sourceScopeObservationPort().read(a, fence, "docs", .{ .version = 1, .owner = owner, .domain = @splat(71), .minimum_applied_index = routed_again.entry.index }, .{});
    try std.testing.expect(!registry.header.strict);
    try std.testing.expect(registry.record == null);
}
