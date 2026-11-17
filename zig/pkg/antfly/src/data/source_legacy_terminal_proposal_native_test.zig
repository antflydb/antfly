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

//! Actual managed recovery proposal, confirmation loss and read-only recovery.
//! Positive terminal replay below uses an original fully retained legacy ABORT.
const std = @import("std");
const metadata = @import("../metadata/api.zig");
const local = @import("antfly_local_sources");
const legacy = local.storage_source_legacy_terminal_ordered;
const http = @import("source_legacy_terminal_http_wire.zig");
const Owner = local.storage_transaction_completion.RoutedOwner;
pub fn run(a: std.mem.Allocator, server: anytype, fence: metadata.CatalogRouteFence, owner: Owner, comptime Event: type, comptime Harness: type) !void {
    var original = try server.historyDecisionPort().read(a, fence, "docs", .{ .version = 1, .owner = owner, .transaction_id = @splat(249), .minimum_applied_index = 1 }, .{});
    defer original.deinit();
    try std.testing.expect(original.value.decision != null);
    const record = original.value.decision.?.record;
    try std.testing.expect(record.status == .aborted and record.intents_resolved);
    const command: legacy.policy.LegacyTerminalCommand = .{ .version = 1, .owner = owner, .proof = .{ .root = try local.storage_source_scope.Basis.fromOwner(owner), .transaction_id = @splat(249), .record_digest = record.digest } };
    const previous_last = Harness.last(server, owner.group_id);
    const before_documents = Harness.documentApplies(server);
    const inventory = local.storage_source_legacy_inventory;
    const inventory_query: inventory.Query = .{ .version = 1, .owner = owner, .after = null, .max_records = 256, .minimum_applied_index = 1 };
    const inventory_request: @import("source_legacy_inventory_http_wire.zig").Request = .{ .version = 1, .request_id = @splat(206), .query = inventory_query };
    var work_reply = try Harness.inventoryRpc(a, server, fence, inventory_request);
    defer work_reply.deinit(a);
    const work = work_reply.observation;
    try work.validate(inventory_query);
    var pending_digest: ?[32]u8 = null;
    for (work.page.items) |item| {
        try std.testing.expect(!std.meta.eql(item.transaction_id, command.proof.transaction_id));
        if (std.meta.eql(item.transaction_id, @as([16]u8, @splat(247)))) {
            try std.testing.expect(item.status == .pending and item.prepared_known and item.prepared and item.coordinator_known and item.coordinator);
            pending_digest = item.record_digest;
        }
    }
    const exact_pending_digest = pending_digest orelse return error.TestExpectedPendingLegacyInventory;
    var pending_record = try server.historyDecisionPort().read(a, fence, "docs", .{ .version = 1, .owner = owner, .transaction_id = @splat(247), .minimum_applied_index = work.cut.index }, .{});
    defer pending_record.deinit();
    try std.testing.expect(pending_record.value.decision != null);
    try std.testing.expectEqualDeep(exact_pending_digest, pending_record.value.decision.?.record.digest);
    var continuation = inventory_request;
    continuation.request_id = @splat(207);
    continuation.query.after = @splat(247);
    continuation.query.max_records = 1;
    continuation.query.minimum_applied_index = work.cut.index;
    var next_page = try Harness.inventoryRpc(a, server, fence, continuation);
    defer next_page.deinit(a);
    try std.testing.expect(next_page.observation.page.scanned_records <= 1);
    for (next_page.observation.page.items) |item| try std.testing.expect(std.mem.order(u8, &item.transaction_id, &@as([16]u8, @splat(247))) == .gt);
    try std.testing.expectEqual(previous_last, Harness.last(server, owner.group_id));
    var stale_inventory = inventory_query;
    stale_inventory.owner.root_generation += 1;
    try std.testing.expectError(error.TopologyChanged, server.legacyRecoveryInventoryLocal(a, fence, "docs", stale_inventory));
    var wrong = command;
    wrong.owner.root_generation += 1;
    wrong.proof.root.root_generation += 1;
    try std.testing.expectError(error.TopologyChanged, server.submitLegacyTerminalLocal(a, fence, "docs", wrong));
    try std.testing.expectEqual(previous_last, Harness.last(server, owner.group_id));
    var cancelled: std.atomic.Value(bool) = .init(true);
    var cancelled_fence = fence;
    cancelled_fence.admission_cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled);
    try std.testing.expectError(error.Canceled, server.legacyRecoveryInventoryLocal(a, cancelled_fence, "docs", inventory_query));
    try std.testing.expectError(error.Canceled, server.submitLegacyTerminalLocal(a, cancelled_fence, "docs", command));
    try std.testing.expectEqual(previous_last, Harness.last(server, owner.group_id));
    const Interrupted = struct {
        accepted_index: ?u64 = null,
        fn reach(ptr: *anyopaque, event: Event) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (event.phase == .proposal_accepted) {
                self.accepted_index = event.log_index;
                return error.InjectedLegacyTerminalConfirmationLoss;
            }
        }
    };
    var interruption: Interrupted = .{};
    const previous_hook = server.data_request_lifecycle_hook;
    defer server.data_request_lifecycle_hook = previous_hook;
    server.data_request_lifecycle_hook = .{ .ptr = &interruption, .reach_fn = Interrupted.reach };
    const request: http.Request = .{ .version = 1, .operation = .submit, .request_id = @splat(201), .owner = owner, .command = command, .receipt = null, .minimum_applied_index = 0 };
    const uncertain = try Harness.legacyRpc(a, server, fence, request);
    try std.testing.expect(uncertain.receipt != null);
    const accepted = uncertain.receipt.?;
    server.data_request_lifecycle_hook = previous_hook;
    try std.testing.expect(uncertain.outcome == null);
    try std.testing.expectEqual(interruption.accepted_index.?, accepted.index);
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (Harness.applied(server, owner.group_id) < accepted.index) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    const query: legacy.Query = .{ .version = 1, .owner = owner, .command = command, .entry = accepted, .minimum_applied_index = accepted.index };
    const before_recovery = Harness.last(server, owner.group_id);
    var recovery_request = request;
    recovery_request.operation = .recover;
    recovery_request.request_id = @splat(202);
    recovery_request.receipt = accepted;
    recovery_request.minimum_applied_index = accepted.index;
    const recovery_reply = try Harness.legacyRpc(a, server, fence, recovery_request);
    try std.testing.expect(recovery_reply.outcome != null);
    const recovered = recovery_reply.outcome.?;
    try std.testing.expectEqual(before_recovery, Harness.last(server, owner.group_id));
    try recovered.validate(a, query);
    try std.testing.expect(recovered.outcome == .applied and recovered.outcome.applied.kind == .aborted);
    try std.testing.expectEqual(record.finalized_at_ns, recovered.outcome.applied.timestamp);
    var repeated_request = request;
    repeated_request.request_id = @splat(203);
    const again = try Harness.legacyRpc(a, server, fence, repeated_request);
    try std.testing.expect(again.receipt != null);
    try std.testing.expect(again.outcome != null);
    const confirmed = again.outcome.?;
    try std.testing.expect(confirmed.cut.index >= again.receipt.?.index);
    try std.testing.expectEqualDeep(recovered.outcome, confirmed.outcome);
    var absent = command;
    absent.proof.transaction_id = @splat(198);
    absent.proof.record_digest = @splat(199);
    var absent_request = request;
    absent_request.request_id = @splat(204);
    absent_request.command = absent;
    const rejected = try Harness.legacyRpc(a, server, fence, absent_request);
    try std.testing.expect(rejected.receipt != null);
    try std.testing.expect(rejected.outcome != null);
    try std.testing.expect(rejected.outcome.?.outcome == .rejected and rejected.outcome.?.outcome.rejected == .not_found);
    const last = Harness.last(server, owner.group_id);
    recovery_request.request_id = @splat(205);
    const expired = try Harness.legacyRpc(a, server, fence, recovery_request);
    try std.testing.expect(expired.outcome != null and expired.outcome.?.outcome == .superseded);
    try std.testing.expect(!expired.confirmed());
    try std.testing.expectEqualDeep(accepted, expired.receipt.?);
    try std.testing.expectEqual(last, Harness.last(server, owner.group_id));
    try std.testing.expectEqual(before_documents, Harness.documentApplies(server));
    const registry = try server.sourceScopeObservationPort().read(a, fence, "docs", .{ .version = 1, .owner = owner, .domain = @splat(71), .minimum_applied_index = rejected.receipt.?.index }, .{});
    try std.testing.expect(!registry.header.strict and registry.record == null);
}
