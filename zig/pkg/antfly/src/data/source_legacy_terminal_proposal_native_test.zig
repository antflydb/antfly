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
    var wrong = command;
    wrong.owner.root_generation += 1;
    wrong.proof.root.root_generation += 1;
    try std.testing.expectError(error.TopologyChanged, server.submitLegacyTerminalLocal(a, fence, "docs", wrong));
    try std.testing.expectEqual(previous_last, Harness.last(server, owner.group_id));
    var cancelled: std.atomic.Value(bool) = .init(true);
    var cancelled_fence = fence;
    cancelled_fence.admission_cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled);
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
    const uncertain = try server.submitLegacyTerminalLocal(a, fence, "docs", command);
    server.data_request_lifecycle_hook = previous_hook;
    try std.testing.expect(uncertain.outcome == null);
    try std.testing.expectEqual(interruption.accepted_index.?, uncertain.entry.index);
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (Harness.applied(server, owner.group_id) < uncertain.entry.index) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    const query: legacy.Query = .{ .version = 1, .owner = owner, .command = command, .entry = uncertain.entry, .minimum_applied_index = uncertain.entry.index };
    const before_recovery = Harness.last(server, owner.group_id);
    const recovered = try server.recoverLegacyTerminalLocal(a, fence, "docs", query);
    try std.testing.expectEqual(before_recovery, Harness.last(server, owner.group_id));
    try recovered.validate(a, query);
    try std.testing.expect(recovered.outcome == .applied and recovered.outcome.applied.kind == .aborted);
    try std.testing.expectEqual(record.finalized_at_ns, recovered.outcome.applied.timestamp);
    const again = try server.submitLegacyTerminalLocal(a, fence, "docs", command);
    try std.testing.expect(again.outcome != null);
    const confirmed = again.outcome.?;
    try std.testing.expect(confirmed.cut.index >= again.entry.index);
    try std.testing.expectEqualDeep(recovered.outcome, confirmed.outcome);
    var absent = command;
    absent.proof.transaction_id = @splat(198);
    absent.proof.record_digest = @splat(199);
    const rejected = try server.submitLegacyTerminalLocal(a, fence, "docs", absent);
    try std.testing.expect(rejected.outcome != null);
    try std.testing.expect(rejected.outcome.?.outcome == .rejected and rejected.outcome.?.outcome.rejected == .not_found);
    const last = Harness.last(server, owner.group_id);
    const expired = try server.recoverLegacyTerminalLocal(a, fence, "docs", query);
    try std.testing.expect(expired.outcome == .superseded);
    try std.testing.expectEqual(last, Harness.last(server, owner.group_id));
    try std.testing.expectEqual(before_documents, Harness.documentApplies(server));
    const registry = try server.sourceScopeObservationPort().read(a, fence, "docs", .{ .version = 1, .owner = owner, .domain = @splat(71), .minimum_applied_index = rejected.entry.index }, .{});
    try std.testing.expect(!registry.header.strict and registry.record == null);
}
