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

//! A retirement proposal starts with intent, never a caller's decision proof.
//! Only an independent qualified decision-port read supplies coordinator evidence.
const std = @import("std");
const local = @import("antfly_local_sources");
const retirement = local.storage_source_scope_retirement;
const decisions = @import("history_decision_operations.zig");
const catalog = @import("table_catalog.zig");
const Context = local.api_operation.RequestContext;
pub const OwnedCommand = struct {
    command: retirement.Command,
    observation: decisions.Reply,
    pub fn deinit(self: *OwnedCommand) void {
        self.observation.deinit();
        self.* = undefined;
    }
};
pub const Authority = struct {
    catalog_source: catalog.CatalogSource,
    decision_port: decisions.Port,
    pub fn capture(self: Authority, a: std.mem.Allocator, intent: retirement.Intent, coordinator_locator: []const u8, original: Context) !OwnedCommand {
        try intent.validate();
        if (coordinator_locator.len == 0 or coordinator_locator.len > 4096) return error.InvalidParticipant;
        var context = try original.platformDeadline();
        context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s);
        try context.ensureActive();
        var budget = self.catalog_source.budget(null);
        budget.deadline_ns = @min(budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io }) orelse std.math.maxInt(u64), budget.nowNs() +| 5 * std.time.ns_per_s);
        budget.cancellation = context.cancellation;
        var session = try catalog.RoutingSession.initWithBudget(a, self.catalog_source, budget);
        defer session.deinit();
        const fence = try @import("completion_recovery.zig").routeRetainedOwner(&session, intent.coordinator.owner, coordinator_locator, budget, context);
        const locator = local.api_local_transaction_contract.parseParticipantRef(coordinator_locator) orelse return error.InvalidParticipant;
        const query = intent.coordinator;
        var observation = try self.decision_port.read(a, fence, locator.table_name, .{ .version = query.version, .owner = query.owner, .transaction_id = query.transaction_id, .minimum_applied_index = query.minimum_applied_index }, context);
        errdefer observation.deinit();
        try context.ensureActive();
        const command: retirement.Command = .{ .version = 1, .intent = intent, .observation = observation.value };
        _ = try command.proof();
        // The supplied locator chooses a route, not authority. The original
        // retained coordinator membership must name exactly that locator.
        const admission = observation.value.decision.?.admission.admission;
        const member = try admission.coordinatorMember();
        if (!std.mem.eql(u8, member.locator, coordinator_locator)) return error.SourceTransactionIdentityConflict;
        try budget.checkpoint();
        try context.ensureActive();
        return .{ .command = command, .observation = observation };
    }
};
