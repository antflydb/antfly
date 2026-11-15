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

//! Typed, private settlement of an exact accepted root control. This never
//! submits a command, accepts an observation as a receipt, or certifies a
//! superseded/unknown result. The dispatcher supplies the authenticated result;
//! metadata atomically applies the resulting effect and its audit binding.
const std = @import("std");
const control = @import("changefeed_control.zig");
const intent = control.root_intent;
const wire = @import("../data/history_control_wire.zig");
const root = @import("antfly_local_sources").api_local_table_writes.table_write_source.history_consumer;
const metadata = @import("api.zig");
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
fn reference(state: control.State, item: control.Root, value: root.Reference) !void {
    try value.validate();
    if (!same(value.namespace, item.namespace) or !same(value.authority, state.authority) or !same(value.consumer, state.consumerId()) or !same(value.definition, state.definition.?.identity()) or value.grant != item.grant_sequence or value.epoch != item.consumer_epoch) return error.ChangefeedConflict;
}
pub const ValidatedRequest = struct { input: wire.Request, policy_revision: u64 };
/// Validate the complete immutable scope before preparing or dispatching a
/// command. Success is not dispatch permission; the journal begin CAS is still
/// required. The same validation runs again during outcome settlement.
pub fn validateRequest(a: std.mem.Allocator, state: control.State, namespace: control.Namespace, payload: intent.Payload) !ValidatedRequest {
    try state.validate();
    try payload.validate();
    const item = state.roots[try state.rootIndex(namespace)];
    if (item.released or state.phase == .empty or state.phase == .deleted) return error.ChangefeedConflict;
    const ceiling = if (item.pending_control) |pending| pending.sequence else std.math.add(u64, state.authority_sequence, 1) catch return error.ChangefeedConflict;
    const input = wire.decodeRequest(a, payload.command, namespace) catch |err| return if (err == error.OutOfMemory) err else error.ChangefeedCorrupt;
    if (input.operation != .submit) return error.ChangefeedConflict;
    var route = std.json.parseFromSlice(metadata.CatalogRouteFence, a, payload.fence, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.ChangefeedCorrupt;
    defer route.deinit();
    try route.value.validate();
    const identity = route.value.route.identity_namespace;
    var expected_namespace: control.Namespace = undefined;
    std.mem.writeInt(u64, expected_namespace[0..8], identity.table_id, .big);
    std.mem.writeInt(u64, expected_namespace[8..16], identity.shard_id, .big);
    std.mem.writeInt(u64, expected_namespace[16..24], identity.range_id, .big);
    if (!same(namespace, expected_namespace)) return error.ChangefeedConflict;
    const table = for (state.definition.?.tables) |table| {
        if (table.table_id == identity.table_id) break table;
    } else return error.ChangefeedConflict;
    if (table.contract != .native_document and table.contract != .native_relational) return error.ChangefeedCapabilityUnsupported;
    switch (input.command) {
        .enroll => |v| {
            if (state.definition.?.start == .snapshot_then_changes and item.consumer_epoch == 0) return error.ChangefeedCapabilityUnsupported;
            try v.validate();
            if (!same(v.authority, state.authority) or !same(v.consumer, state.consumerId()) or !same(v.definition, state.definition.?.identity()) or v.sequence != item.grant_sequence or v.policy_revision != table.policy_revision or v.expires_at_seconds > state.expires_at) return error.ChangefeedConflict;
        },
        .inspect => |v| try reference(state, item, v),
        .acknowledge => |v| {
            try reference(state, item, v.reference);
            if (v.previous < item.reclaimed_through or v.next < v.previous or v.next > item.acknowledged.sequence) return error.ChangefeedConflict;
        },
        .renew => |v| {
            try reference(state, item, v.reference);
            if (v.previous == 0 or v.next <= v.previous or v.next > state.expires_at) return error.ChangefeedConflict;
        },
        .reclaim => |v| try reference(state, item, v.reference),
        .retire => |v| {
            if (!same(v.authority, state.authority) or !same(v.consumer, state.consumerId()) or !same(v.definition, state.definition.?.identity()) or v.grant != item.grant_sequence or v.sequence <= v.grant or v.sequence >= ceiling or (state.phase != .closing and state.phase != .reset_required)) return error.ChangefeedConflict;
        },
        .collect => |v| if (!same(v.authority, state.authority)) return error.ChangefeedConflict,
    }
    return .{ .input = input, .policy_revision = table.policy_revision };
}
pub fn settle(a: std.mem.Allocator, state: control.State, namespace: control.Namespace, response: wire.Response) !intent.Settlement {
    try state.validate();
    const item = state.roots[try state.rootIndex(namespace)];
    const pending = item.pending_control orelse return error.ChangefeedConflict;
    if (pending.stage != .accepted or !response.confirmed()) return error.ChangefeedConflict;
    const entry = response.receipt.?;
    if (entry.term != pending.receipt.?.term or entry.index != pending.receipt.?.index) return error.ChangefeedConflict;
    const validated = try validateRequest(a, state, namespace, pending.payload);
    const input = validated.input;
    try response.validate(input);
    if (response.outcome.? == .applied) {
        if (response.outcome.?.applied.consumer) |consumer| {
            if (consumer.policy_revision != validated.policy_revision or consumer.expires_at_seconds > state.expires_at) return error.ChangefeedConflict;
            if (input.command == .enroll and item.consumer_epoch == 0) {
                if (consumer.acknowledged != consumer.start) return error.ChangefeedConflict;
            } else if (consumer.epoch != item.consumer_epoch or consumer.start != item.start or consumer.acknowledged < item.reclaimed_through or consumer.acknowledged > item.acknowledged.sequence) return error.ChangefeedConflict;
        }
    }
    const effect: intent.Effect = switch (response.outcome.?) {
        .rejected => |code| .{ .rejected = @backingInt(code) },
        .superseded => return error.ChangefeedConflict,
        .applied => |result| switch (input.command) {
            .enroll => .{ .enrolled = .{ .consumer_epoch = result.consumer.?.epoch, .start = result.consumer.?.start } },
            .acknowledge => |v| .{ .acknowledged = .{ .consumer_epoch = item.consumer_epoch, .previous = item.reclaimed_through, .next = v.next } },
            .renew => .{ .renewed = .{ .consumer_epoch = item.consumer_epoch, .expires_at = result.consumer.?.expires_at_seconds } },
            .retire => |v| .{ .retired = .{ .sequence = v.sequence } },
            .inspect, .collect, .reclaim => .maintenance,
        },
    };
    const result: intent.Settlement = .{
        .sequence = pending.sequence,
        .actor = pending.actor,
        .receipt = pending.receipt.?,
        .request_digest = pending.digest,
        .proof = control.hash(.{ .domain = "antfly:root-control-settlement:v1", .feed = state.id, .epoch = state.epoch, .namespace = namespace, .intent = pending.sequence, .request = pending.digest, .receipt = entry, .outcome = response.outcome.? }),
        .effect = effect,
    };
    try pending.verifySettlement(result);
    return result;
}

pub const test_support = if (@import("builtin").is_test) struct {
    fn apply(a: std.mem.Allocator, state: control.State, mutation: control.Mutation) !control.State {
        return mutation.apply(a, state, .{ .incarnation = @splat(9), .sequence = state.authority_sequence + 1 });
    }
    fn make(a: std.mem.Allocator, bad_authority: bool) !control.State {
        const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
        var namespace: [24]u8 = undefined;
        std.mem.writeInt(u64, namespace[0..8], 7, .big);
        std.mem.writeInt(u64, namespace[8..16], 17, .big);
        std.mem.writeInt(u64, namespace[16..24], 19, .big);
        var state = try apply(a, .{}, .{ .create = .{ .id = @splat(4), .definition = .{ .owner = @splat(1), .authorization = @splat(2), .tables = &.{.{ .table_id = 7, .binding = @splat(3), .policy_revision = 1, .contract = .native_document }} }, .now = 100, .expires_at = 1000 } });
        state = try apply(a, state, .{ .plan_roots = &.{.{ .namespace = namespace, .ownership_epoch = 1 }} });
        const input: wire.Request = .{ .version = 1, .operation = .submit, .command = .{ .enroll = .{ .namespace = namespace, .authority = if (bad_authority) @splat(8) else state.authority, .sequence = state.roots[0].grant_sequence, .consumer = state.consumerId(), .definition = state.definition.?.identity(), .expires_at_seconds = 1000, .policy_revision = 1 } } };
        const payload: intent.Payload = .{ .command = try std.json.Stringify.valueAlloc(a, input, .{}), .fence = try std.json.Stringify.valueAlloc(a, fence, .{}), .table = "docs" };
        state = try apply(a, state, .{ .root_control = .{ .namespace = namespace, .operation = .{ .prepare = .{ .ownership_epoch = 1, .grant_sequence = state.roots[0].grant_sequence, .payload = payload } } } });
        const sequence = state.roots[0].pending_control.?.sequence;
        state = try apply(a, state, .{ .root_control = .{ .namespace = namespace, .operation = .{ .begin = .{ .sequence = sequence, .actor = @splat(3) } } } });
        return apply(a, state, .{ .root_control = .{ .namespace = namespace, .operation = .{ .accepted = .{ .sequence = sequence, .actor = @splat(3), .receipt = .{ .term = 4, .index = 20 } } } } });
    }
    pub fn contracts() !void {
        const a = std.testing.allocator;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const state = try make(arena.allocator(), false);
        const ns = state.roots[0].namespace;
        const response: wire.Response = .{ .version = 1, .receipt = .{ .term = 4, .index = 20 }, .outcome = .{ .applied = .{ .consumer = .{ .id = state.consumerId(), .epoch = 7, .start = 3, .acknowledged = 3, .expires_at_seconds = 1000, .policy_revision = 1 } } } };
        _ = try validateRequest(a, state, ns, state.roots[0].pending_control.?.payload);
        const settlement = try settle(a, state, ns, response);
        try std.testing.expectEqual(@as(u64, 7), settlement.effect.enrolled.consumer_epoch);
        const resolved = try apply(arena.allocator(), state, .{ .root_control = .{ .namespace = ns, .operation = .{ .settled = settlement } } });
        try std.testing.expect(resolved.roots[0].pending_control == null);
        try std.testing.expectEqual(@as(u64, 3), resolved.roots[0].start);
        try std.testing.expectEqualDeep(settlement, resolved.roots[0].last_control.?);
        var wrong = response;
        wrong.receipt.?.index += 1;
        try std.testing.expectError(error.ChangefeedConflict, settle(a, state, ns, wrong));
        wrong = response;
        wrong.outcome = null;
        try std.testing.expectError(error.ChangefeedConflict, settle(a, state, ns, wrong));
        wrong.outcome = .superseded;
        try std.testing.expectError(error.ChangefeedConflict, settle(a, state, ns, wrong));
        wrong = response;
        wrong.outcome.?.applied.consumer.?.policy_revision = 2;
        try std.testing.expectError(error.ChangefeedConflict, settle(a, state, ns, wrong));
        wrong = response;
        wrong.outcome.?.applied.consumer.?.acknowledged = 4;
        try std.testing.expectError(error.ChangefeedConflict, settle(a, state, ns, wrong));
        wrong = response;
        wrong.outcome.?.applied.consumer.?.id = @splat(6);
        try std.testing.expectError(error.InvalidHistoryControlWire, settle(a, state, ns, wrong));
        wrong = response;
        wrong.outcome = .{ .rejected = .checkpoint_conflict };
        const rejected = try settle(a, state, ns, wrong);
        try std.testing.expectEqual(@as(u8, 5), rejected.effect.rejected);
        try std.testing.expect(!same(settlement.proof, rejected.proof));
        const foreign = try make(arena.allocator(), true);
        try std.testing.expectError(error.ChangefeedConflict, validateRequest(a, foreign, ns, foreign.roots[0].pending_control.?.payload));
        try std.testing.expectError(error.ChangefeedConflict, settle(a, foreign, ns, response));
        try std.testing.checkAllAllocationFailures(a, checkAllocations, .{ state, ns, response });
    }
    fn checkAllocations(a: std.mem.Allocator, state: control.State, namespace: control.Namespace, response: wire.Response) !void {
        _ = try settle(a, state, namespace, response);
    }
} else struct {};
