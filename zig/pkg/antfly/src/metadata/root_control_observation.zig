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

//! Current-state reconciliation of an ambiguous/superseded control. A positive
//! predicate is stable under a delayed original command: enroll preserves an
//! existing epoch/start, ACK/renew use monotonic CAS, and a retirement floor
//! prevents late enrollment. Neither absence nor an unbound authority proves
//! that an in-flight command cannot commit. No original outcome is invented.
const std = @import("std");
const control = @import("changefeed_control.zig");
const intent = control.root_intent;
const outcome = @import("root_control_outcome.zig");
const operations = @import("../api/history_grant_operations.zig");
const native = @import("antfly_local_sources").api_local_table_writes.table_write_source.history_consumer;
pub fn request(a: std.mem.Allocator, state: control.State, namespace: control.Namespace) !operations.Request {
    const item = state.roots[try state.rootIndex(namespace)];
    const pending = item.pending_control orelse return error.ChangefeedConflict;
    if (pending.stage == .prepared) return error.ChangefeedConflict;
    const validated = try outcome.validateRequest(a, state, namespace, pending.payload);
    return .{ .version = 1, .identity = .{ .namespace = namespace, .authority = state.authority, .consumer = state.consumerId(), .definition = state.definition.?.identity(), .sequence = item.grant_sequence, .policy_revision = validated.policy_revision } };
}
pub fn resolve(a: std.mem.Allocator, state: control.State, namespace: control.Namespace, observed: native.GrantObservation) !?intent.Reconciliation {
    const input = try request(a, state, namespace);
    _ = try operations.wire.Reply.init(input.binding(), observed);
    const item = state.roots[try state.rootIndex(namespace)];
    const pending = item.pending_control.?;
    const validated = try outcome.validateRequest(a, state, namespace, pending.payload);
    const command = validated.input.command;
    if (observed == .other_grant) {
        if (observed.other_grant.grant > item.grant_sequence) return error.ChangefeedResynchronizationRequired;
        return null;
    }
    const effect: @FieldType(intent.Reconciliation, "effect") = effect: {
        if (observed == .retired and (state.phase == .closing or state.phase == .reset_required)) break :effect .released;
        // These maintenance commands cannot grant ACKs or move metadata cursors.
        // Their original work count remains unknown; new GC work is independent.
        if (command == .collect) break :effect .{ .progress = .maintenance };
        if (observed != .active) return null;
        const current = observed.active.consumer;
        if (current.policy_revision != validated.policy_revision or current.expires_at_seconds > state.expires_at) return error.ChangefeedConflict;
        if (item.consumer_epoch != 0 and (current.epoch != item.consumer_epoch or current.start != item.start or current.acknowledged < item.reclaimed_through or current.acknowledged > item.acknowledged.sequence)) return error.ChangefeedConflict;
        switch (command) {
            .configure_policy, .inspect_policy => return error.ChangefeedCapabilityUnsupported,
            .enroll => |grant| {
                if (observed.active.limit != grant.limit) return error.ChangefeedConflict;
                if (item.consumer_epoch == 0 and current.acknowledged != current.start) return error.ChangefeedConflict;
                break :effect .{ .progress = .{ .enrolled = .{ .consumer_epoch = current.epoch, .start = current.start } } };
            },
            .acknowledge => |value| {
                if (current.acknowledged < value.next) return null;
                break :effect .{ .progress = .{ .acknowledged = .{ .consumer_epoch = current.epoch, .previous = item.reclaimed_through, .next = value.next } } };
            },
            .renew => |value| {
                if (current.expires_at_seconds < value.next) return null;
                break :effect .{ .progress = .{ .renewed = .{ .consumer_epoch = current.epoch, .expires_at = current.expires_at_seconds } } };
            },
            .inspect, .reclaim => break :effect .{ .progress = .maintenance },
            .retire => return null,
            .collect => unreachable,
        }
    };
    const result: intent.Reconciliation = .{ .sequence = pending.sequence, .actor = pending.actor, .request_digest = pending.digest, .original_receipt = pending.receipt, .displaced = pending.displaced, .effect = effect, .proof = control.hash(.{ .domain = "antfly:root-control-observation:v1", .feed = state.id, .epoch = state.epoch, .intent = pending.sequence, .request = pending.digest, .read = input.binding(), .observed = observed }) };
    try pending.verifyReconciliation(result);
    return result;
}

pub const test_support = if (@import("builtin").is_test) struct {
    const fixture = outcome.test_support;
    pub fn contracts() !void {
        const a = std.testing.allocator;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var state = try fixture.make(scratch, false);
        const roots = try scratch.dupe(control.Root, state.roots);
        roots[0].pending_control.?.stage = .dispatching;
        roots[0].pending_control.?.receipt = null;
        state.roots = roots;
        const ns = state.roots[0].namespace;
        const input = try request(a, state, ns);
        const original = try outcome.validateRequest(a, state, ns, state.roots[0].pending_control.?.payload);
        var active: native.GrantObservation = .{ .active = .{ .grant = input.identity.sequence, .limit = original.input.command.enroll.limit, .consumer = .{ .id = input.identity.consumer, .epoch = 7, .start = 3, .acknowledged = 3, .expires_at_seconds = 1000, .policy_revision = 1 } } };
        try std.testing.expect((try resolve(a, state, ns, .unbound)) == null);
        try std.testing.expect((try resolve(a, state, ns, .{ .absent = 0 })) == null);
        try std.testing.expect((try resolve(a, state, ns, .{ .retired = input.identity.sequence })) == null);
        var other = active.active;
        other.grant -= 1;
        try std.testing.expect((try resolve(a, state, ns, .{ .other_grant = other })) == null);
        other.grant = input.identity.sequence + 1;
        try std.testing.expectError(error.ChangefeedResynchronizationRequired, resolve(a, state, ns, .{ .other_grant = other }));
        const proof = (try resolve(a, state, ns, active)).?;
        try std.testing.expect(proof.original_receipt == null);
        const enrolled = try fixture.apply(scratch, state, .{ .root_control = .{ .namespace = ns, .operation = .{ .reconciled = proof } } });
        try std.testing.expect(enrolled.roots[0].last_control == null);
        try std.testing.expect(enrolled.roots[0].pending_control == null);
        try std.testing.expectEqual(@as(u64, 7), enrolled.roots[0].consumer_epoch);
        try std.testing.expectEqualDeep(enrolled, try fixture.apply(scratch, enrolled, .{ .root_control = .{ .namespace = ns, .operation = .{ .reconciled = proof } } }));
        const bytes = try control.encode(a, enrolled);
        defer a.free(bytes);
        var parsed = try control.parse(a, bytes);
        defer parsed.deinit();
        try std.testing.expectEqualDeep(proof, parsed.value.roots[0].last_reconciliation.?);
        var wrong = active;
        wrong.active.limit += 1;
        try std.testing.expectError(error.ChangefeedConflict, resolve(a, state, ns, wrong));
        wrong = active;
        wrong.active.consumer.acknowledged = 4;
        try std.testing.expectError(error.ChangefeedConflict, resolve(a, state, ns, wrong));
        const closed = try fixture.apply(scratch, state, .close);
        const release = (try resolve(a, closed, ns, .{ .retired = input.identity.sequence })).?;
        try std.testing.expectEqual(.released, release.effect);
        var released = try fixture.apply(scratch, closed, .{ .root_control = .{ .namespace = ns, .operation = .{ .reconciled = release } } });
        try std.testing.expect(released.roots[0].released);
        released = try fixture.apply(scratch, released, .finish_close);
        try std.testing.expectEqualDeep(released, try fixture.apply(scratch, released, .{ .root_control = .{ .namespace = ns, .operation = .{ .reconciled = release } } }));
        // A native ACK below the authorized target proves no completion.
        var streaming = try fixture.apply(scratch, enrolled, .{ .seal = .{ .cut = @splat(1) } });
        var delivery: control.Delivery = .{ .sequence = 1, .before = streaming.latest_cursor, .cursor = @splat(0), .authority = @splat(8), .issued_at = 101, .advances = &.{.{ .namespace = ns, .position = .{ .sequence = 10 } }} };
        delivery.cursor = delivery.identity(streaming.id, streaming.epoch);
        streaming = try fixture.apply(scratch, streaming, .{ .issue = delivery });
        streaming = try fixture.apply(scratch, streaming, .{ .acknowledge = delivery.cursor });
        const ref: native.Reference = .{ .namespace = ns, .authority = streaming.authority, .consumer = streaming.consumerId(), .epoch = 7, .grant = streaming.roots[0].grant_sequence, .definition = streaming.definition.?.identity() };
        streaming = try pending(scratch, streaming, .{ .acknowledge = .{ .reference = ref, .previous = 3, .next = 10 } }, state.roots[0].pending_control.?.payload.fence);
        active.active.consumer.acknowledged = 9;
        try std.testing.expect((try resolve(a, streaming, ns, active)) == null);
        active.active.consumer.acknowledged = 10;
        const acknowledged = (try resolve(a, streaming, ns, active)).?;
        try std.testing.expectEqual(@as(u64, 10), acknowledged.effect.progress.acknowledged.next);
        streaming = try fixture.apply(scratch, streaming, .{ .root_control = .{ .namespace = ns, .operation = .{ .reconciled = acknowledged } } });
        try std.testing.expectEqual(@as(u64, 10), streaming.roots[0].reclaimed_through);
        streaming = try fixture.apply(scratch, streaming, .{ .renew = .{ .now = 150, .expires_at = 1200 } });
        streaming = try pending(scratch, streaming, .{ .renew = .{ .reference = ref, .previous = 1000, .next = 1200 } }, state.roots[0].pending_control.?.payload.fence);
        active.active.consumer.expires_at_seconds = 1100;
        try std.testing.expect((try resolve(a, streaming, ns, active)) == null);
        active.active.consumer.expires_at_seconds = 1200;
        const renewed = (try resolve(a, streaming, ns, active)).?;
        try std.testing.expectEqual(@as(u64, 1200), renewed.effect.progress.renewed.expires_at);
        try std.testing.checkAllAllocationFailures(a, checkAllocations, .{ state, ns });
    }
    fn pending(a: std.mem.Allocator, before: control.State, command: native.Command, frozen: []const u8) !control.State {
        const wire = @import("../data/history_control_wire.zig");
        const body = try std.json.Stringify.valueAlloc(a, wire.Request{ .version = 1, .operation = .submit, .command = command }, .{});
        const item = before.roots[0];
        var state = try fixture.apply(a, before, .{ .root_control = .{ .namespace = item.namespace, .operation = .{ .prepare = .{ .ownership_epoch = item.ownership_epoch, .grant_sequence = item.grant_sequence, .payload = .{ .command = body, .fence = frozen, .table = "docs" } } } } });
        state = try fixture.apply(a, state, .{ .root_control = .{ .namespace = item.namespace, .operation = .{ .begin = .{ .sequence = state.roots[0].pending_control.?.sequence, .actor = @splat(3) } } } });
        return state;
    }
    fn checkAllocations(a: std.mem.Allocator, state: control.State, ns: control.Namespace) !void {
        _ = try resolve(a, state, ns, .{ .absent = 0 });
    }
} else struct {};
