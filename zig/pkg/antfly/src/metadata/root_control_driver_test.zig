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

const std = @import("std");
const driver = @import("root_control_driver.zig");
const control = @import("changefeed_control.zig");
const operations = @import("../api/history_control_operations.zig");
const metadata = @import("api.zig");
const fixture = @import("../pg_spool_cleanup_test.zig");
const Store = @import("storage/raft_apply_store.zig").RaftApplyStore;
const Table = @import("domain.zig").TableRecord;
const A = std.mem.Allocator;
const Request = fixture.Request;
const Context = @import("antfly_local_sources").api_operation.RequestContext;
const group = @import("antfly_local_sources").common_group_ids.main_metadata_group_id;
pub const Policy = struct {
    denied: bool = false,
    pub fn authorizeRoot(self: *@This(), _: control.State, _: control.Namespace, request: Request) !void {
        try request.ensureActive();
        if (self.denied) return error.TestDenied;
    }
    pub fn validateRoot(_: *@This(), state: control.State, namespace: control.Namespace, route: metadata.CatalogRouteFence, table: []const u8, _: Request) !void {
        try std.testing.expectEqualStrings("docs", table);
        try std.testing.expectEqual(@as(u64, 7), route.table_id);
        try std.testing.expectEqual(namespace, state.roots[0].namespace);
    }
    pub fn newAttempt(_: *@This(), _: control.State, _: control.Namespace, _: Request) !control.Id {
        var actor: control.Id = undefined;
        std.testing.io.random(&actor);
        return actor;
    }
    pub fn context(_: *@This(), request: Request) !Context {
        try request.ensureActive();
        return .{};
    }
};
const Mode = enum { known, unknown_accepted, ambiguous, superseded, rejected, lose_acceptance_ack, lose_settlement_ack };
const Root = struct {
    mode: Mode = .known,
    svc: *fixture.Service,
    submits: usize = 0,
    recoveries: usize = 0,
    cancel_after: ?*bool = null,
    fn port(self: *@This()) operations.Port {
        return .{ .ptr = self, .execute_fn = execute };
    }
    fn execute(raw: *anyopaque, _: A, route: metadata.CatalogRouteFence, table: []const u8, input: operations.wire.Request, _: Context) !operations.wire.Response {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try std.testing.expectEqual(@as(u64, 17), route.route.group_id);
        try std.testing.expectEqualStrings("docs", table);
        if (input.operation == .submit) {
            self.submits += 1;
            if (self.mode == .ambiguous) return error.ReadUnavailable;
            if (self.mode == .lose_acceptance_ack) self.svc.lose_ack_at = self.svc.proposals + 1;
            if (self.mode == .lose_settlement_ack) self.svc.lose_ack_at = self.svc.proposals + 2;
        } else {
            self.recoveries += 1;
            try std.testing.expectEqualDeep((@import("antfly_local_sources").api_local_table_writes.table_write_source.history_ordered.Entry{ .term = 4, .index = 20 }), input.receipt.?);
        }
        var response: operations.wire.Response = .{ .version = 1, .receipt = input.receipt orelse .{ .term = 4, .index = 20 } };
        response.outcome = switch (self.mode) {
            .unknown_accepted => null,
            .superseded => .superseded,
            .rejected => .{ .rejected = .checkpoint_conflict },
            else => .{ .applied = .{ .consumer = .{
                .id = switch (input.command) {
                    .enroll => |enroll| enroll.consumer,
                    .renew => |renew| renew.reference.consumer,
                    else => unreachable,
                },
                .epoch = 7,
                .start = 3,
                .acknowledged = 3,
                .expires_at_seconds = if (input.command == .renew) input.command.renew.next else 1000,
                .policy_revision = 1,
            } } },
        };
        if (input.operation == .submit) if (self.cancel_after) |cancelled| {
            cancelled.* = true;
        };
        return response;
    }
};
fn testNamespace() control.Namespace {
    var ns: control.Namespace = undefined;
    std.mem.writeInt(u64, ns[0..8], 7, .big);
    std.mem.writeInt(u64, ns[8..16], 17, .big);
    std.mem.writeInt(u64, ns[16..24], 19, .big);
    return ns;
}
fn prepare(svc: *fixture.Service, policy: *Policy, a: A) !void {
    const table: Table = .{ .table_id = 7, .name = "docs", .schema_json = "{}" };
    try svc.store.applyStandaloneCommand(group, .{ .upsert_table = table });
    try svc.mutate(a, .{ .create = .{ .id = svc.id, .definition = .{ .owner = @splat(1), .authorization = @splat(2), .tables = &.{.{ .table_id = 7, .binding = control.tableBinding(table), .policy_revision = 1, .contract = .native_document }} }, .now = 100, .expires_at = 1000 } });
    try svc.mutate(a, .{ .plan_roots = &.{.{ .namespace = testNamespace(), .ownership_epoch = 1 }} });
    var state = try svc.load(a);
    defer state.deinit();
    const input: operations.wire.Request = .{ .version = 1, .operation = .submit, .command = .{ .enroll = .{ .namespace = testNamespace(), .authority = state.value.authority, .consumer = state.value.consumerId(), .definition = state.value.definition.?.identity(), .sequence = state.value.roots[0].grant_sequence, .expires_at_seconds = 1000, .policy_revision = 1 } } };
    const route: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    const command = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(command);
    const fence = try std.json.Stringify.valueAlloc(a, route, .{});
    defer a.free(fence);
    const payload: control.root_intent.Payload = .{ .command = command, .fence = fence, .table = "docs" };
    policy.denied = true;
    const before = svc.proposals;
    try std.testing.expectError(error.TestDenied, driver.prepare(svc, policy, a, svc.id, testNamespace(), payload, Request{}));
    try std.testing.expectEqual(before, svc.proposals);
    policy.denied = false;
    try driver.prepare(svc, policy, a, svc.id, testNamespace(), payload, Request{});
}
pub fn run() !void {
    const a = std.testing.allocator;
    const Case = enum { known, unknown_accepted, ambiguous, lose_begin_ack, lose_acceptance_ack, lose_settlement_ack, superseded, rejected, cancelled_after_native };
    inline for (std.meta.tags(Case)) |case| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/root-driver", .{tmp.sub_path});
        defer a.free(path);
        var store = try Store.init(a, .{ .root_dir = path });
        defer store.deinit();
        var svc: fixture.Service = .{ .store = &store, .id = @splat(4) };
        var policy: Policy = .{};
        try prepare(&svc, &policy, a);
        var cancelled = false;
        var root: Root = .{ .svc = &svc, .cancel_after = if (case == .cancelled_after_native) &cancelled else null, .mode = switch (case) {
            .known, .lose_begin_ack, .cancelled_after_native => .known,
            .superseded, .unknown_accepted => .unknown_accepted,
            .ambiguous => .ambiguous,
            .lose_acceptance_ack => .lose_acceptance_ack,
            .lose_settlement_ack => .lose_settlement_ack,
            .rejected => .rejected,
        } };
        policy.denied = true;
        try std.testing.expectError(error.TestDenied, driver.drive(&svc, &policy, root.port(), a, svc.id, testNamespace(), Request{}));
        try std.testing.expectEqual(@as(usize, 0), root.submits);
        policy.denied = false;
        var first: ?driver.Result = null;
        if (case == .lose_begin_ack) {
            svc.lose_ack_at = svc.proposals + 1;
            try std.testing.expectError(error.MetadataMutationOutcomeUnknown, driver.drive(&svc, &policy, root.port(), a, svc.id, testNamespace(), Request{}));
        } else first = try driver.drive(&svc, &policy, root.port(), a, svc.id, testNamespace(), Request{ .canceled = &cancelled });
        if (case == .cancelled_after_native) {
            try std.testing.expectEqual(.accepted, first.?.status);
            try std.testing.expectEqual(error.Canceled, first.?.deferred_error.?);
        }
        cancelled = false;
        try std.testing.expectEqual(@as(usize, if (case == .lose_begin_ack) 0 else 1), root.submits);
        svc.lose_ack_at = null;
        store.deinit();
        store = try Store.init(a, .{ .root_dir = path });
        if (first) |result| if (result.acceptance) |accepted| try driver.recordAcceptance(&svc, &policy, a, svc.id, testNamespace(), accepted, Request{});
        root.mode = if (case == .superseded) .superseded else .known;
        const recovered = try driver.drive(&svc, &policy, root.port(), a, svc.id, testNamespace(), Request{});
        switch (case) {
            .known, .rejected, .lose_settlement_ack => {
                try std.testing.expectEqual(.idle, recovered.status);
                try std.testing.expect(recovered.settlement != null);
                try std.testing.expectEqual(@as(usize, 0), root.recoveries);
            },
            .unknown_accepted, .lose_acceptance_ack, .cancelled_after_native => {
                try std.testing.expectEqual(.settled, recovered.status);
                try std.testing.expectEqual(@as(usize, 1), root.recoveries);
            },
            .superseded => {
                try std.testing.expectEqual(.superseded, recovered.status);
                try std.testing.expectEqual(@as(usize, 1), root.recoveries);
            },
            .ambiguous, .lose_begin_ack => try std.testing.expectEqual(.ambiguous, recovered.status),
        }
        try std.testing.expectEqual(@as(usize, if (case == .lose_begin_ack) 0 else 1), root.submits);
        var state = try svc.load(a);
        defer state.deinit();
        const completed = case == .known or case == .unknown_accepted or case == .lose_acceptance_ack or case == .lose_settlement_ack or case == .cancelled_after_native;
        try std.testing.expectEqual(@as(u64, if (completed) 7 else 0), state.value.roots[0].consumer_epoch);
        if (case == .rejected) try std.testing.expectEqual(@as(u8, 5), state.value.roots[0].last_control.?.effect.rejected);
        if (case == .superseded or case == .ambiguous or case == .lose_begin_ack) try std.testing.expect(state.value.roots[0].pending_control != null);
    }

    try recoverRenewal(a);
}

fn recoverRenewal(a: A) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/root-renewal", .{tmp.sub_path});
    defer a.free(path);
    var store = try Store.init(a, .{ .root_dir = path });
    defer store.deinit();
    var svc: fixture.Service = .{ .store = &store, .id = @splat(4) };
    var policy: Policy = .{};
    var root: Root = .{ .svc = &svc };
    try prepare(&svc, &policy, a);
    var prepared = try svc.load(a);
    defer prepared.deinit();
    const route = prepared.value.roots[0].pending_control.?.payload.fence;
    try std.testing.expectEqual(.settled, (try driver.drive(&svc, &policy, root.port(), a, svc.id, testNamespace(), Request{})).status);
    try svc.mutate(a, .{ .seal = .{ .cut = @splat(9) } });
    try svc.mutate(a, .{ .renew = .{ .now = 500, .expires_at = 1500 } });
    var current = try svc.load(a);
    defer current.deinit();
    const item = current.value.roots[0];
    const input: operations.wire.Request = .{ .version = 1, .operation = .submit, .command = .{ .renew = .{
        .reference = .{
            .namespace = item.namespace,
            .authority = current.value.authority,
            .consumer = current.value.consumerId(),
            .definition = current.value.definition.?.identity(),
            .grant = item.grant_sequence,
            .epoch = item.consumer_epoch,
        },
        .previous = 1000,
        .next = 1500,
    } } };
    const bytes = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(bytes);
    try driver.prepare(&svc, &policy, a, svc.id, item.namespace, .{ .command = bytes, .fence = route, .table = "docs" }, Request{});
    svc.lose_ack_at = svc.proposals + 1;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, driver.drive(&svc, &policy, root.port(), a, svc.id, item.namespace, Request{}));
    try std.testing.expectEqual(@as(usize, 1), root.submits);
    svc.lose_ack_at = null;
    var interrupted = try svc.load(a);
    defer interrupted.deinit();
    const sequence = interrupted.value.roots[0].pending_control.?.sequence;
    const result = try driver.resumeRenewal(&svc, &policy, root.port(), a, svc.id, item.namespace, sequence, Request{});
    try std.testing.expectEqual(.settled, result.status);
    try std.testing.expectEqual(@as(usize, 2), root.submits);
    var renewed = try svc.load(a);
    defer renewed.deinit();
    try std.testing.expect(!renewed.value.renewal_pending);
    try std.testing.expectEqual(@as(u64, 1500), renewed.value.roots[0].renewed_through);
}
