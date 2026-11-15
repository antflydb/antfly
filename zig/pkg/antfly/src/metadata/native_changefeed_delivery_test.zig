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

//! End-to-end private provider qualification. The policy fixture owns the entire
//! single-root test universe; production authorization/routing is not mocked as
//! a shipped capability by this fixture.
const std = @import("std");
const local = @import("antfly_local_sources").api_local_table_writes;
const control = @import("changefeed_control.zig");
const coordinator = @import("changefeed_delivery.zig");
const native = @import("native_changefeed_root.zig");
const provider = @import("native_changefeed_delivery.zig");
const Store = @import("storage/raft_apply_store.zig").RaftApplyStore;
const Table = @import("domain.zig").TableRecord;
const group = @import("antfly_local_sources").common_group_ids.main_metadata_group_id;
const A = std.mem.Allocator;
const Request = struct {
    pub fn ensureActive(_: @This()) !void {}
};
const Service = struct {
    store: *Store,
    metadata_group_id: u64 = group,
    proposals: u64 = 0,
    lose_ack: bool = false,
    pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: Request, version: u16) !void {
        try std.testing.expectEqual(control.protocol_version, version);
    }
    pub fn ensureLinearizableReadWithContext(_: *@This(), _: Request) !void {}
    pub fn projectedStore(self: *@This()) ?*Store {
        return self.store;
    }
    pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: anytype) !u64 {
        try self.store.applyStandaloneCommand(group, .{ .apply_changefeed_control = command.apply_changefeed_control });
        self.proposals += 1;
        return self.proposals;
    }
    pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: Request) !void {
        if (self.lose_ack) return error.TestLostAcknowledgment;
    }
    fn load(self: *@This(), a: A) !control.Parsed {
        const bytes = try self.store.getChangefeedControl(a, group, @splat(4));
        defer a.free(bytes);
        return control.parse(a, bytes);
    }
    fn mutate(self: *@This(), a: A, mutation: control.Mutation) !void {
        var before = try self.load(a);
        defer before.deinit();
        try control.mutateOnService(self, a, .{ .id = @splat(4), .authority = @splat(9), .owner = @splat(1), .authorization = @splat(2), .expected_revision = before.value.revision, .expected_epoch = before.value.epoch, .mutation = mutation }, Request{});
    }
};
const Policy = struct {
    denied: bool = false,
    pub fn authorize(self: *@This(), state: control.State, _: Request) !void {
        if (self.denied or state.definition == null or !std.mem.eql(u8, &state.definition.?.owner, &@as(control.Digest, @splat(1)))) return error.TestDenied;
        // The fixture's complete source set is exactly one known local root.
        if (state.definition.?.tables.len != 1 or state.definition.?.tables[0].policy_revision != 1) return error.ChangefeedResynchronizationRequired;
    }
    pub fn nowSeconds(_: *@This(), _: Request) !u64 {
        return 200;
    }
    pub fn images(_: *@This(), _: A, _: control.State, _: anytype, _: []const u8, before: ?std.json.Value, after: ?std.json.Value, _: Request) !provider.Images {
        return .{ .before = .{ .row = before }, .after = .{ .row = after } };
    }
};
fn body(a: A, bytes: []const u8) !std.json.Parsed(std.json.Value) {
    return std.json.parseFromSlice(std.json.Value, a, bytes, .{ .allocate = .alloc_always, .parse_numbers = false });
}
fn acknowledge(svc: *Service, p: anytype, root: *native.Adapter, a: A, cursor: control.Digest) !void {
    var state = try coordinator.acknowledge(svc, p, a, @splat(4), cursor, Request{});
    defer state.deinit();
    try svc.mutate(a, try root.flushAcknowledged(state.value));
}
pub fn run() !void {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/provider-data", .{tmp.sub_path});
    defer a.free(path);
    const meta_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/provider-meta", .{tmp.sub_path});
    defer a.free(meta_path);
    var db = try local.db_mod.DB.open(a, path, .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .online_source_authority = .native });
    defer db.close();
    var store = try Store.init(a, .{ .root_dir = meta_path });
    defer store.deinit();
    var svc: Service = .{ .store = &store };
    const table: Table = .{ .table_id = 7, .name = "docs", .schema_json = "{}" };
    try store.applyStandaloneCommand(group, .{ .upsert_table = table });
    var bound = local.BoundTableWriteSource.init("docs", &db);
    var root = try native.Adapter.init(bound.source(), table);
    var policy: Policy = .{};
    var p: provider.Provider(Policy) = .{ .root = &root, .policy = &policy };
    const definition: control.Definition = .{ .owner = @splat(1), .authorization = @splat(2), .projection = .rows, .tables = &.{.{ .table_id = 7, .binding = control.tableBinding(table), .policy_revision = 1, .contract = .native_document, .fields = &.{"value"}, .predicates = &.{.{ .field = "tenant", .value = .{ .string = "A" } }} }} };
    try svc.mutate(a, .{ .create = .{ .id = @splat(4), .definition = definition, .now = 100, .expires_at = 1000 } });
    {
        var state = try svc.load(a);
        defer state.deinit();
        const plan = try root.plan(state.value);
        try svc.mutate(a, .{ .plan_roots = &.{plan} });
    }
    {
        var state = try svc.load(a);
        defer state.deinit();
        try svc.mutate(a, try root.enroll(state.value));
    }
    try svc.mutate(a, .{ .seal = .{ .cut = @splat(7) } });
    var initial = try svc.load(a);
    defer initial.deinit();
    const start = initial.value.latest_cursor;
    try db.batch(.{ .writes = &.{ .{ .key = "one", .value = "{\"tenant\":\"A\",\"value\":1,\"secret\":\"never-return\"}" }, .{ .key = "two", .value = "{\"tenant\":\"B\",\"value\":2}" } } });
    svc.lose_ack = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, coordinator.pull(&svc, &p, a, @splat(4), start, .{ .events = 1 }, Request{}));
    svc.lose_ack = false;
    // A current-row lookup would now reconstruct the wrong historical update.
    try db.batch(.{ .writes = &.{.{ .key = "one", .value = "{\"tenant\":\"A\",\"value\":99}" }} });
    var first = try coordinator.pull(&svc, &p, a, @splat(4), start, .{ .events = 1 }, Request{});
    defer first.deinit();
    try std.testing.expect(first.replayed);
    var first_body = try body(a, first.body);
    defer first_body.deinit();
    const first_event = first_body.value.object.get("events").?.array.items[0];
    try std.testing.expectEqualStrings("1", first_event.object.get("after").?.object.get("value").?.number_string);
    try std.testing.expect(!first_event.object.get("after").?.object.contains("tenant"));
    try std.testing.expect(std.mem.indexOf(u8, first.body, "never-return") == null);
    try std.testing.expect(!first_body.value.object.get("transaction").?.object.get("complete").?.bool);
    try acknowledge(&svc, &p, &root, a, first.cursor);
    var second = try coordinator.pull(&svc, &p, a, @splat(4), first.cursor, .{ .events = 1 }, Request{});
    defer second.deinit();
    var second_body = try body(a, second.body);
    defer second_body.deinit();
    try std.testing.expectEqual(@as(usize, 0), second_body.value.object.get("events").?.array.items.len);
    try std.testing.expect(second_body.value.object.get("transaction").?.object.get("complete").?.bool);
    try std.testing.expectEqualStrings(first_body.value.object.get("transaction").?.object.get("transaction_id").?.string, second_body.value.object.get("transaction").?.object.get("transaction_id").?.string);
    try std.testing.expect(std.mem.indexOf(u8, second.body, "two") == null);
    try acknowledge(&svc, &p, &root, a, second.cursor);
    var updated = try coordinator.pull(&svc, &p, a, @splat(4), second.cursor, .{}, Request{});
    defer updated.deinit();
    var update_body = try body(a, updated.body);
    defer update_body.deinit();
    const update = update_body.value.object.get("events").?.array.items[0];
    try std.testing.expectEqualStrings("1", update.object.get("before").?.object.get("value").?.number_string);
    try std.testing.expectEqualStrings("99", update.object.get("after").?.object.get("value").?.number_string);
    try acknowledge(&svc, &p, &root, a, updated.cursor);
    try db.batch(.{ .writes = &.{.{ .key = "one", .value = "{\"tenant\":\"B\",\"value\":77}" }} });
    var left = try coordinator.pull(&svc, &p, a, @splat(4), updated.cursor, .{}, Request{});
    defer left.deinit();
    var leave_body = try body(a, left.body);
    defer leave_body.deinit();
    const leave = leave_body.value.object.get("events").?.array.items[0];
    try std.testing.expectEqualStrings("leave", leave.object.get("operation").?.string);
    try std.testing.expect(leave.object.get("after").? == .null);
    try acknowledge(&svc, &p, &root, a, left.cursor);
    try db.batch(.{ .writes = &.{.{ .key = "one", .value = "{\"tenant\":\"A\",\"value\":88}" }} });
    var entered = try coordinator.pull(&svc, &p, a, @splat(4), left.cursor, .{}, Request{});
    defer entered.deinit();
    var enter_body = try body(a, entered.body);
    defer enter_body.deinit();
    const enter = enter_body.value.object.get("events").?.array.items[0];
    try std.testing.expectEqualStrings("enter", enter.object.get("operation").?.string);
    try std.testing.expect(enter.object.get("before").? == .null);
    try acknowledge(&svc, &p, &root, a, entered.cursor);
    try db.batch(.{ .writes = &.{.{ .key = "hidden", .value = "{\"tenant\":\"B\",\"value\":100}" }} });
    var hidden = try coordinator.pull(&svc, &p, a, @splat(4), entered.cursor, .{}, Request{});
    defer hidden.deinit();
    var hidden_body = try body(a, hidden.body);
    defer hidden_body.deinit();
    try std.testing.expectEqual(@as(usize, 0), hidden_body.value.object.get("events").?.array.items.len);
    try std.testing.expect(hidden_body.value.object.get("transaction").? == .null);
    try acknowledge(&svc, &p, &root, a, hidden.cursor);
    var heartbeat = try coordinator.pull(&svc, &p, a, @splat(4), hidden.cursor, .{}, Request{});
    defer heartbeat.deinit();
    try db.batch(.{ .writes = &.{.{ .key = "one", .value = "{\"tenant\":\"A\",\"value\":89}" }} });
    var old_heartbeat = try coordinator.pull(&svc, &p, a, @splat(4), hidden.cursor, .{}, Request{});
    defer old_heartbeat.deinit();
    try std.testing.expect(old_heartbeat.replayed);
    try std.testing.expectEqualStrings(heartbeat.body, old_heartbeat.body);
    try acknowledge(&svc, &p, &root, a, heartbeat.cursor);
    var latest = try coordinator.pull(&svc, &p, a, @splat(4), heartbeat.cursor, .{}, Request{});
    defer latest.deinit();
    try acknowledge(&svc, &p, &root, a, latest.cursor);
    const txn = try db.beginTransaction(5000);
    try db.writeTransaction(txn, .{ .writes = &.{.{ .key = "coordinated", .value = "{\"tenant\":\"A\",\"value\":90}" }} });
    try db.commitTransaction(txn, 5001);
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, coordinator.pull(&svc, &p, a, @splat(4), latest.cursor, .{}, Request{}));
    policy.denied = true;
    try std.testing.expectError(error.TestDenied, coordinator.pull(&svc, &p, a, @splat(4), @splat(99), .{}, Request{}));
}
