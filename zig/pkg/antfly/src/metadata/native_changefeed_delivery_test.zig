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
    pub fn snapshotImage(_: *@This(), _: A, _: control.State, _: []const u8, row: ?std.json.Value, _: Request) !local.table_write_source.changefeed_projection.Image {
        return .{ .row = row };
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

pub fn bootstrap() !void {
    const a = std.testing.allocator;
    const history = local.table_write_source.history_consumer;
    const pages = local.table_write_source.history_page;
    const Root = struct {
        namespace: control.Namespace,
        db: *local.db_mod.DB,
        pub fn validateScope(_: *@This(), state: control.State) !control.Table {
            return state.definition.?.tables[0];
        }
        pub fn readBootstrap(self: *@This(), alloc: A, state: control.State, after: u32, now: u64, max_rows: u32, max_bytes: u32, _: Request) !pages.BootstrapPage {
            const root = state.roots[0];
            return self.db.historyBootstrapPage(alloc, .{
                .reference = .{ .namespace = self.namespace, .authority = state.authority, .consumer = state.consumerId(), .epoch = root.consumer_epoch, .grant = root.grant_sequence, .definition = state.definition.?.identity() },
                .digest = root.snapshot.?.digest,
                .now_seconds = now,
                .after_rows = after,
                .max_rows = max_rows,
                .max_bytes = max_bytes,
            }, null);
        }
        pub fn readWithContext(_: *@This(), _: A, _: control.State, _: control.Digest, _: u64, _: u32, _: u32, _: Request) !pages.Page {
            return error.TestUnexpectedStreamingRead;
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/bootstrap-data", .{tmp.sub_path});
    defer a.free(path);
    const meta_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/bootstrap-meta", .{tmp.sub_path});
    defer a.free(meta_path);
    var db = try local.db_mod.DB.open(a, path, .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .online_source_authority = .native });
    defer db.close();
    var store = try Store.init(a, .{ .root_dir = meta_path });
    defer store.deinit();
    var svc: Service = .{ .store = &store };
    const table: Table = .{ .table_id = 7, .name = "docs", .schema_json = "{}" };
    try store.applyStandaloneCommand(group, .{ .upsert_table = table });
    const namespace = try db.nativeSourceAdmissionNamespace();
    var root: Root = .{ .namespace = namespace, .db = &db };
    var policy: Policy = .{};
    var p: provider.ProviderFor(Root, Policy) = .{ .root = &root, .policy = &policy };
    const definition: control.Definition = .{ .start = .snapshot_then_changes, .owner = @splat(1), .authorization = @splat(2), .projection = .rows, .tables = &.{.{ .table_id = 7, .binding = control.tableBinding(table), .policy_revision = 1, .contract = .native_document, .fields = &.{"value"}, .predicates = &.{.{ .field = "tenant", .value = .{ .string = "A" } }} }} };
    try svc.mutate(a, .{ .create = .{ .id = @splat(4), .definition = definition, .now = 100, .expires_at = 1000, .bootstrap_expires_at = 500 } });
    try svc.mutate(a, .{ .plan_roots = &.{.{ .namespace = namespace, .ownership_epoch = 1 }} });
    try db.batch(.{ .writes = &.{
        .{ .key = "one", .value = "{\"tenant\":\"A\",\"value\":1,\"secret\":\"never-return\"}" },
        .{ .key = "two", .value = "{\"tenant\":\"B\",\"value\":2}" },
    } });
    {
        var before = try svc.load(a);
        defer before.deinit();
        const state = before.value;
        const grant_sequence = state.roots[0].grant_sequence;
        const grant: history.Grant = .{ .namespace = namespace, .authority = state.authority, .consumer = state.consumerId(), .definition = definition.identity(), .sequence = grant_sequence, .expires_at_seconds = 1000, .policy_revision = 1 };
        const enrolled = (try db.nativeHistoryConsumerControl(.{ .capture_bootstrap = .{ .grant = grant, .expires_at_seconds = 500 } })).consumer.?;
        var identity = history.GrantIdentity.fromGrant(grant);
        identity.include_bootstrap = true;
        const pin = (try db.nativeHistoryGrant(identity)).active.bootstrap.?;
        try svc.mutate(a, .{ .enroll = .{ .namespace = namespace, .ownership_epoch = 1, .grant_sequence = grant_sequence, .consumer_epoch = enrolled.epoch, .start = enrolled.start, .snapshot = .{ .id = state.consumerId(), .digest = pin.digest, .expires_at = pin.expires_at_seconds } } });
    }
    try svc.mutate(a, .{ .seal = .{ .cut = @splat(7) } });
    var initial = try svc.load(a);
    defer initial.deinit();
    try db.batch(.{ .writes = &.{.{ .key = "one", .value = "{\"tenant\":\"A\",\"value\":9}" }} });
    var first = try coordinator.pull(&svc, &p, a, @splat(4), initial.value.latest_cursor, .{ .events = 1 }, Request{});
    defer first.deinit();
    var first_body = try body(a, first.body);
    defer first_body.deinit();
    const first_event = first_body.value.object.get("events").?.array.items[0].object;
    try std.testing.expectEqualStrings("document_snapshot", first_event.get("kind").?.string);
    try std.testing.expectEqualStrings("1", first_event.get("after").?.object.get("value").?.number_string);
    try std.testing.expect(first_event.get("after").?.object.get("secret") == null);
    try std.testing.expect(!first_body.value.object.get("snapshot").?.object.get("complete").?.bool);
    var replay = try coordinator.pull(&svc, &p, a, @splat(4), initial.value.latest_cursor, .{}, Request{});
    defer replay.deinit();
    try std.testing.expect(replay.replayed);
    try std.testing.expectEqualStrings(first.body, replay.body);
    var final = try coordinator.pull(&svc, &p, a, @splat(4), first.cursor, .{}, Request{});
    defer final.deinit();
    var final_body = try body(a, final.body);
    defer final_body.deinit();
    try std.testing.expectEqual(@as(usize, 0), final_body.value.object.get("events").?.array.items.len);
    try std.testing.expect(final_body.value.object.get("snapshot").?.object.get("complete").?.bool);
    var acknowledged = try coordinator.acknowledge(&svc, &p, a, @splat(4), final.cursor, Request{});
    defer acknowledged.deinit();
    try std.testing.expect(acknowledged.value.roots[0].snapshot_acknowledged.done);
    _ = try acknowledged.value.bootstrapCompletionProof();
}

pub fn coordinated() !void {
    const a = std.testing.allocator;
    const Page = local.table_write_source.history_page.Page;
    const record: Table = .{ .table_id = 7, .name = "docs", .schema_json = "{}" };
    const table: control.Table = .{
        .table_id = 7,
        .binding = control.tableBinding(record),
        .policy_revision = 1,
        .contract = .native_document,
        .predicates = &.{.{ .field = "tenant", .value = .{ .string = "A" } }},
    };
    const first_ns = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 7 } ++ @as([16]u8, @splat(1));
    const second_ns = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 7 } ++ @as([16]u8, @splat(2));
    const Root = struct {
        namespace: control.Namespace,
        pub fn validateScope(_: *@This(), state: control.State) !control.Table {
            try state.validate();
            return state.definition.?.tables[0];
        }
        pub fn readWithContext(_: *@This(), _: A, _: control.State, _: control.Digest, _: u64, _: u32, _: u32, _: Request) !Page {
            return error.TestExpectedProjectedRead;
        }
        pub fn readProjectedWithContext(self: *@This(), alloc: A, _: control.State, _: control.Digest, _: u64, _: u32, _: u32, _: Request) !Page {
            const visible = std.mem.eql(u8, &self.namespace, &first_ns);
            var arena = std.heap.ArenaAllocator.init(alloc);
            errdefer arena.deinit();
            const scratch = arena.allocator();
            const key = try @import("antfly_local_sources").storage_internal_keys.documentKeyAlloc(scratch, if (visible) "visible" else "hidden");
            const events = try scratch.dupe(@typeInfo(@FieldType(Page, "events")).pointer.child, &.{.{ .key = key, .before = null, .after = if (visible) "{\"tenant\":\"A\"}" else "{\"tenant\":\"B\"}" }});
            return .{
                .arena = arena,
                .header = .{ .namespace = self.namespace, .sequence = 1, .transaction_id = @splat(5), .transaction_scope = .coordinated },
                .completion = .{ .certificate_digest = @splat(6), .commit_version = 7 },
                .completion_effects = &.{ .{ .sequence = 1, .frame_digest = @splat(8) }, .{ .sequence = 1, .frame_digest = @splat(9) } },
                .events = events,
                .frame_events = 1,
                .next = .{ .sequence = 1 },
                .observed_latest = 1,
                .caught_up = true,
            };
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const meta_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/coordinated-meta", .{tmp.sub_path});
    defer a.free(meta_path);
    var store = try Store.init(a, .{ .root_dir = meta_path });
    defer store.deinit();
    try store.applyStandaloneCommand(group, .{ .upsert_table = record });
    var svc: Service = .{ .store = &store };
    const definition: control.Definition = .{ .owner = @splat(1), .authorization = @splat(2), .projection = .rows, .tables = &.{table} };
    try svc.mutate(a, .{ .create = .{ .id = @splat(4), .definition = definition, .now = 100, .expires_at = 1000 } });
    try svc.mutate(a, .{ .plan_roots = &.{ .{ .namespace = first_ns, .ownership_epoch = 1 }, .{ .namespace = second_ns, .ownership_epoch = 1 } } });
    inline for (.{ first_ns, second_ns }) |namespace| {
        var state = try svc.load(a);
        defer state.deinit();
        const root = state.value.roots[try state.value.rootIndex(namespace)];
        try svc.mutate(a, .{ .enroll = .{ .namespace = namespace, .ownership_epoch = 1, .grant_sequence = root.grant_sequence, .consumer_epoch = 1, .start = 0 } });
    }
    try svc.mutate(a, .{ .seal = .{ .cut = @splat(7) } });
    var state = try svc.load(a);
    defer state.deinit();
    var policy: Policy = .{};
    var root: Root = .{ .namespace = first_ns };
    var renderer: provider.ProviderFor(Root, Policy) = .{ .root = &root, .policy = &policy };
    const id = state.value.id;
    var first = try coordinator.pull(&svc, &renderer, a, id, state.value.latest_cursor, .{}, Request{});
    defer first.deinit();
    var initial = try body(a, first.body);
    defer initial.deinit();
    const transaction = initial.value.object.get("transaction").?.object;
    try std.testing.expect(!transaction.get("complete").?.bool);
    try std.testing.expectEqual(@as(usize, 1), initial.value.object.get("events").?.array.items.len);

    root.namespace = second_ns;
    var last = try coordinator.pull(&svc, &renderer, a, id, first.cursor, .{}, Request{});
    defer last.deinit();
    var completed = try body(a, last.body);
    defer completed.deinit();
    const ending = completed.value.object.get("transaction").?.object;
    try std.testing.expect(ending.get("complete").?.bool);
    try std.testing.expectEqualStrings(transaction.get("transaction_id").?.string, ending.get("transaction_id").?.string);
    try std.testing.expectEqual(@as(usize, 0), completed.value.object.get("events").?.array.items.len);
    var replay = try coordinator.pull(&svc, &renderer, a, id, first.cursor, .{}, Request{});
    defer replay.deinit();
    try std.testing.expect(replay.replayed);
    try std.testing.expectEqualStrings(last.body, replay.body);
}
