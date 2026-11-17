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
const local = @import("antfly_local_sources");
const authority = @import("source_retirement_authority.zig");
const catalog = @import("table_catalog.zig");
const metadata = @import("../metadata/api.zig");
const tables = @import("../metadata/table_manager.zig");
const decisions = @import("history_decision_operations.zig");
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;
pub fn run() !void {
    const a = std.testing.allocator;
    var records = [_]tables.TableRecord{.{ .table_id = 7, .name = "docs", .schema_json = "{}" }};
    records[0].routing_storage_binding = local.common_topology_records.tableStorageBinding(records[0]);
    const ranges = [_]tables.RangeRecord{.{ .table_id = 7, .group_id = 601, .range_id = 13, .start_key = "", .end_key = null, .doc_identity_shard_id = 11, .doc_identity_range_id = 13 }};
    const generation = try catalog.RoutingGeneration.create(a, .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .catalog_revision = 10, .tables = &records, .ranges = @constCast(&ranges) }, .{});
    defer generation.release();
    const view = try generation.indexed.tableRoutingView("docs", .{});
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 11, .big);
    std.mem.writeInt(u64, namespace[16..24], 13, .big);
    const owner: @FieldType(local.storage_source_scope_retirement.Intent, "owner") = .{ .metadata_group_id = 1, .metadata_incarnation = generation.indexed.snapshot.value.metadata_incarnation.?, .namespace = namespace, .table_binding = @import("../metadata/changefeed_control.zig").tableBinding(records[0]), .root_generation = 0, .group_id = 601, .topology_epoch = view.topology_epoch };
    var fixture: @import("../data/source_retirement_test_fixture.zig").Fixture = .{};
    try fixture.init(owner);
    const locator = "table2:00000004:docs:601";
    fixture.members[0].locator = locator;
    const admission_digest = fixture.command.observation.decision.?.admission.admission.digest();
    fixture.command.observation.decision.?.admission.digest = admission_digest;
    fixture.command.intent.admission_digest = admission_digest;
    fixture.command.intent.coordinator.minimum_applied_index = 3;
    const Fake = struct {
        generation: *catalog.RoutingGeneration,
        reply: decisions.wire.Reply,
        calls: usize = 0,
        next_index: u64 = 6,
        mode: enum { aborted, absent, pending, committed, wrong_scope, unresolved } = .aborted,
        fn acquire(ptr: *anyopaque, budget: catalog.RoutingBudget, authoritative: bool) !*catalog.RoutingGeneration {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try budget.checkpoint();
            try std.testing.expect(authoritative);
            self.generation.retain();
            return self.generation;
        }
        fn admin(_: *anyopaque) !metadata.AdminSnapshot {
            return error.UnexpectedAdminSnapshot;
        }
        fn freeAdmin(_: *anyopaque, _: *metadata.AdminSnapshot) void {}
        fn read(ptr: *anyopaque, alloc: A, fence: metadata.CatalogRouteFence, table: []const u8, query: decisions.Request, context: Context) !decisions.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            try query.validateFence(fence);
            try std.testing.expectEqualStrings("docs", table);
            try std.testing.expectEqual(@as(u64, 3), query.minimum_applied_index);
            self.calls += 1;
            var reply = self.reply;
            reply.request_digest = query.binding().digest();
            reply.cut.index = self.next_index;
            self.next_index += 1;
            switch (self.mode) {
                .aborted => {},
                .absent => reply.decision = null,
                .pending => {
                    reply.decision.?.record.status = .pending;
                    reply.decision.?.record.finalized_at_ns = 0;
                    reply.decision.?.record.intents_resolved = false;
                },
                .committed => {
                    reply.decision.?.record.status = .committed;
                    reply.decision.?.record.commit_version = 21;
                },
                .wrong_scope => reply.decision.?.record.source_scope.?.token += 1,
                .unresolved => reply.decision.?.record.intents_resolved = false,
            }
            const bytes = try decisions.wire.encodeReply(alloc, query.binding(), reply);
            defer alloc.free(bytes);
            return decisions.decodeReply(alloc, bytes, query);
        }
    };
    var fake: Fake = .{ .generation = generation, .reply = fixture.command.observation };
    const port: authority.Authority = .{ .catalog_source = .{ .ptr = &fake, .vtable = &.{ .admin_snapshot = Fake.admin, .free_admin_snapshot = Fake.freeAdmin, .acquire_routing_generation = Fake.acquire } }, .decision_port = .{ .ptr = &fake, .read_fn = Fake.read } };
    var first = try port.capture(a, fixture.command.intent, locator, .{});
    defer first.deinit();
    var second = try port.capture(a, fixture.command.intent, locator, .{});
    defer second.deinit();
    try std.testing.expectEqual(@as(u64, 6), first.command.observation.cut.index);
    try std.testing.expectEqual(@as(u64, 7), second.command.observation.cut.index);
    fixture.members[0].locator = "released original locator";
    try std.testing.expectEqualStrings(locator, first.command.observation.decision.?.admission.admission.participants[0].locator);
    _ = try first.command.proof();
    fixture.members[0].locator = locator;
    for ([_]@TypeOf(fake.mode){ .absent, .pending, .committed }) |mode| {
        fake.mode = mode;
        try std.testing.expectError(error.SourceOutcomeUnresolved, port.capture(a, fixture.command.intent, locator, .{}));
    }
    for ([_]@TypeOf(fake.mode){ .wrong_scope, .unresolved }) |mode| {
        fake.mode = mode;
        try std.testing.expectError(error.SourceTransactionIdentityConflict, port.capture(a, fixture.command.intent, locator, .{}));
    }
    fake.mode = .aborted;
    var changed = fixture.command.intent;
    changed.admission_digest[0] ^= 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, port.capture(a, changed, locator, .{}));
    fixture.members[0].locator = "different retained locator";
    const changed_digest = fake.reply.decision.?.admission.admission.digest();
    fake.reply.decision.?.admission.digest = changed_digest;
    changed = fixture.command.intent;
    changed.admission_digest = changed_digest;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, port.capture(a, changed, locator, .{}));
    fixture.members[0].locator = locator;
    fake.reply.decision.?.admission.digest = admission_digest;
    const calls_before = fake.calls;
    changed = fixture.command.intent;
    changed.coordinator.owner.topology_epoch += 1;
    try std.testing.expectError(error.TopologyChanged, port.capture(a, changed, locator, .{}));
    var cancelled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, port.capture(a, fixture.command.intent, locator, .{ .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled) }));
    try std.testing.expectEqual(calls_before, fake.calls);
}
