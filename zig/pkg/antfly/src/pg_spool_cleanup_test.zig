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
const control = @import("metadata/changefeed_control.zig");
const cleanup = @import("metadata/pg_spool_cleanup.zig");
const spool = @import("metadata/pg_spool.zig");
const adapter = @import("metadata/pg_spool_artifacts.zig");
const Store = @import("metadata/storage/raft_apply_store.zig").RaftApplyStore;
const Table = @import("metadata/domain.zig").TableRecord;
const artifacts = @import("serverless/artifacts/store.zig");
const FsStore = @import("serverless/artifacts/fs_store.zig").FsStore;
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const group = @import("antfly_local_sources").common_group_ids.main_metadata_group_id;
const A = std.mem.Allocator;
pub const Request = struct {
    canceled: ?*const bool = null,
    pub fn ensureActive(self: @This()) !void {
        if (self.canceled) |value| if (value.*) return error.Canceled;
    }
};
pub const Service = struct {
    store: *Store,
    id: control.Id,
    metadata_group_id: u64 = group,
    proposals: u64 = 0,
    lose_ack: bool = false,
    lose_ack_at: ?u64 = null,
    pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: anytype, version: u16) !void {
        try std.testing.expectEqual(control.protocol_version, version);
    }
    pub fn ensureLinearizableReadWithContext(_: *@This(), _: anytype) !void {}
    pub fn projectedStore(self: *@This()) ?*Store {
        return self.store;
    }
    pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: anytype) !u64 {
        try self.store.applyStandaloneCommand(group, .{ .apply_changefeed_control = command.apply_changefeed_control });
        self.proposals += 1;
        return self.proposals;
    }
    pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: anytype) !void {
        if (self.lose_ack or self.lose_ack_at == self.proposals) return error.TestLostAcknowledgment;
    }
    pub fn load(self: *@This(), a: A) !control.Parsed {
        const bytes = try self.store.getChangefeedControl(a, group, self.id);
        defer a.free(bytes);
        return control.parse(a, bytes);
    }
    pub fn mutate(self: *@This(), a: A, mutation: control.Mutation) !void {
        var before = try self.load(a);
        defer before.deinit();
        try control.mutateOnService(self, a, .{ .id = self.id, .authority = @splat(9), .owner = @splat(1), .authorization = @splat(2), .expected_revision = before.value.revision, .expected_epoch = before.value.epoch, .mutation = mutation }, Request{});
    }
};
const Registry = struct {
    fs: *FsStore,
    denied: bool = false,
    namespace: [32]u8 = @splat(9),
    calls: usize = 0,
    deletes: usize = 0,
    fail_delete: ?usize = null,
    force_nonempty: bool = false,
    cancel_after_delete: ?*bool = null,
    interfere: ?*Service = null,
    pub fn authorizeCleanup(self: *@This(), state: control.State, _: Request) !void {
        if (self.denied or state.definition == null or !std.mem.eql(u8, &state.definition.?.owner, &@as(control.Digest, @splat(1)))) return error.TestDenied;
    }
    pub fn artifactStore(self: *@This(), _: control.pg_slot.spool_control.Scope, _: Request) !artifacts.ArtifactStore {
        self.calls += 1;
        return .{ .allocator = std.testing.allocator, .ptr = self, .namespace_identity = self.namespace, .vtable = &vtable };
    }
    fn cast(raw: *anyopaque) *@This() {
        return @ptrCast(@alignCast(raw));
    }
    fn retire(raw: *anyopaque, domain: [32]u8, through: u64, token: CancellationToken) !void {
        return cast(raw).fs.retireScopedUploads(domain, through, token);
    }
    fn verify(raw: *anyopaque, scope: artifacts.UploadScope, token: CancellationToken) !bool {
        const self = cast(raw);
        const empty = try self.fs.scopedUploadsReclaimed(scope, token);
        if (self.interfere) |svc| {
            self.interfere = null;
            var current = try svc.load(std.testing.allocator);
            defer current.deinit();
            const record = current.value.slot.?.spool.records[0];
            var progress = record.cleanup;
            progress.revision += 1;
            try svc.mutate(std.testing.allocator, .{ .slot = .{ .checkpoint_spool_cleanup = .{ .id = record.id, .storage = record.spec.storage, .expected = record.cleanup.revision, .progress = progress } } });
        }
        return empty and !self.force_nonempty;
    }
    fn visit(raw: *anyopaque, domain: [32]u8, visitor: artifacts.ScopedUploadVisitor, token: CancellationToken) !void {
        var store = cast(raw).fs.artifactStore();
        return store.visitScopedUploads(domain, visitor, token);
    }
    fn remove(raw: *anyopaque, id: []const u8, token: CancellationToken) !void {
        const self = cast(raw);
        self.deletes += 1;
        if (self.fail_delete == self.deletes) return error.TestDeleteFailure;
        var store = self.fs.artifactStore();
        try store.deleteWithCancellation(id, token);
        if (self.cancel_after_delete) |value| value.* = true;
    }
    fn deinit(_: A, _: *anyopaque) void {}
    fn put(_: *anyopaque, _: A, _: []const u8) !artifacts.ArtifactMetadata {
        return error.Unsupported;
    }
    fn get(_: *anyopaque, _: A, _: []const u8) ![]u8 {
        return error.Unsupported;
    }
    fn range(_: *anyopaque, _: A, _: []const u8, _: u64, _: usize) ![]u8 {
        return error.Unsupported;
    }
    fn stat(_: *anyopaque, _: A, _: []const u8) !artifacts.ArtifactMetadata {
        return error.Unsupported;
    }
    fn removeBare(_: *anyopaque, _: []const u8) !void {
        return error.Unsupported;
    }
    const vtable: artifacts.ArtifactStore.VTable = .{ .deinit = deinit, .put = put, .get_alloc = get, .get_range_alloc = range, .stat = stat, .delete = removeBare, .delete_with_cancellation = remove, .retire_scoped_uploads = retire, .scoped_uploads_reclaimed = verify, .visit_scoped_uploads = visit };
};
fn namespace() control.Namespace {
    var value: control.Namespace = @splat(0);
    std.mem.writeInt(u64, value[0..8], 7, .big);
    std.mem.writeInt(u64, value[8..16], 1, .big);
    std.mem.writeInt(u64, value[16..24], 1, .big);
    return value;
}
fn build(svc: *Service, fs: *FsStore, a: A, id: u64, rows: u32) !spool.Seal {
    var before = try svc.load(a);
    defer before.deinit();
    const state = before.value;
    const session = state.slot.?.session.?;
    const now = state.slot.?.observed_at + 1;
    var delivery: control.Delivery = .{ .sequence = state.latest_delivery + 1, .before = state.latest_cursor, .cursor = @splat(0), .authority = @splat(8), .issued_at = now, .advances = &.{.{ .namespace = namespace(), .position = .{ .sequence = 10 + id } }} };
    delivery.cursor = delivery.identity(state.id, state.epoch);
    try svc.mutate(a, .{ .issue = delivery });
    const scope = try artifacts.UploadScope.forPublication(control.pg_slot.spool_control.Scope.domainFor(state.id, state.epoch), id, std.testing.io);
    var store = fs.artifactStore();
    store.namespace_identity = @splat(9);
    var writer = try adapter.Adapter.init(store, scope, .write, .{});
    const bytes = 47 + @as(u64, rows) * 45;
    const plan: spool.Plan = .{ .binding = .{ .feed = state.id, .epoch = state.epoch, .definition = state.definition.?.identity(), .projection = @splat(8), .storage = writer.storageIdentity() }, .before = state.slot.?.mapped_cursor, .after = delivery.cursor, .completion = @splat(4), .start_lsn = state.slot.?.next_lsn, .bytes = bytes, .commit_offset = bytes - 26, .xid = 42, .commit_time = -1 };
    const accounting_scope: control.pg_slot.spool_control.Scope = .{ .namespace = @splat(9), .domain = scope.domain, .attempt = scope.attempt };
    try svc.mutate(a, .{ .slot = .{ .reserve_spool = .{ .session = session, .now = now, .id = id, .scope = accounting_scope, .spec = control.pg_slot.spool_control.Spec.fromPlan(plan) } } });
    const sealed = try spool.testBuild(a, &writer, plan, rows, 32);
    _ = try writer.recordClosure(sealed);
    _ = try spool.Reader.open(a, &writer, sealed.identity, plan.binding);
    try svc.mutate(a, .{ .slot = .{ .publish_spool = .{ .session = session, .now = now + 1, .id = id, .manifest = sealed.identity, .content = sealed.serialization } } });
    return sealed;
}
pub fn run() !void {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/runtime-spool", .{tmp.sub_path});
    defer a.free(path);
    const meta_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/runtime-meta", .{tmp.sub_path});
    defer a.free(meta_path);
    var fs = try FsStore.init(a, path);
    defer fs.deinit();
    var meta = try Store.init(a, .{ .root_dir = meta_path });
    defer meta.deinit();
    const id = control.pg_slot.feedId(@splat(1), "cleanup_slot");
    var svc: Service = .{ .store = &meta, .id = id };
    var registry: Registry = .{ .fs = &fs };
    const table: Table = .{ .table_id = 7, .name = "rows", .schema_json = "{}" };
    try meta.applyStandaloneCommand(group, .{ .upsert_table = table });
    const definition: control.Definition = .{ .owner = @splat(1), .authorization = @splat(2), .projection = .rows, .tables = &.{.{ .table_id = 7, .binding = control.tableBinding(table), .policy_revision = 1, .contract = .native_relational }} };
    try svc.mutate(a, .{ .create = .{ .id = id, .definition = definition, .now = 100, .expires_at = 1000 } });
    try svc.mutate(a, .{ .plan_roots = &.{.{ .namespace = namespace(), .ownership_epoch = 1 }} });
    {
        var current = try svc.load(a);
        defer current.deinit();
        try svc.mutate(a, .{ .enroll = .{ .namespace = namespace(), .ownership_epoch = 1, .grant_sequence = current.value.roots[0].grant_sequence, .consumer_epoch = 1, .start = 10 } });
    }
    try svc.mutate(a, .{ .seal = .{ .cut = @splat(7) } });
    try svc.mutate(a, .{ .slot = .{ .configure = .{ .database = @splat(1), .name = "cleanup_slot" } } });
    try svc.mutate(a, .{ .slot = .{ .acquire = .{ .actor = @splat(2), .expected = 0, .now = 100, .expires = 150 } } });
    const first = try build(&svc, &fs, a, 1, 64);
    const limits: cleanup.Limits = .{ .work = 32, .deletions = 8 };
    try std.testing.expectEqual(.waiting, (try cleanup.cleanup(&svc, &registry, a, id, limits, Request{})).status);
    try std.testing.expectEqual(@as(usize, 0), registry.calls);
    try svc.mutate(a, .{ .slot = .{ .confirm = .{ .session = .{ .actor = @splat(2), .fence = 1 }, .now = 104, .flushed = first.plan.start_lsn + first.plan.bytes } } });
    // A later live transaction shares this domain and must survive every sweep.
    const live = try build(&svc, &fs, a, 2, 1);
    registry.denied = true;
    try std.testing.expectError(error.TestDenied, cleanup.cleanup(&svc, &registry, a, id, limits, Request{}));
    registry.denied = false;
    registry.namespace = @splat(10);
    try std.testing.expectError(error.PgSpoolStorageUnsupported, cleanup.cleanup(&svc, &registry, a, id, limits, Request{}));
    registry.namespace = @splat(9);
    var canceled = true;
    try std.testing.expectError(error.Canceled, cleanup.cleanup(&svc, &registry, a, id, limits, Request{ .canceled = &canceled }));
    canceled = false;
    svc.lose_ack = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, cleanup.cleanup(&svc, &registry, a, id, limits, Request{}));
    svc.lose_ack = false;
    try std.testing.expectEqual(@as(usize, 0), registry.deletes);
    // The fence was durably checkpointed despite losing its metadata response.
    {
        var current = try svc.load(a);
        defer current.deinit();
        try std.testing.expect(current.value.slot.?.spool.records[0].cleanup.fenced);
    }
    // A canonical metadata token can become obsolete for its storage provider.
    // Reset only this private cursor; keep the original fence and reservation.
    try svc.mutate(a, .{ .slot = .{ .checkpoint_spool_cleanup = .{ .id = 1, .storage = first.plan.binding.storage, .expected = 1, .progress = .{ .revision = 2, .fenced = true, .continuation = "YmFk" } } } });
    try std.testing.expectEqual(.checkpointed, (try cleanup.cleanup(&svc, &registry, a, id, limits, Request{})).status);
    try std.testing.expectEqual(@as(usize, 0), registry.deletes);
    {
        var reset = try svc.load(a);
        defer reset.deinit();
        try std.testing.expectEqualStrings("", reset.value.slot.?.spool.records[0].cleanup.continuation);
        try std.testing.expect(reset.value.slot.?.spool.records[0].cleanup.fenced);
    }
    registry.cancel_after_delete = &canceled;
    try std.testing.expectError(error.Canceled, cleanup.cleanup(&svc, &registry, a, id, limits, Request{ .canceled = &canceled }));
    registry.cancel_after_delete = null;
    canceled = false;
    registry.fail_delete = registry.deletes + 2;
    try std.testing.expectError(error.TestDeleteFailure, cleanup.cleanup(&svc, &registry, a, id, limits, Request{}));
    registry.fail_delete = null;
    registry.interfere = &svc;
    try std.testing.expectError(error.ChangefeedConflict, cleanup.cleanup(&svc, &registry, a, id, limits, Request{}));
    var turns: usize = 0;
    var continued = false;
    var lost_page_ack = false;
    var lost_reclaim_ack = false;
    var checked_absence_proof = false;
    registry.force_nonempty = true;
    while (true) {
        // Keep no in-process provider cursor across calls.
        meta.deinit();
        meta = try Store.init(a, .{ .root_dir = meta_path });
        fs.deinit();
        fs = try FsStore.initExisting(a, path);
        var current = try svc.load(a);
        defer current.deinit();
        const ledger = current.value.slot.?.spool;
        if (ledger.collected_through == 1) break;
        try std.testing.expect(ledger.charged_bytes > try control.pg_slot.spool_control.reservationBytes(live.plan.bytes));
        continued = continued or ledger.records[0].cleanup.continuation.len != 0;
        svc.lose_ack = (!lost_page_ack and !ledger.records[0].cleanup.complete) or (!lost_reclaim_ack and ledger.records[0].cleanup.complete);
        const result = cleanup.cleanup(&svc, &registry, a, id, limits, Request{}) catch |err| switch (err) {
            error.MetadataMutationOutcomeUnknown => {
                if (ledger.records[0].cleanup.complete) lost_reclaim_ack = true else lost_page_ack = true;
                svc.lose_ack = false;
                continue;
            },
            else => return err,
        };
        try std.testing.expect(result.work <= limits.work);
        try std.testing.expect(result.deletions <= limits.deletions);
        if (!checked_absence_proof and result.status == .retained) {
            const scope = ledger.records[0].scope;
            if (try fs.scopedUploadsReclaimed(.{ .domain = scope.domain, .attempt = scope.attempt }, .{})) {
                var observed = try svc.load(a);
                defer observed.deinit();
                try std.testing.expect(!observed.value.slot.?.spool.records[0].cleanup.complete);
                try std.testing.expectEqual(ledger.charged_bytes, observed.value.slot.?.spool.charged_bytes);
                registry.force_nonempty = false;
                checked_absence_proof = true;
            }
        }
        turns += 1;
        try std.testing.expect(turns < 300);
    }
    try std.testing.expect(continued and lost_page_ack and lost_reclaim_ack and checked_absence_proof);
    try std.testing.expect(turns > 1);
    var remaining = try svc.load(a);
    defer remaining.deinit();
    try std.testing.expectEqual(@as(usize, 1), remaining.value.slot.?.spool.records.len);
    try std.testing.expectEqual(@as(u64, 2), remaining.value.slot.?.spool.records[0].id);
    try std.testing.expectEqual(.live, remaining.value.slot.?.spool.records[0].phase);
    try std.testing.expectEqual(try control.pg_slot.spool_control.reservationBytes(live.plan.bytes), remaining.value.slot.?.spool.charged_bytes);
    try std.testing.expectEqual(.waiting, (try cleanup.cleanup(&svc, &registry, a, id, limits, Request{})).status);
    const live_scope = remaining.value.slot.?.spool.records[0].scope;
    var store = fs.artifactStore();
    store.namespace_identity = @splat(9);
    var reader_store = try adapter.Adapter.init(store, .{ .domain = live_scope.domain, .attempt = live_scope.attempt }, .read, .{});
    const reader = try spool.Reader.open(a, &reader_store, live.identity, live.plan.binding);
    const message = (try reader.message(a, &reader_store, 21, 1024)).?;
    defer a.free(message);
    try std.testing.expectEqual(@as(u8, 'I'), message[0]);
}
