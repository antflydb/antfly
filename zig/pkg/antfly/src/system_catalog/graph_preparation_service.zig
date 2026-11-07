// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Capability admission before trigger preparation, using the existing native
//! metadata nonce journal. This is private control work, not a SQL statement
//! effect, and an ambiguous proposal is observed rather than replayed.
const std = @import("std");
const operation = @import("antfly_local_sources").api_operation;
const contract = @import("graph_preparation.zig");
const protocol = @import("../metadata/topology_protocol.zig");
const A = std.mem.Allocator;
fn requirePrivate(context: operation.RequestContext, request: contract.Mutation) !void {
    // HTTP receivers install their own proposal-submission tracker after
    // authenticating the private grant. It records delivery uncertainty and
    // carries no enclosing SQL command or completion authority.
    if (!context.setting_admin or context.sql_command_fence != null or context.sql_completion != null or request.admission.command_fence != null) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
}
fn prior(a: A, store: anytype, group: u64, request: contract.Observation) !?[]u8 {
    const result = try store.sqlGraphPreparationOutcome(a, group, request);
    if (std.mem.eql(u8, result, "null")) {
        a.free(result);
        return null;
    }
    return result;
}
pub fn admit(svc: anytype, a: A, context: operation.RequestContext, request: contract.Mutation) ![]u8 {
    try requirePrivate(context, request);
    const observation = try contract.observation(a, request);
    try observation.validate(a);
    // A retained body-bound result is authoritative even if the source cut
    // moved or a member went away after its actual committed admission.
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    if (try prior(a, store, svc.metadata_group_id, observation)) |result| return result;
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.sql_table_triggers_version);
    const preview = try store.sqlGraphPreparationRead(a, svc.metadata_group_id, request.source, context);
    a.free(preview);
    const encoded = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(encoded);
    if (encoded.len > contract.max_command_bytes) return error.CatalogCommandTooLarge;
    svc.lockCatalogMutation();
    var locked = true;
    defer if (locked) svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    // Another coordinator may have resolved this exact nonce while the
    // catalog lane was occupied. Do not spend a second proposal on it.
    if (try prior(a, store, svc.metadata_group_id, observation)) |result| return result;
    const current = try store.sqlGraphPreparationRead(a, svc.metadata_group_id, request.source, context);
    a.free(current);
    const receipt = try @import("../metadata/service.zig").proposeCatalogWithContext(svc, context, .{ .apply_sql_graph_preparation = encoded });
    svc.unlockCatalogMutation();
    locked = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    return (prior(a, store, svc.metadata_group_id, observation) catch return error.MetadataMutationOutcomeUnknown) orelse error.MetadataMutationOutcomeUnknown;
}
pub fn admitStandalone(store: anytype, a: A, group: u64, context: operation.RequestContext, request: contract.Mutation) ![]u8 {
    try requirePrivate(context, request);
    const observation = try contract.observation(a, request);
    try observation.validate(a);
    // The existing standalone catalog lane and standby mutation barrier are
    // held by the caller; the native receiver repeats readiness atomically.
    if (try prior(a, store, group, observation)) |result| return result;
    const preview = try store.sqlGraphPreparationRead(a, group, request.source, context);
    a.free(preview);
    const encoded = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(encoded);
    if (encoded.len > contract.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    store.applyStandaloneCommand(group, .{ .apply_sql_graph_preparation = encoded }) catch return error.MetadataMutationOutcomeUnknown;
    return (prior(a, store, group, observation) catch return error.MetadataMutationOutcomeUnknown) orelse error.MetadataMutationOutcomeUnknown;
}

test "graph preparation service observes retained decisions before new readiness and never replays an ambiguous proposal" {
    const Spy = struct {
        metadata_group_id: u64 = 1,
        ready: bool = true,
        drift: bool = false,
        fail_second_read: bool = false,
        lose_wait: bool = false,
        committed: bool = false,
        locked: bool = false,
        reads: usize = 0,
        proposals: usize = 0,
        pub fn projectedStore(self: *@This()) ?*@This() {
            return self;
        }
        pub fn ensureLinearizableReadWithContext(_: *@This(), context: operation.RequestContext) !void {
            try context.ensureActive();
        }
        pub fn ensureTableTopologyProtocolReadyWithContext(self: *@This(), _: operation.RequestContext, version: u16) !bool {
            try std.testing.expectEqual(protocol.sql_table_triggers_version, version);
            if (!self.ready) return error.TableTopologyProtocolUpgradeRequired;
            return true;
        }
        pub fn validateTableTopologyProtocolReadinessWithContext(self: *@This(), _: operation.RequestContext, _: bool) !void {
            try std.testing.expect(self.locked);
            if (self.drift) return error.TableTopologyProtocolUpgradeRequired;
        }
        pub fn lockCatalogMutation(self: *@This()) void {
            std.debug.assert(!self.locked);
            self.locked = true;
        }
        pub fn unlockCatalogMutation(self: *@This()) void {
            std.debug.assert(self.locked);
            self.locked = false;
        }
        pub fn sqlGraphPreparationRead(self: *@This(), a: A, group: u64, request: contract.Source, context: operation.RequestContext) ![]u8 {
            try std.testing.expectEqual(self.metadata_group_id, group);
            try context.ensureActive();
            try request.validate();
            self.reads += 1;
            if (self.fail_second_read and self.reads > 1) return error.CatalogGenerationChanged;
            return a.dupe(u8, "{}");
        }
        pub fn sqlGraphPreparationOutcome(self: *@This(), a: A, group: u64, request: contract.Observation) ![]u8 {
            try std.testing.expectEqual(self.metadata_group_id, group);
            try request.validate(a);
            if (!self.committed) return a.dupe(u8, "null");
            return std.json.Stringify.valueAlloc(a, contract.outcomes.Outcome{ .success = .{ .revision = 3, .identity = null, .removed = 0 } }, .{});
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), request: anytype) !u64 {
            try std.testing.expect(self.locked);
            try std.testing.expect(request == .apply_sql_graph_preparation);
            var parsed = try std.json.parseFromSlice(contract.Mutation, std.testing.allocator, request.apply_sql_graph_preparation, .{});
            defer parsed.deinit();
            try parsed.value.validate();
            self.proposals += 1;
            self.committed = true;
            return 3;
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: operation.RequestContext) !void {
            try std.testing.expect(!self.locked);
            if (self.lose_wait) return error.Timeout;
        }
    };
    const a = std.testing.allocator;
    const request: contract.Mutation = .{ .source = .{ .table_id = 7, .epoch = .{ .incarnation = @splat(1), .revision = 2 }, .schema_digest = @splat(2), .indexes = &.{.{ .name = "g", .digest = @splat(3) }} }, .admission = .{ .nonce = @splat(4), .head = 0 } };
    const context: operation.RequestContext = .{ .setting_admin = true };
    var denied: Spy = .{};
    try std.testing.expectError(error.Forbidden, admit(&denied, a, .{}, request));
    try std.testing.expectEqual(@as(usize, 0), denied.reads);
    var old: Spy = .{ .ready = false };
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, admit(&old, a, context, request));
    try std.testing.expectEqual(@as(usize, 0), old.proposals);
    var changed: Spy = .{ .fail_second_read = true };
    try std.testing.expectError(error.CatalogGenerationChanged, admit(&changed, a, context, request));
    try std.testing.expectEqual(@as(usize, 0), changed.proposals);
    try std.testing.expect(!changed.locked);
    var drifted: Spy = .{ .drift = true };
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, admit(&drifted, a, context, request));
    try std.testing.expectEqual(@as(usize, 0), drifted.proposals);
    // Match the authenticated HTTP receiver's local tracking context. The
    // first proposal marks submission; retained observation does not replay.
    var submitted = false;
    var transport_context = context;
    transport_context.sql_completion_submission = &submitted;
    var tracked: Spy = .{};
    const accepted = try admit(&tracked, a, transport_context, request);
    defer a.free(accepted);
    try std.testing.expect(submitted);
    try std.testing.expectEqual(@as(usize, 1), tracked.proposals);
    submitted = false;
    const retained = try admit(&tracked, a, transport_context, request);
    defer a.free(retained);
    try std.testing.expect(!submitted);
    try std.testing.expectEqual(@as(usize, 1), tracked.proposals);
    var unready_submission = false;
    transport_context.sql_completion_submission = &unready_submission;
    var unready: Spy = .{ .ready = false };
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, admit(&unready, a, transport_context, request));
    try std.testing.expect(!unready_submission);
    try std.testing.expectEqual(@as(usize, 0), unready.proposals);

    var lost: Spy = .{ .lose_wait = true };
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, admit(&lost, a, context, request));
    try std.testing.expectEqual(@as(usize, 1), lost.proposals);
    const reads = lost.reads;
    lost.ready = false;
    lost.fail_second_read = true;
    const observed = try admit(&lost, a, context, request);
    defer a.free(observed);
    var outcome = try std.json.parseFromSlice(contract.outcomes.Outcome, a, observed, .{});
    defer outcome.deinit();
    try std.testing.expect(outcome.value == .success);
    try std.testing.expectEqual(reads, lost.reads);
    try std.testing.expectEqual(@as(usize, 1), lost.proposals);
}
