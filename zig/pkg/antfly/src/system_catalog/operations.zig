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

//! Transport-independent system catalog admission, capability fencing, and
//! exact receipt completion. Logical and physical table identities are separate.
const std = @import("std");
const domain = @import("antfly_local_sources").system_catalog_domain;
const storage = @import("../metadata/storage/raft_apply_store.zig");
const protocol = @import("../metadata/topology_protocol.zig");
const operation = @import("antfly_local_sources").api_operation;
const tables_api = @import("../api/tables.zig");
const indexes_api = @import("../api/indexes.zig");
const managed_embedder = @import("antfly_local_sources").inference_managed_embedder;
const table_manager = @import("../metadata/table_manager.zig");
const settings = @import("antfly_local_sources").system_catalog_settings;

pub const Request = domain.Request;

/// One ordinary metadata proposal. The read-only preview is never returned
/// as the allocated identity; only its transactionally stored outcome is.
pub fn mutateSqlSequence(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("sequence_catalog.zig").Mutation) ![]u8 {
    if (!context.setting_admin) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    try svc.ensureSqlSequenceDecoderReadyWithContext(context);
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.sql_sequence_catalog_version);
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const preview = try store.sqlSequencePreflight(alloc, svc.metadata_group_id, request);
    if (preview == .rejected) return std.json.Stringify.valueAlloc(alloc, preview, .{});
    const bytes = try std.json.Stringify.valueAlloc(alloc, @import("sequence_catalog.zig").command(request), .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    // Object guards and the bounded admission window are checked again by
    // native apply. Network waits, preparation and encoding need no catalog
    // serialization; hold its gate only for capability validation/proposal.
    svc.lockCatalogMutation();
    var locked = true;
    defer if (locked) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_system_catalog = bytes });
    svc.unlockCatalogMutation();
    locked = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const outcome = store.sqlSequenceOutcome(alloc, svc.metadata_group_id, request) catch return error.MetadataMutationOutcomeUnknown;
    return std.json.Stringify.valueAlloc(alloc, outcome, .{}) catch error.MetadataMutationOutcomeUnknown;
}

pub fn mutateSqlSequenceCounter(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("sequence_catalog.zig").CounterRequest) ![]u8 {
    const sequence = @import("sequence_catalog.zig");
    if (!context.setting_admin) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    try svc.ensureSqlSequenceDecoderReadyWithContext(context);
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.sql_sequence_catalog_version);
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    switch (try store.sqlSequenceCounterPreflight(alloc, svc.metadata_group_id, request)) {
        .unknown => return error.MetadataMutationOutcomeUnknown,
        .applied => |value| return std.json.Stringify.valueAlloc(alloc, sequence.CounterOutcome{ .success = value }, .{}) catch error.MetadataMutationOutcomeUnknown,
        .rejected => |value| return std.json.Stringify.valueAlloc(alloc, sequence.CounterOutcome{ .rejected = value }, .{}),
        .ready => {},
    }
    const bytes = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    svc.lockCatalogMutation();
    var locked = true;
    defer if (locked) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_sql_sequence_counter = bytes });
    svc.unlockCatalogMutation();
    locked = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const outcome = (store.sqlSequenceCounterOutcome(alloc, svc.metadata_group_id, request) catch return error.MetadataMutationOutcomeUnknown) orelse return error.MetadataMutationOutcomeUnknown;
    return std.json.Stringify.valueAlloc(alloc, outcome, .{}) catch error.MetadataMutationOutcomeUnknown;
}

test "sequence catalog counter service never issues previews or replays ambiguous proposals" {
    const t = std.testing;
    const sequence = @import("sequence_catalog.zig");
    const initial = try sequence.sequence.Record.create(.{ .cache = 4 });
    const request: sequence.CounterRequest = .{ .target = .{ .table = "counter" }, .namespace_id = 2, .sequence_id = 41, .incarnation = @splat(7), .request = .{ .expected = initial.state, .request_id = @splat(1), .operation = .reserve } };
    const actual = (try sequence.journal.prepare(.{ .group = 41, .sequence_id = 41 }, initial.definition, initial.state, request.request, null)).apply;
    const Store = struct {
        admission: sequence.CounterAdmission = .ready,
        observed: ?sequence.CounterOutcome = null,
        reads: usize = 0,
        read_failure: bool = false,
        pub fn sqlSequenceCounterPreflight(self: *@This(), _: std.mem.Allocator, _: u64, _: sequence.CounterRequest) !sequence.CounterAdmission {
            return self.admission;
        }
        pub fn sqlSequenceCounterOutcome(self: *@This(), _: std.mem.Allocator, _: u64, _: sequence.CounterRequest) !?sequence.CounterOutcome {
            self.reads += 1;
            if (self.read_failure) return error.OutOfMemory;
            return self.observed;
        }
    };
    const Svc = struct {
        const Receipt = struct { term: u64, index: u64 };
        metadata_group_id: u64 = 41,
        store: Store = .{},
        locked: bool = false,
        proposals: usize = 0,
        waits: usize = 0,
        wait_failure: bool = false,
        cancel_on_validate: ?*std.atomic.Value(bool) = null,
        fail_reply: ?*t.FailingAllocator = null,
        pub fn ensureSqlSequenceDecoderReadyWithContext(_: *@This(), _: operation.RequestContext) !void {}
        pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: operation.RequestContext, version: u16) !bool {
            try t.expectEqual(protocol.sql_sequence_catalog_version, version);
            return true;
        }
        pub fn lockCatalogMutation(self: *@This()) void {
            std.debug.assert(!self.locked);
            self.locked = true;
        }
        pub fn unlockCatalogMutation(self: *@This()) void {
            std.debug.assert(self.locked);
            self.locked = false;
        }
        pub fn ensureLinearizableReadWithContext(self: *@This(), _: operation.RequestContext) !void {
            try t.expect(!self.locked);
        }
        pub fn validateTableTopologyProtocolReadinessWithContext(self: *@This(), _: operation.RequestContext, ready: bool) !void {
            try t.expect(ready and self.locked);
            if (self.cancel_on_validate) |signal| signal.store(true, .release);
        }
        pub fn projectedStore(self: *@This()) ?*Store {
            std.debug.assert(!self.locked);
            return &self.store;
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: storage.TransitionCommand) !Receipt {
            try t.expect(self.locked and command == .apply_sql_sequence_counter);
            var parsed = try std.json.parseFromSlice(sequence.CounterRequest, t.allocator, command.apply_sql_sequence_counter, .{});
            defer parsed.deinit();
            try parsed.value.validate();
            self.proposals += 1;
            if (self.fail_reply) |failing| failing.fail_index = failing.alloc_index;
            return .{ .term = 3, .index = 99 };
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), _: Receipt, _: operation.RequestContext) !void {
            try t.expect(!self.locked);
            self.waits += 1;
            if (self.wait_failure) return error.DeadlineExceeded;
        }
    };
    const context: operation.RequestContext = .{ .setting_admin = true };
    var svc: Svc = .{};
    try t.expectError(error.Forbidden, mutateSqlSequenceCounter(&svc, t.allocator, .{}, request));
    try t.expectError(error.DeadlineExceeded, mutateSqlSequenceCounter(&svc, t.allocator, .{ .setting_admin = true, .deadline_ns = 0 }, request));
    try t.expectEqual(@as(usize, 0), svc.proposals);
    for ([_]sequence.CounterAdmission{ .{ .applied = actual }, .{ .rejected = .SequenceCounterChanged }, .unknown }) |admission| {
        svc = .{ .store = .{ .admission = admission } };
        if (admission == .unknown) {
            try t.expectError(error.MetadataMutationOutcomeUnknown, mutateSqlSequenceCounter(&svc, t.allocator, context, request));
        } else {
            const bytes = try mutateSqlSequenceCounter(&svc, t.allocator, context, request);
            defer t.allocator.free(bytes);
            var parsed = try std.json.parseFromSlice(sequence.CounterOutcome, t.allocator, bytes, .{});
            defer parsed.deinit();
            if (admission == .applied) try t.expectEqualDeep(actual, try parsed.value.result()) else try t.expectError(error.SequenceCounterChanged, parsed.value.result());
        }
        try t.expectEqual(@as(usize, 0), svc.proposals);
        try t.expectEqual(@as(usize, 0), svc.store.reads);
    }
    {
        var signal = std.atomic.Value(bool).init(false);
        var canceled = context;
        canceled.cancellation = operation.CancellationToken.fromAtomic(&signal);
        svc = .{ .cancel_on_validate = &signal };
        try t.expectError(error.Canceled, mutateSqlSequenceCounter(&svc, t.allocator, canceled, request));
        try t.expectEqual(@as(usize, 0), svc.proposals);
        try t.expect(!svc.locked);
    }
    svc = .{ .store = .{ .observed = .{ .success = actual } } };
    {
        const bytes = try mutateSqlSequenceCounter(&svc, t.allocator, context, request);
        defer t.allocator.free(bytes);
        var parsed = try std.json.parseFromSlice(sequence.CounterOutcome, t.allocator, bytes, .{});
        defer parsed.deinit();
        try t.expectEqualDeep(actual, try parsed.value.result());
        try t.expectEqual(@as(usize, 1), svc.proposals);
    }
    for (0..3) |mode| {
        svc = .{ .wait_failure = mode == 0, .store = .{ .read_failure = mode == 1 } };
        try t.expectError(error.MetadataMutationOutcomeUnknown, mutateSqlSequenceCounter(&svc, t.allocator, context, request));
        try t.expectEqual(@as(usize, 1), svc.proposals);
        try t.expectEqual(@as(usize, 1), svc.waits);
        try t.expect(!svc.locked);
    }
    var failing = t.FailingAllocator.init(t.allocator, .{ .fail_index = std.math.maxInt(usize) });
    svc = .{ .store = .{ .observed = .{ .success = actual } }, .fail_reply = &failing };
    try t.expectError(error.MetadataMutationOutcomeUnknown, mutateSqlSequenceCounter(&svc, failing.allocator(), context, request));
    try t.expectEqual(@as(usize, 1), svc.proposals);
}

test "sequence catalog service uses actual outcomes and preserves proposal ambiguity without retries" {
    const t = std.testing;
    const sequence = @import("sequence_catalog.zig");
    const Store = struct {
        preview: sequence.Outcome = .{ .success = .{ .revision = 1, .identity = .{ .id = 41, .generation = 1 }, .removed = 0 } },
        observed: sequence.Outcome = .{ .success = .{ .revision = 2, .identity = .{ .id = 42, .generation = 1 }, .removed = 0 } },
        reads: usize = 0,
        missing: bool = false,
        pub fn sqlSequencePreflight(self: *@This(), _: std.mem.Allocator, _: u64, _: sequence.Mutation) !sequence.Outcome {
            return self.preview;
        }
        pub fn sqlSequenceOutcome(self: *@This(), _: std.mem.Allocator, _: u64, _: sequence.Mutation) !sequence.Outcome {
            self.reads += 1;
            if (self.missing) return error.MetadataMutationOutcomeUnknown;
            return self.observed;
        }
    };
    const Svc = struct {
        const Receipt = struct { term: u64, index: u64 };
        metadata_group_id: u64 = 41,
        store: Store = .{},
        locked: bool = false,
        proposals: usize = 0,
        waits: usize = 0,
        wait_failure: bool = false,
        cancel_on_validate: ?*std.atomic.Value(bool) = null,
        fail_reply: ?*t.FailingAllocator = null,
        pub fn ensureSqlSequenceDecoderReadyWithContext(_: *@This(), _: operation.RequestContext) !void {}
        pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: operation.RequestContext, version: u16) !bool {
            try t.expectEqual(protocol.sql_sequence_catalog_version, version);
            return true;
        }
        pub fn lockCatalogMutation(self: *@This()) void {
            std.debug.assert(!self.locked);
            self.locked = true;
        }
        pub fn unlockCatalogMutation(self: *@This()) void {
            std.debug.assert(self.locked);
            self.locked = false;
        }
        pub fn ensureLinearizableReadWithContext(self: *@This(), _: operation.RequestContext) !void {
            try t.expect(!self.locked);
        }
        pub fn validateTableTopologyProtocolReadinessWithContext(self: *@This(), _: operation.RequestContext, ready: bool) !void {
            try t.expect(ready and self.locked);
            if (self.cancel_on_validate) |signal| signal.store(true, .release);
        }
        pub fn projectedStore(self: *@This()) ?*Store {
            std.debug.assert(!self.locked);
            return &self.store;
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: storage.TransitionCommand) !Receipt {
            try t.expect(self.locked and command == .apply_system_catalog);
            var parsed = try std.json.parseFromSlice(storage.SystemCatalogCommand, t.allocator, command.apply_system_catalog, .{});
            defer parsed.deinit();
            try t.expect(parsed.value.sql_sequence != null);
            self.proposals += 1;
            if (self.fail_reply) |failing| failing.fail_index = failing.alloc_index;
            return .{ .term = 3, .index = 99 };
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), receipt: Receipt, _: operation.RequestContext) !void {
            try t.expect(!self.locked);
            try t.expectEqualDeep(Receipt{ .term = 3, .index = 99 }, receipt);
            self.waits += 1;
            if (self.wait_failure) return error.DeadlineExceeded;
        }
    };
    const request: sequence.Mutation = .{ .admission = .{ .nonce = @splat(1), .head = 0 }, .target = .{ .table = "counter" }, .namespace_id = 2, .incarnation = @splat(7), .operation = .{ .create = .{ .options = .{} } } };
    const context: operation.RequestContext = .{ .setting_admin = true };
    var svc: Svc = .{};
    try t.expectError(error.Forbidden, mutateSqlSequence(&svc, t.allocator, .{}, request));
    try t.expectError(error.DeadlineExceeded, mutateSqlSequence(&svc, t.allocator, .{ .setting_admin = true, .deadline_ns = 0 }, request));
    try t.expectEqual(@as(usize, 0), svc.proposals);
    {
        var signal = std.atomic.Value(bool).init(false);
        var canceled = context;
        canceled.cancellation = operation.CancellationToken.fromAtomic(&signal);
        svc = .{ .cancel_on_validate = &signal };
        try t.expectError(error.Canceled, mutateSqlSequence(&svc, t.allocator, canceled, request));
        try t.expectEqual(@as(usize, 0), svc.proposals);
        try t.expect(!svc.locked);
        svc = .{};
    }
    {
        const bytes = try mutateSqlSequence(&svc, t.allocator, context, request);
        defer t.allocator.free(bytes);
        var parsed = try std.json.parseFromSlice(sequence.Outcome, t.allocator, bytes, .{});
        defer parsed.deinit();
        try t.expectEqual(@as(u64, 42), (try parsed.value.result()).identity.?.id);
        try t.expectEqual(@as(usize, 1), svc.proposals);
    }
    svc = .{ .store = .{ .preview = .{ .rejected = .InvalidSequenceDefinition } } };
    {
        const bytes = try mutateSqlSequence(&svc, t.allocator, context, request);
        defer t.allocator.free(bytes);
        var parsed = try std.json.parseFromSlice(sequence.Outcome, t.allocator, bytes, .{});
        defer parsed.deinit();
        try t.expectError(error.InvalidSequenceDefinition, parsed.value.result());
        try t.expectEqual(@as(usize, 0), svc.proposals);
    }
    svc = .{ .store = .{ .observed = .{ .rejected = .CatalogGenerationChanged } } };
    {
        const bytes = try mutateSqlSequence(&svc, t.allocator, context, request);
        defer t.allocator.free(bytes);
        var parsed = try std.json.parseFromSlice(sequence.Outcome, t.allocator, bytes, .{});
        defer parsed.deinit();
        try t.expectError(error.CatalogGenerationChanged, parsed.value.result());
        try t.expectEqual(@as(usize, 1), svc.proposals);
    }
    svc = .{ .wait_failure = true };
    try t.expectError(error.MetadataMutationOutcomeUnknown, mutateSqlSequence(&svc, t.allocator, context, request));
    try t.expectEqual(@as(usize, 1), svc.proposals);
    try t.expectEqual(@as(usize, 0), svc.store.reads);
    svc = .{ .store = .{ .missing = true } };
    try t.expectError(error.MetadataMutationOutcomeUnknown, mutateSqlSequence(&svc, t.allocator, context, request));
    try t.expectEqual(@as(usize, 1), svc.proposals);
    var failing = t.FailingAllocator.init(t.allocator, .{ .fail_index = std.math.maxInt(usize) });
    svc = .{ .fail_reply = &failing };
    try t.expectError(error.MetadataMutationOutcomeUnknown, mutateSqlSequence(&svc, failing.allocator(), context, request));
    try t.expectEqual(@as(usize, 1), svc.proposals);
    try t.expect(!svc.locked);
}

pub fn mutateSqlView(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("views.zig").Request) ![]u8 {
    if (!context.setting_admin) return error.Forbidden;
    try context.ensureActive();
    try request.mutation.validate();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.sql_view_catalog_version);
    svc.lockCatalogMutation();
    var locked = true;
    defer if (locked) svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const command = @import("views.zig").command(request);
    const result = try store.prepareSystemCatalogResult(alloc, svc.metadata_group_id, command);
    errdefer alloc.free(result);
    var parsed = try std.json.parseFromSlice(@import("views.zig").MutationResult, alloc, result, .{});
    defer parsed.deinit();
    if (parsed.value.revision == request.expected_revision) return result;
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_system_catalog = bytes });
    svc.unlockCatalogMutation();
    locked = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const meta = store.systemCatalogMeta(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    if (meta.revision != parsed.value.revision or !std.mem.eql(u8, &hash, &meta.last_command)) return error.MetadataMutationOutcomeUnknown;
    return result;
}

pub fn storeRootMutation(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, input: @import("server_call.zig").Call) ![]u8 {
    const enroll = input == .store_root_enroll;
    if (enroll and !context.setting_admin) return error.Forbidden;
    if (!enroll and input != .fk_initial_retirement_ack) return error.InvalidArgument;
    try context.ensureActive();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.store_root_enrollment_version);
    try svc.ensureLinearizableReadWithContext(context);
    svc.lockCatalogMutation();
    var gate_held = true;
    defer if (gate_held) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const wire = @import("../metadata/fk_initial_retirement_wire.zig");
    const preflight: wire.Control = if (enroll) .{ .preflight_enrollment = input.store_root_enroll } else .{ .preflight_ack = input.fk_initial_retirement_ack };
    const checked = try store.storeRootControlJson(alloc, svc.metadata_group_id, preflight);
    alloc.free(checked);
    const bytes = if (enroll) try std.json.Stringify.valueAlloc(alloc, input.store_root_enroll, .{}) else try std.json.Stringify.valueAlloc(alloc, input.fk_initial_retirement_ack, .{});
    defer alloc.free(bytes);
    if (bytes.len > 8192) return error.CatalogCommandTooLarge;
    const command: storage.TransitionCommand = if (enroll) .{ .apply_store_root_enrollment = bytes } else .{ .ack_initial_fk_retirement = bytes };
    const receipt = try svc.proposeTransitionCommandWithReceipt(command);
    svc.unlockCatalogMutation();
    gate_held = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed: wire.Control = if (enroll) .{ .enrollment_status = input.store_root_enroll.identity } else .{ .ack_status = input.fk_initial_retirement_ack };
    return store.storeRootControlJson(alloc, svc.metadata_group_id, observed) catch return error.MetadataMutationOutcomeUnknown;
}

pub fn storeRootPage(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/fk_initial_retirement_wire.zig").SignedPageRequest) ![]u8 {
    try request.verify();
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.storeRootControlJson(alloc, svc.metadata_group_id, .{ .authorized_page = request });
}

pub fn storeRootEnrollmentStatus(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, identity: @import("../metadata/store_root_enrollment.zig").Identity) ![]u8 {
    if (!context.setting_admin) return error.Forbidden;
    try identity.validate();
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.storeRootControlJson(alloc, svc.metadata_group_id, .{ .enrollment_status = identity });
}

// Own the validated ingress representation across the catalog barrier. JSON
// request parsing and storage validation stay outside the serialized lane;
// name allocation, schema-derived index expansion and placement still use its
// admitted cut. Reparsing also needlessly mints new private index identities.
fn prepareCreateRequest(alloc: std.mem.Allocator, request: Request) !?tables_api.CreateTableRequest {
    const creating_table = request.mutation.kind == .table and request.mutation.action == .create;
    if (creating_table != (request.create_table_json != null)) return error.InvalidCatalogMutation;
    const json = request.create_table_json orelse return null;
    var req = try tables_api.parseStoredCreateTableRequest(alloc, json);
    errdefer req.deinit(alloc);
    const settings_value = req.storage orelse @import("antfly_local_sources").common_table_storage.Settings{};
    try settings_value.validateCreate(req.num_shards, if (req.replication_sources_json) |sources| !std.mem.eql(u8, sources, "[]") else false);
    try tables_api.validateObjectCreateDefinition(alloc, req);
    return req;
}

test "system catalog table create preparation owns one validated request across publication" {
    const alloc = std.testing.allocator;
    var req = blk: {
        const input = try alloc.dupe(u8, "{\"num_shards\":2,\"description\":\"owned create\",\"schema\":{\"version\":0}}");
        defer alloc.free(input);
        const prepared = (try prepareCreateRequest(alloc, .{ .mutation = .{ .kind = .table, .action = .create, .name = "items" }, .create_table_json = input })).?;
        @memset(input, ' ');
        break :blk prepared;
    };
    defer req.deinit(alloc);
    try std.testing.expectEqualStrings("owned create", req.description.?);
    try std.testing.expectEqual(@as(u32, 2), req.num_shards.?);
    try std.testing.expect(req.indexes_json.?.len != 0);
    var schema = try std.json.parseFromSlice(std.json.Value, alloc, req.schema_json.?, .{});
    defer schema.deinit();
    try std.testing.expectEqual(@as(i64, 0), schema.value.object.get("version").?.integer);
    try std.testing.expect((try prepareCreateRequest(alloc, .{ .mutation = .{ .kind = .database, .action = .create, .name = "db" } })) == null);
    try std.testing.expectError(error.InvalidCatalogMutation, prepareCreateRequest(alloc, .{ .mutation = .{ .kind = .table, .action = .create, .name = "items" } }));
    try std.testing.expectError(error.InvalidCatalogMutation, prepareCreateRequest(alloc, .{ .mutation = .{ .kind = .table, .action = .drop, .name = "items" }, .create_table_json = "{}" }));
    try std.testing.expectError(error.InvalidCreateTableRequest, prepareCreateRequest(alloc, .{ .mutation = .{ .kind = .table, .action = .create, .name = "items" }, .create_table_json = "{\"num_shards\":0}" }));
}

pub fn mutate(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: Request) ![]u8 {
    if (request.mutation.action == .comment and !context.setting_admin) return error.Forbidden;
    try context.ensureActive();
    if (request.mutation.native_service != null and request.mutation.native_service != context.native_service) return error.Forbidden;
    if (request.mutation.table_id != 0 or request.mutation.storage_name.len != 0) return error.InvalidCatalogMutation;
    if (request.generated_constraint_names.len != 0 and (request.mutation.kind != .table or request.mutation.action != .create)) return error.InvalidCatalogMutation;
    if (request.column_sequences.len > 256 or (request.column_sequences.len != 0 and (request.mutation.kind != .table or request.mutation.action != .create))) return error.InvalidCatalogMutation;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const create_req = try prepareCreateRequest(a, request);
    const object_create = if (create_req) |req| (req.storage orelse @import("antfly_local_sources").common_table_storage.Settings{}).engine == .object else false;
    const ordinary_version = if (request.mutation.action == .comment) protocol.sql_resource_comments_version else if (object_create) protocol.object_table_engine_version else if (request.mutation.kind == .table and request.mutation.action == .drop) protocol.system_catalog_drop_version else protocol.system_catalog_version;
    var resolver_create = false;
    if (create_req) |req| {
        resolver_create = protocol.usesResolverNamespaces(req.indexes_json orelse tables_api.default_indexes_json);
        try @import("../metadata/table_workflow.zig").preflightTableRecordWithContext(svc, context, .{
            .table_id = 0,
            .name = request.physical_name orelse "",
            .indexes_json = req.indexes_json orelse tables_api.default_indexes_json,
        });
    }
    const required_version = @max(if (request.mutation.native_service != null) protocol.native_service_tables_version else @as(u16, 0), @max(if (resolver_create) protocol.resolver_namespaces_version else @as(u16, 0), if (request.generated_constraint_names.len != 0) @max(ordinary_version, protocol.constraint_occupancy_version) else ordinary_version));
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, @max(required_version, if (request.column_sequences.len != 0) protocol.sql_column_sequences_version else 0));
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const admission = try store.systemCatalogAdmission(a, svc.metadata_group_id, request.mutation);
    var command: storage.SystemCatalogCommand = .{ .expected_revision = admission.meta.revision, .mutation = request.mutation, .column_sequences = request.column_sequences };
    if (request.mutation.action == .create and request.mutation.kind == .table) {
        var req = create_req orelse return error.InvalidCatalogMutation;
        if (request.generated_constraint_names.len != 0) {
            const bytes = try store.prepareConstraintNamesJsonWithContext(a, svc.metadata_group_id, .{ .create = .{ .expected_revision = admission.meta.revision, .namespace_id = admission.namespace_id, .logical_name = request.mutation.name, .schema_json = req.schema_json orelse return error.InvalidCatalogMutation, .generated_names = request.generated_constraint_names } }, context);
            const names = try std.json.parseFromSliceLeaky(domain.ConstraintNamePrepared, a, bytes, .{});
            req.schema_json = try a.dupe(u8, names.schema_json orelse return error.InvalidCatalogRecord);
            command.constraint_name_admission = names.admission;
            command.version = 2;
        }
        const storage_name = request.physical_name orelse try std.fmt.allocPrint(a, "table:{d}", .{admission.meta.next_id});
        if (!std.mem.startsWith(u8, storage_name, "table:") or storage_name.len > 1024) return error.InvalidCatalogMutation;
        req.indexes_json = try tables_api.expandSchemaDerivedAlgebraicIndexesAlloc(a, storage_name, req.indexes_json orelse tables_api.default_indexes_json, tables_api.effectiveSchemaJson(req.schema_json));
        try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(a, req.indexes_json.?);
        try managed_embedder.validateEmbeddingProducerOwnershipJson(a, req.indexes_json.?);
        var table = tables_api.deriveTableRecord(storage_name, req);
        if (!try store.resolverProjectionValid(a, svc.metadata_group_id, table)) return error.ResolverCurationConflict;
        if (try fk_publication.schemaHasForeignKeys(a, table.schema_json)) return error.ForeignKeyGenerationPublicationRequired;
        const policy = admission.placement_policy;
        try policy.validate();
        if (table.storage.engine == .local) {
            if (policy.placement_role) |role| table.placement_role = role;
            if (policy.desired_replica_count) |count| table.desired_replica_count = count;
            if (req.num_shards == null) if (policy.min_ranges) |count| {
                table.min_ranges = count;
            };
        }
        const generation = try svc.captureTableCreateGeneration(a, table.table_id);
        if (table.storage.engine == .object) table.object_storage_generation = generation;
        const ranges = try tables_api.deriveInitialRangesForGeneration(a, table, generation);
        command.mutation.table_id = table.table_id;
        command.mutation.storage_name = storage_name;
        command.topology = .{ .create = .{ .expected_transition_generation = generation, .table = table, .ranges = ranges } };
        if (command.column_sequences.len != 0) {
            command.version = 3;
            const bound = try store.bindTableColumnSequences(a, svc.metadata_group_id, command);
            command = try std.json.parseFromSliceLeaky(storage.SystemCatalogCommand, a, bound, .{});
        }
    } else if (request.create_table_json != null) return error.InvalidCatalogMutation;
    if (request.mutation.action == .drop and request.mutation.kind == .table) {
        const target: domain.Target = .{ .database = request.mutation.database, .namespace = request.mutation.namespace, .table = request.mutation.name };
        const table = (try store.resolveSystemCatalogTable(a, svc.metadata_group_id, target)) orelse return error.CatalogNotFound;
        const drop = try svc.captureTableDropAdmission(a, table.name);
        if (drop.table_id != table.table_id) return error.TableGenerationChanged;
        command.mutation.table_id = drop.table_id;
        command.mutation.storage_name = drop.expected_name;
        command.topology = .{ .drop = .{ .table_id = drop.table_id, .expected_name = drop.expected_name, .expected_transition_generation = drop.expected_transition_generation, .range_contract = .{ .membership = drop.range_membership } } };
    }
    if (request.mutation.action == .set_tablespace and request.mutation.kind == .table) {
        const target: domain.Target = .{ .database = request.mutation.database, .namespace = request.mutation.namespace, .table = request.mutation.name };
        const current = (try store.resolveSystemCatalogTable(a, svc.metadata_group_id, target)) orelse return error.TableNotFound;
        const policy = admission.placement_policy;
        var replacement = current;
        replacement.placement_role = policy.placement_role orelse "data";
        replacement.desired_replica_count = if (replacement.storage.engine == .object) 0 else policy.desired_replica_count orelse 3;
        replacement.min_ranges = if (replacement.storage.engine == .object) 0 else policy.min_ranges orelse 1;
        command.placement_update = .{ .expected = current, .replacement = replacement };
    }
    const result = try store.prepareSystemCatalogResult(alloc, svc.metadata_group_id, command);
    errdefer alloc.free(result);
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_system_catalog = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.systemCatalogMeta(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected_hash, .{});
    if (observed.revision != command.expected_revision + 1 or !std.mem.eql(u8, &observed.last_command, &expected_hash)) return error.MetadataMutationOutcomeUnknown;
    if (command.topology) |topology| switch (topology) {
        .create => |create| svc.verifyTableCreateProjection(a, create.table, create.ranges) catch return error.MetadataMutationOutcomeUnknown,
        .drop => |drop| svc.verifyTableDropProjection(a, drop.table_id) catch return error.MetadataMutationOutcomeUnknown,
    };
    return result;
}

pub fn snapshotJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext) ![]u8 {
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var snapshot = try store.systemCatalogSnapshot(alloc, svc.metadata_group_id);
    defer snapshot.deinit();
    return std.json.Stringify.valueAlloc(alloc, snapshot.value, .{});
}

pub fn settingSnapshotJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, scope: settings.Scope) ![]u8 {
    try context.ensureActive();
    if (scope.principal.len == 0 or scope.database.len == 0) return error.InvalidSettingRecord;
    // Native callers carry the authenticated principal. The metadata HTTP
    // transport drops that identity, but authenticates the internal service;
    // public API ingress must derive Scope, never accept it from SQL text.
    if (context.principal != null) {
        const admitted = context.setting_read_principal orelse return error.Forbidden;
        if (!std.mem.eql(u8, admitted, scope.principal)) return error.Forbidden;
    }
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.sqlSettingSnapshotJson(alloc, svc.metadata_group_id, scope);
}

pub fn policySnapshotJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("antfly_local_sources").system_catalog_policies.SnapshotRequest) ![]u8 {
    try context.ensureActive();
    if (request.table_id == 0 or request.principal.len == 0 or request.database.len == 0) return error.InvalidRowPolicyRecord;
    // This principal-independent catalog read cannot assert roles. The API
    // separately mints a short-lived authenticated role proof, which the
    // owner verifies against this immutable serving generation.
    if (request.roles.len != 0) return error.RowPolicyAuthenticationRequired;
    if (context.principal != null) {
        const admitted = context.setting_read_principal orelse return error.Forbidden;
        if (!std.mem.eql(u8, admitted, request.principal)) return error.Forbidden;
    }
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.sqlPolicySnapshotJson(alloc, svc.metadata_group_id, request.table_id, request.principal, request.database, request.roles);
}

pub fn policyInstallSnapshotJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("antfly_local_sources").system_catalog_policies.InstallRequest) ![]u8 {
    try context.ensureActive();
    if (!context.row_policy_install_authority or request.table_id == 0 or
        request.expected_generation == 0 or request.expected_catalog_epoch == 0)
        return error.Forbidden;
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.sqlPolicyInstallSnapshotJson(alloc, svc.metadata_group_id, request);
}

pub fn policyPublicationStatusJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, table_id: u64) ![]u8 {
    try context.ensureActive();
    if (!context.row_policy_install_authority or table_id == 0) return error.Forbidden;
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.sqlPolicyPublicationStatusJson(alloc, svc.metadata_group_id, table_id);
}

pub fn policyPublicationWorkJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, after_table_id: u64) ![]u8 {
    try context.ensureActive();
    if (!context.row_policy_install_authority) return error.Forbidden;
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.sqlPolicyPublicationWorkJson(alloc, svc.metadata_group_id, after_table_id);
}

pub fn beginPolicyPublication(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("antfly_local_sources").system_catalog_policies.BeginRequest) ![]u8 {
    if (!context.setting_admin or !context.row_policy_install_authority or request.table_id == 0) return error.Forbidden;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try store.sqlPolicyBeginCommandJson(alloc, svc.metadata_group_id, request);
    defer alloc.free(bytes);
    var command = try std.json.parseFromSlice(@import("antfly_local_sources").system_catalog_policies.PublicationCommand, alloc, bytes, .{});
    defer command.deinit();
    return mutatePolicyPublication(svc, alloc, context, command.value);
}

/// The reconciler supplies a precomputed owner cut and durable owner receipt;
/// metadata apply repeats the topology/ACK checks before committing. An
/// ambiguous proposal is reconciled by reading publication status, not by
/// blindly resubmitting a transition.
pub fn mutatePolicyPublication(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, command: @import("antfly_local_sources").system_catalog_policies.PublicationCommand) ![]u8 {
    if (!context.setting_admin or !context.row_policy_install_authority) return error.Forbidden;
    try context.ensureActive();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.sql_row_policy_intent_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const current = try store.systemCatalogMeta(alloc, svc.metadata_group_id);
    if (current.revision != command.expected_revision) return error.RowPolicyCatalogChanged;
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_sql_policy_publication = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.systemCatalogMeta(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected_hash, .{});
    if (observed.revision != command.expected_revision + 1 or !std.mem.eql(u8, &observed.last_command, &expected_hash))
        return error.MetadataMutationOutcomeUnknown;
    return std.json.Stringify.valueAlloc(alloc, observed, .{});
}

fn serializePolicyDefinitionCommand(alloc: std.mem.Allocator, command: @import("antfly_local_sources").system_catalog_policies.Command) ![]u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    defer output.deinit();
    var stream: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
    try @import("antfly_local_sources").storage_db_relational_integrity_json.write(command, &stream);
    return output.toOwnedSlice();
}

test "policy definition command serializes logical JSON literals without map pointers" {
    const alloc = std.testing.allocator;
    var literal = try std.json.parseFromSlice(std.json.Value, alloc, "{\"roles\":[\"reader\"],\"enabled\":true}", .{});
    defer literal.deinit();
    const command: @import("antfly_local_sources").system_catalog_policies.Command = .{
        .expected_revision = 1,
        .change = .{ .put = .{
            .id = 1,
            .generation = 1,
            .table_id = 2,
            .schema_version = 1,
            .schema_digest = @splat(1),
            .name = "p",
            .commands = .{ .select = true },
            .roles = &.{"reader"},
            .using = .{ .instructions = &.{.{ .type = .{ .kind = .boolean }, .operation = .{ .literal = literal.value } }}, .root = 0 },
        } },
    };
    const bytes = try serializePolicyDefinitionCommand(alloc, command);
    defer alloc.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "\"enabled\":true") != null);
    var decoded = try std.json.parseFromSlice(@import("antfly_local_sources").system_catalog_policies.Command, alloc, bytes, .{});
    defer decoded.deinit();
    try std.testing.expect(decoded.value.change.put.using.?.instructions[0].operation.literal.object.get("enabled").?.bool);
}

const fk_publication = @import("../metadata/fk_generation_publication.zig");

pub fn fkGenerationPublicationStatusJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, child_table_id: u64) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.fkGenerationPublicationStatusJson(alloc, svc.metadata_group_id, child_table_id);
}

pub fn fkGenerationPublicationWorkJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, after_child_table_id: u64) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.fkGenerationPublicationWorkJson(alloc, svc.metadata_group_id, after_child_table_id);
}

pub fn fkGenerationPublicationDecisionJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: fk_publication.DecisionRequest) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.fkGenerationPublicationDecisionJson(alloc, svc.metadata_group_id, request);
}

pub fn fkGenerationPublicationSourceDecisionJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: fk_publication.SourceDecisionRequest) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.fkGenerationPublicationSourceDecisionJson(alloc, svc.metadata_group_id, request);
}

pub fn relationalIndexStatusJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").StatusRequest) ![]u8 {
    if (!context.setting_admin or !context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.relationalIndexStatusJson(alloc, svc.metadata_group_id, request);
}

pub fn prepareRelationalIndexPublication(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").PrepareRequest) ![]u8 {
    if (!context.setting_admin or !context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.relationalIndexPrepareCutJson(alloc, svc.metadata_group_id, request);
}

pub fn beginRelationalIndexPublication(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").BeginRequest) ![]u8 {
    const publication = @import("../metadata/relational_index_publication.zig");
    if (!context.setting_admin or !context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    var prepared = try request.begin.plan.prepare(alloc);
    defer prepared.deinit();
    const observation = try request.observation(&prepared);
    const bytes = try std.json.Stringify.valueAlloc(alloc, request.begin, .{});
    defer alloc.free(bytes);
    if (bytes.len > publication.max_command_bytes) return error.CatalogCommandTooLarge;
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.index_publication_head_version);
    try svc.ensureLinearizableReadWithContext(context);
    svc.lockCatalogMutation();
    var gate_held = true;
    defer if (gate_held) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    try store.validateRelationalIndexBegin(svc.metadata_group_id, request, &prepared);
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .begin_relational_index_publication = bytes });
    svc.unlockCatalogMutation();
    gate_held = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.relationalIndexAdmissionStatusJson(alloc, svc.metadata_group_id, observation) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(observed);
    var parsed = std.json.parseFromSlice(publication.AdmissionStatus, alloc, observed, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer parsed.deinit();
    if (!(parsed.value.isAdmitted(observation) catch return error.MetadataMutationOutcomeUnknown)) return error.MetadataMutationOutcomeUnknown;
    return observed;
}

test "relational index publication admission releases its gate and recovers lost replies without replay" {
    const publication = @import("../metadata/relational_index_publication.zig");
    const Mock = struct {
        metadata_group_id: u64 = 13,
        locked: bool = false,
        proposals: usize = 0,
        lose_reply: bool = false,
        store: Store,
        const Store = struct {
            head: @import("antfly_local_sources").storage_db_relational_index_publication_progress.Head,
            admitted: bool = false,
            pending: bool = false,
            conflict: bool = false,
            pub fn validateRelationalIndexBegin(self: *@This(), _: u64, request: publication.BeginRequest, prepared: *const publication.Prepared) !void {
                _ = try request.observation(prepared);
                if (self.conflict) return error.CatalogAlreadyExists;
                if (self.admitted) return error.GenerationPublicationChanged;
            }
            pub fn relationalIndexAdmissionStatusJson(self: *@This(), a: std.mem.Allocator, _: u64, request: publication.AdmissionRequest) ![]u8 {
                return std.json.Stringify.valueAlloc(a, publication.AdmissionStatus{ .metadata_incarnation = request.metadata_incarnation, .head = if (self.admitted) self.head else null, .replication_pending = self.pending }, .{});
            }
        };
        pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: operation.RequestContext, version: u16) !u16 {
            return version;
        }
        pub fn ensureLinearizableReadWithContext(_: *@This(), _: operation.RequestContext) !void {}
        pub fn lockCatalogMutation(self: *@This()) void {
            std.debug.assert(!self.locked);
            self.locked = true;
        }
        pub fn unlockCatalogMutation(self: *@This()) void {
            std.debug.assert(self.locked);
            self.locked = false;
        }
        pub fn validateTableTopologyProtocolReadinessWithContext(self: *@This(), _: operation.RequestContext, _: u16) !void {
            try std.testing.expect(self.locked);
        }
        pub fn projectedStore(self: *@This()) ?*Store {
            return &self.store;
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: storage.TransitionCommand) !u64 {
            try std.testing.expect(self.locked);
            try std.testing.expect(command == .begin_relational_index_publication);
            self.proposals += 1;
            self.store.admitted = true;
            return 1;
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: operation.RequestContext) !void {
            try std.testing.expect(!self.locked);
            if (self.lose_reply) return error.Timeout;
        }
    };
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const plan = try publication.testing.plan(arena.allocator());
    var prepared = try plan.prepare(alloc);
    defer prepared.deinit();
    const request: publication.BeginRequest = .{ .metadata_incarnation = @splat('1'), .begin = .{ .plan = plan } };
    const observation = try request.observation(&prepared);
    const context: operation.RequestContext = .{ .setting_admin = true, .index_publication_authority = true };
    var svc: Mock = .{ .store = .{ .head = prepared.head } };
    try std.testing.expectError(error.Forbidden, beginRelationalIndexPublication(&svc, alloc, .{ .setting_admin = true }, request));
    try std.testing.expectError(error.Forbidden, beginRelationalIndexPublication(&svc, alloc, .{ .index_publication_authority = true }, request));
    try std.testing.expectEqual(@as(usize, 0), svc.proposals);
    const bytes = try beginRelationalIndexPublication(&svc, alloc, context, request);
    defer alloc.free(bytes);
    var status = try std.json.parseFromSlice(publication.AdmissionStatus, alloc, bytes, .{});
    defer status.deinit();
    try std.testing.expect(try status.value.isAdmitted(observation));
    try std.testing.expectEqual(@as(usize, 1), svc.proposals);
    try std.testing.expect(!svc.locked);
    svc = .{ .store = .{ .head = prepared.head, .conflict = true } };
    try std.testing.expectError(error.CatalogAlreadyExists, beginRelationalIndexPublication(&svc, alloc, context, request));
    try std.testing.expectEqual(@as(usize, 0), svc.proposals);
    try std.testing.expect(!svc.locked);
    svc = .{ .store = .{ .head = prepared.head }, .lose_reply = true };
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, beginRelationalIndexPublication(&svc, alloc, context, request));
    const recovered = try relationalIndexAdmissionStatusJson(&svc, alloc, context, observation);
    defer alloc.free(recovered);
    var recovered_status = try std.json.parseFromSlice(publication.AdmissionStatus, alloc, recovered, .{});
    defer recovered_status.deinit();
    try std.testing.expect(try recovered_status.value.isAdmitted(observation));
    try std.testing.expectEqual(@as(usize, 1), svc.proposals);
    svc = .{ .store = .{ .head = prepared.head, .pending = true } };
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, beginRelationalIndexPublication(&svc, alloc, context, request));
    try std.testing.expectEqual(@as(usize, 1), svc.proposals);
    try std.testing.expect(!svc.locked);
}

pub fn relationalIndexAdmissionStatusJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").AdmissionRequest) ![]u8 {
    if (!context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.relationalIndexAdmissionStatusJson(alloc, svc.metadata_group_id, request);
}

test "relational index publication admission status requires authority and a linearizable cut" {
    const publication = @import("../metadata/relational_index_publication.zig");
    const Mock = struct {
        metadata_group_id: u64 = 13,
        reads: usize = 0,
        store: Store = .{},
        const Store = struct {
            calls: usize = 0,
            pub fn relationalIndexAdmissionStatusJson(self: *@This(), alloc: std.mem.Allocator, group: u64, request: publication.AdmissionRequest) ![]u8 {
                try std.testing.expectEqual(@as(u64, 13), group);
                try request.validate();
                self.calls += 1;
                return alloc.dupe(u8, "{}");
            }
        };
        pub fn ensureLinearizableReadWithContext(self: *@This(), context: operation.RequestContext) !void {
            try context.ensureActive();
            self.reads += 1;
        }
        pub fn projectedStore(self: *@This()) ?*Store {
            return &self.store;
        }
    };
    var svc: Mock = .{};
    const request: publication.AdmissionRequest = .{ .table_id = 17, .id = @splat(1), .plan_digest = @splat(2), .metadata_incarnation = @splat(3) };
    try std.testing.expectError(error.Forbidden, relationalIndexAdmissionStatusJson(&svc, std.testing.allocator, .{}, request));
    var invalid = request;
    invalid.table_id = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, relationalIndexAdmissionStatusJson(&svc, std.testing.allocator, .{ .index_publication_authority = true }, invalid));
    try std.testing.expectEqual(@as(usize, 0), svc.reads);
    try std.testing.expectEqual(@as(usize, 0), svc.store.calls);
    const bytes = try relationalIndexAdmissionStatusJson(&svc, std.testing.allocator, .{ .index_publication_authority = true }, request);
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqual(@as(usize, 1), svc.reads);
    try std.testing.expectEqual(@as(usize, 1), svc.store.calls);
}

pub fn relationalIndexOwnerAckStatusJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").Ack) ![]u8 {
    if (!context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.relationalIndexOwnerAckStatusJson(alloc, svc.metadata_group_id, request);
}

pub fn relationalIndexPublicationDecisionStatusJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").DecidePublish) ![]u8 {
    if (!context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.relationalIndexPublicationDecisionStatusJson(alloc, svc.metadata_group_id, request);
}

pub fn decideRelationalIndexPublication(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").DecidePublish) ![]u8 {
    return decideRelationalIndex(svc, alloc, context, request, false);
}

pub fn cancelRelationalIndexPublication(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").CancelDecision) ![]u8 {
    return decideRelationalIndex(svc, alloc, context, request, true);
}

pub fn relationalIndexCancellationStatusJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").CancelDecision) ![]u8 {
    if (!context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.relationalIndexCancellationStatusJson(alloc, svc.metadata_group_id, request);
}

/// Both irreversible publication and pre-publication cancellation share the
/// same proposal/uncertainty boundary. Neither holds the gate while waiting.
fn decideRelationalIndex(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: anytype, comptime cancel: bool) ![]u8 {
    if (!context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.index_publication_decision_version);
    try svc.ensureLinearizableReadWithContext(context);
    svc.lockCatalogMutation();
    var gate_held = true;
    defer if (gate_held) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    if (comptime cancel) try store.validateRelationalIndexCancellation(svc.metadata_group_id, request) else try store.validateRelationalIndexPublicationDecision(svc.metadata_group_id, request);
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(if (comptime cancel) .{ .cancel_relational_index_publication = request } else .{ .decide_relational_index_publication = request });
    svc.unlockCatalogMutation();
    gate_held = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const publication = @import("../metadata/relational_index_publication.zig");
    const observed = (if (comptime cancel) store.relationalIndexCancellationStatusJson(alloc, svc.metadata_group_id, request) else store.relationalIndexPublicationDecisionStatusJson(alloc, svc.metadata_group_id, request)) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(observed);
    var parsed = std.json.parseFromSlice(if (cancel) publication.CancellationStatus else publication.DecisionStatus, alloc, observed, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer parsed.deinit();
    const confirmed = (if (comptime cancel) parsed.value.isCanceled(request) else parsed.value.isDecided(request)) catch return error.MetadataMutationOutcomeUnknown;
    if (!confirmed) return error.MetadataMutationOutcomeUnknown;
    return observed;
}

pub fn acknowledgeRelationalIndexOwner(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").Ack) ![]u8 {
    if (!context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.index_publication_head_version);
    try svc.ensureLinearizableReadWithContext(context);
    svc.lockCatalogMutation();
    var gate_held = true;
    defer if (gate_held) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(bytes);
    const publication = @import("../metadata/relational_index_publication.zig");
    if (bytes.len > publication.max_ack_bytes) return error.CatalogCommandTooLarge;
    try store.validateRelationalIndexOwnerAck(svc.metadata_group_id, bytes);
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .ack_relational_index_publication = bytes });
    svc.unlockCatalogMutation();
    gate_held = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.relationalIndexOwnerAckStatusJson(alloc, svc.metadata_group_id, request) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(observed);
    var parsed = std.json.parseFromSlice(publication.AckStatus, alloc, observed, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer parsed.deinit();
    if (!(parsed.value.isAcknowledged(request) catch return error.MetadataMutationOutcomeUnknown)) return error.MetadataMutationOutcomeUnknown;
    return observed;
}

/// Submit once, observe exactly, never turn a lost wait/read into nonadmission.
/// Callers retain the immutable command/receipt identity for status recovery.
pub fn submitSchemaValidation(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, command: []const u8) ![]u8 {
    if (!context.schema_validation_authority) return error.Forbidden;
    const ledger = @import("../metadata/schema_validation_ledger.zig");
    const receipts = @import("../metadata/schema_validation_receipt.zig");
    if (ledger.requiresAdmissionAuthority(command) and !context.setting_admin) return error.Forbidden;
    try context.ensureActive();
    if (command.len == 0 or command.len > ledger.max_command_bytes) return error.InvalidGenerationPublication;
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.schema_validation_publication_version);
    try svc.ensureLinearizableReadWithContext(context);
    svc.lockCatalogMutation();
    var gate_held = true;
    defer if (gate_held) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const preflight = try store.schemaValidationPreflightJson(alloc, svc.metadata_group_id, command);
    defer alloc.free(preflight);
    var expected = try std.json.parseFromSlice(receipts.Request, alloc, preflight, .{});
    defer expected.deinit();
    try expected.value.validate();
    if (expected.value.lookup.group != svc.metadata_group_id) return error.InvalidGenerationPublication;
    // Preflight is read-only. Repeated exact acceptance needs neither a new
    // proposal nor another durable receipt key, even after later phases.
    const prior = try store.schemaValidationReceiptJson(alloc, svc.metadata_group_id, expected.value);
    var prior_owned = true;
    defer if (prior_owned) alloc.free(prior);
    var observed = try std.json.parseFromSlice(receipts.Observation, alloc, prior, .{});
    defer observed.deinit();
    if (try observed.value.isAcknowledged(expected.value)) {
        prior_owned = false;
        return prior;
    }
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_schema_validation = command });
    svc.unlockCatalogMutation();
    gate_held = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const bytes = store.schemaValidationReceiptJson(alloc, svc.metadata_group_id, expected.value) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(bytes);
    var accepted = std.json.parseFromSlice(receipts.Observation, alloc, bytes, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer accepted.deinit();
    if (!(accepted.value.isAcknowledged(expected.value) catch return error.MetadataMutationOutcomeUnknown)) return error.MetadataMutationOutcomeUnknown;
    return bytes;
}

test "system catalog schema validation submissions preflight once observe exact acceptance and never replay uncertainty" {
    const receipts = @import("../metadata/schema_validation_receipt.zig");
    const ledger = @import("../metadata/schema_validation_ledger.zig");
    const barriers = @import("../metadata/schema_validation_publication.zig");
    const Fixture = struct {
        request: receipts.Request,
        initial: barriers.Head,
        terminal: barriers.Head,
        command: []const u8,
        readiness_error: ?anyerror = null,
        stale: bool = false,
        absent: bool = false,
        preflight_error: ?anyerror = null,
        proposal_error: ?anyerror = null,
        wait_error: bool = false,
        read_error: bool = false,
        malformed: bool = false,
        mismatched: bool = false,
        accepted: bool = false,
        locked: bool = false,
        readiness: usize = 0,
        barriers: usize = 0,
        preflights: usize = 0,
        reads: usize = 0,
        proposals: usize = 0,
        waits: usize = 0,
        metadata_group_id: u64 = 21,
        fn ensureTableTopologyProtocolReadyWithContext(self: *@This(), _: operation.RequestContext, version: u16) !u64 {
            try std.testing.expectEqual(protocol.schema_validation_publication_version, version);
            self.readiness += 1;
            if (self.readiness_error) |err| return err;
            return 42;
        }
        fn ensureLinearizableReadWithContext(self: *@This(), _: operation.RequestContext) !void {
            try std.testing.expect(!self.locked);
            self.barriers += 1;
        }
        fn lockCatalogMutation(self: *@This()) void {
            std.debug.assert(!self.locked);
            self.locked = true;
        }
        fn unlockCatalogMutation(self: *@This()) void {
            std.debug.assert(self.locked);
            self.locked = false;
        }
        fn validateTableTopologyProtocolReadinessWithContext(self: *@This(), _: operation.RequestContext, value: u64) !void {
            try std.testing.expect(self.locked);
            try std.testing.expectEqual(@as(u64, 42), value);
            if (self.stale) return error.GenerationPublicationChanged;
        }
        fn projectedStore(self: *@This()) ?*@This() {
            return if (self.absent) null else self;
        }
        fn schemaValidationPreflightJson(self: *@This(), alloc: std.mem.Allocator, group: u64, bytes: []const u8) ![]u8 {
            try std.testing.expect(self.locked);
            try std.testing.expectEqual(self.metadata_group_id, group);
            try std.testing.expectEqualSlices(u8, self.command, bytes);
            self.preflights += 1;
            if (self.preflight_error) |err| return err;
            return std.json.Stringify.valueAlloc(alloc, self.request, .{});
        }
        fn schemaValidationReceiptJson(self: *@This(), alloc: std.mem.Allocator, _: u64, request: receipts.Request) ![]u8 {
            try std.testing.expectEqualDeep(self.request, request);
            try std.testing.expectEqual(self.proposals == 0, self.locked);
            self.reads += 1;
            if (self.proposals != 0) {
                if (self.read_error) return error.MetadataReplicationPending;
                if (self.malformed) return alloc.dupe(u8, "{}");
            }
            var result: receipts.Observation = .{ .lookup = self.request.lookup, .head = if (self.accepted) self.terminal else self.initial, .observed = if (self.accepted) self.request.expected else null };
            if (self.mismatched and self.proposals != 0) result.lookup.plan_digest[0] ^= 1;
            return std.json.Stringify.valueAlloc(alloc, result, .{});
        }
        fn proposeTransitionCommandWithReceipt(self: *@This(), command: storage.TransitionCommand) !u64 {
            try std.testing.expect(self.locked);
            try std.testing.expect(command == .apply_schema_validation);
            try std.testing.expectEqualSlices(u8, self.command, command.apply_schema_validation);
            self.proposals += 1;
            if (self.proposal_error) |err| return err;
            self.accepted = true;
            return 9;
        }
        fn waitForTransitionAppliedWithContext(self: *@This(), receipt: u64, _: operation.RequestContext) !void {
            try std.testing.expect(!self.locked);
            try std.testing.expectEqual(@as(u64, 9), receipt);
            self.waits += 1;
            if (self.wait_error) return error.DeadlineExceeded;
        }
    };
    const a = std.testing.allocator;
    const head: barriers.Head = .{ .metadata_group_id = 21, .metadata_incarnation = @splat('1'), .id = @splat(1), .plan_digest = @splat(2), .table_id = 7, .owner_count = 2 };
    const terminal = try head.cancel(1, .requested);
    const lookup = ledger.Lookup.fromHead(head);
    const command = try std.json.Stringify.valueAlloc(a, ledger.Command{ .cancel = .{ .decision = .{ .lookup = lookup, .revision = 1 }, .failure = .requested } }, .{});
    defer a.free(command);
    const request: receipts.Request = .{ .lookup = lookup, .expected = .{ .cancellation = .{ .decision = .{ .revision = 1, .digest = try terminal.cancellationDecision() }, .failure = .requested } } };
    const initial: Fixture = .{ .request = request, .initial = head, .terminal = terminal, .command = command };
    const context: operation.RequestContext = .{ .schema_validation_authority = true };
    var svc = initial;
    for ([_]operation.RequestContext{ .{}, .{ .setting_admin = true }, .{ .index_publication_authority = true } }) |denied| {
        try std.testing.expectError(error.Forbidden, submitSchemaValidation(&svc, a, denied, command));
    }
    try std.testing.expectError(error.Forbidden, submitSchemaValidation(&svc, a, context, "SVBI\x01\x00\x00\x00"));
    try std.testing.expectEqual(@as(usize, 0), svc.readiness);
    var expired = context;
    expired.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, submitSchemaValidation(&svc, a, expired, command));
    svc.readiness_error = error.TableTopologyProtocolUpgradeRequired;
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, submitSchemaValidation(&svc, a, context, command));
    try std.testing.expectEqual(@as(usize, 0), svc.preflights);
    svc = initial;
    svc.stale = true;
    try std.testing.expectError(error.GenerationPublicationChanged, submitSchemaValidation(&svc, a, context, command));
    try std.testing.expect(!svc.locked);
    try std.testing.expectEqual(@as(usize, 0), svc.proposals);
    svc = initial;
    svc.preflight_error = error.StoreRootEnrollmentChanged;
    try std.testing.expectError(error.StoreRootEnrollmentChanged, submitSchemaValidation(&svc, a, context, command));
    try std.testing.expect(!svc.locked);
    try std.testing.expectEqual(@as(usize, 0), svc.proposals);
    for (0..7) |mode| {
        svc = initial;
        svc.wait_error = mode == 1;
        svc.read_error = mode == 2;
        svc.malformed = mode == 3;
        svc.mismatched = mode == 4;
        svc.accepted = mode == 5;
        svc.proposal_error = if (mode == 6) error.NotLeader else null;
        if (mode >= 1 and mode <= 4) {
            try std.testing.expectError(error.MetadataMutationOutcomeUnknown, submitSchemaValidation(&svc, a, context, command));
        } else if (mode == 6) {
            try std.testing.expectError(error.NotLeader, submitSchemaValidation(&svc, a, context, command));
        } else {
            const bytes = try submitSchemaValidation(&svc, a, context, command);
            defer a.free(bytes);
            var accepted = try std.json.parseFromSlice(receipts.Observation, a, bytes, .{});
            defer accepted.deinit();
            try std.testing.expect(try accepted.value.isAcknowledged(request));
        }
        try std.testing.expect(!svc.locked);
        try std.testing.expectEqual(@as(usize, 1), svc.preflights);
        try std.testing.expectEqual(@as(usize, if (mode == 5) 0 else 1), svc.proposals);
        try std.testing.expectEqual(@as(usize, if (mode == 5 or mode == 6) 0 else 1), svc.waits);
    }
}

pub fn schemaValidationReceiptRead(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/schema_validation_receipt.zig").Request) ![]u8 {
    if (!context.schema_validation_authority) return error.Forbidden;
    try request.validate();
    try context.ensureActive();
    if (request.lookup.group != svc.metadata_group_id) return error.InvalidGenerationPublication;
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.schemaValidationReceiptJson(alloc, svc.metadata_group_id, request);
}

test "system catalog schema validation receipts require narrow authority without proposing mutations" {
    const receipts = @import("../metadata/schema_validation_receipt.zig");
    const Fake = struct {
        metadata_group_id: u64 = 21,
        barriers: usize = 0,
        available: bool = true,
        store: Store = .{},
        const Store = struct {
            reads: usize = 0,
            pub fn schemaValidationReceiptJson(self: *@This(), a: std.mem.Allocator, _: u64, request: receipts.Request) ![]u8 {
                self.reads += 1;
                return std.json.Stringify.valueAlloc(a, receipts.Observation{ .lookup = request.lookup }, .{});
            }
        };
        pub fn ensureLinearizableReadWithContext(self: *@This(), context: operation.RequestContext) !void {
            try context.ensureActive();
            self.barriers += 1;
        }
        pub fn projectedStore(self: *@This()) ?*Store {
            return if (self.available) &self.store else null;
        }
    };
    const a = std.testing.allocator;
    var svc: Fake = .{};
    const request: receipts.Request = .{ .lookup = .{ .group = 21, .table = 7, .id = @splat(1), .metadata_incarnation = @splat('1'), .plan_digest = @splat(2) }, .expected = .admission };
    for ([_]operation.RequestContext{ .{}, .{ .setting_admin = true }, .{ .index_publication_authority = true } }) |untrusted|
        try std.testing.expectError(error.Forbidden, schemaValidationReceiptRead(&svc, a, untrusted, request));
    try std.testing.expectEqual(0, svc.barriers);
    const context: operation.RequestContext = .{ .schema_validation_authority = true };
    try std.testing.expectError(error.DeadlineExceeded, schemaValidationReceiptRead(&svc, a, .{ .schema_validation_authority = true, .deadline_ns = 0 }, request));
    svc.metadata_group_id = 22;
    try std.testing.expectError(error.InvalidGenerationPublication, schemaValidationReceiptRead(&svc, a, context, request));
    svc.metadata_group_id = 21;
    svc.available = false;
    try std.testing.expectError(error.MissingMetadataStore, schemaValidationReceiptRead(&svc, a, context, request));
    svc.available = true;
    const bytes = try schemaValidationReceiptRead(&svc, a, context, request);
    defer a.free(bytes);
    var observed = try std.json.parseFromSlice(receipts.Observation, a, bytes, .{});
    defer observed.deinit();
    try std.testing.expect(!try observed.value.isAcknowledged(request));
    try std.testing.expectEqual(1, svc.store.reads);
    try std.testing.expectEqual(2, svc.barriers);
}

pub fn schemaValidationPrepare(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/schema_validation_admission.zig").Request) ![]u8 {
    if (!context.setting_admin or !context.schema_validation_authority) return error.Forbidden;
    try request.validate();
    try context.ensureActive();
    // Activate only the decoder cohort, never a table/job reservation. A new
    // cluster must not need a previous job to make its first preparation legal.
    _ = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.schema_validation_publication_version);
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.schemaValidationPrepareCut(alloc, svc.metadata_group_id, request);
}

pub fn schemaValidationWork(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/schema_validation_work.zig").Query) ![]u8 {
    if (!context.schema_validation_authority) return error.Forbidden;
    try request.validate();
    try context.ensureActive();
    if (request.group != svc.metadata_group_id) return error.InvalidGenerationPublication;
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.schemaValidationWork(alloc, svc.metadata_group_id, request);
}

pub fn schemaValidationDiscover(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/schema_validation_read.zig").DiscoveryRequest) ![]u8 {
    if (!context.schema_validation_authority) return error.Forbidden;
    try request.validate();
    try context.ensureActive();
    if (request.group != svc.metadata_group_id) return error.InvalidGenerationPublication;
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.schemaValidationDiscover(alloc, svc.metadata_group_id, request);
}

pub fn schemaValidationOwnerRead(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/schema_validation_read.zig").OwnerReadRequest) ![]u8 {
    if (!context.schema_validation_authority) return error.Forbidden;
    try request.validate();
    try context.ensureActive();
    if (request.lookup.group != svc.metadata_group_id) return error.InvalidGenerationPublication;
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.schemaValidationOwnerRead(alloc, svc.metadata_group_id, request);
}

test "system catalog schema validation owner reads require narrow authority and preserve binary projection" {
    const ledger = @import("../metadata/schema_validation_read.zig");
    const Fake = struct {
        metadata_group_id: u64 = 21,
        barriers: usize = 0,
        available: bool = true,
        store: Store = .{},
        const Store = struct {
            control: usize = 0,
            inputs: usize = 0,
            pub fn schemaValidationDiscover(self: *@This(), a: std.mem.Allocator, _: u64, _: ledger.DiscoveryRequest) ![]u8 {
                self.control += 1;
                return a.dupe(u8, "null");
            }
            pub fn schemaValidationWork(self: *@This(), a: std.mem.Allocator, _: u64, _: @import("../metadata/schema_validation_work.zig").Query) ![]u8 {
                self.control += 1;
                return std.json.Stringify.valueAlloc(a, @import("../metadata/schema_validation_work.zig").Page{}, .{});
            }
            pub fn schemaValidationOwnerRead(self: *@This(), a: std.mem.Allocator, group: u64, request: ledger.OwnerReadRequest) ![]u8 {
                return switch (request.projection) {
                    .control => self.schemaValidationOwnerControlJson(a, group, request.lookup, request.ordinal, request.action),
                    .inputs => self.schemaValidationOwnerInputBytes(a, group, request.lookup, request.ordinal),
                    .parent => self.schemaValidationParentJson(a, group, request.lookup, request.source_image.?),
                    .recovery_inputs => self.schemaValidationRecoveryInputBytes(a, group, request.lookup, request.ordinal, request.source_image.?),
                    .replay => a.dupe(u8, "null"),
                    .replay_inputs => self.schemaValidationOwnerInputBytes(a, group, request.lookup, request.ordinal),
                };
            }
            pub fn schemaValidationOwnerControlJson(self: *@This(), a: std.mem.Allocator, _: u64, _: ledger.Lookup, _: u16, _: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Action) ![]u8 {
                self.control += 1;
                return a.dupe(u8, "{}");
            }
            pub fn schemaValidationOwnerInputBytes(self: *@This(), a: std.mem.Allocator, _: u64, _: ledger.Lookup, _: u16) ![]u8 {
                self.inputs += 1;
                return a.dupe(u8, "\x00\xffASVI-transport-fixture");
            }
            pub fn schemaValidationParentJson(_: *@This(), a: std.mem.Allocator, _: u64, _: ledger.Lookup, _: ledger.ImageIdentity) ![]u8 {
                return a.dupe(u8, "null");
            }
            pub fn schemaValidationRecoveryInputBytes(self: *@This(), a: std.mem.Allocator, group: u64, lookup: ledger.Lookup, ordinal: u16, _: ledger.ImageIdentity) ![]u8 {
                return self.schemaValidationOwnerInputBytes(a, group, lookup, ordinal);
            }
        };
        pub fn ensureLinearizableReadWithContext(self: *@This(), context: operation.RequestContext) !void {
            try context.ensureActive();
            self.barriers += 1;
        }
        pub fn projectedStore(self: *@This()) ?*Store {
            return if (self.available) &self.store else null;
        }
    };
    const a = std.testing.allocator;
    var svc: Fake = .{};
    var request: ledger.OwnerReadRequest = .{ .lookup = .{ .group = 21, .table = 7, .id = @splat(1), .metadata_incarnation = @splat('1'), .plan_digest = @splat(2) }, .ordinal = 0 };
    for ([_]operation.RequestContext{ .{}, .{ .setting_admin = true }, .{ .index_publication_authority = true }, .{ .fk_generation_publication_authority = true } }) |untrusted|
        try std.testing.expectError(error.Forbidden, schemaValidationOwnerRead(&svc, a, untrusted, request));
    try std.testing.expectEqual(0, svc.barriers);
    const context: operation.RequestContext = .{ .schema_validation_authority = true };
    const discovery: ledger.DiscoveryRequest = .{ .group = request.lookup.group, .table = request.lookup.table, .metadata_incarnation = request.lookup.metadata_incarnation };
    try std.testing.expectError(error.Forbidden, schemaValidationDiscover(&svc, a, .{ .setting_admin = true }, discovery));
    var foreign_discovery = discovery;
    foreign_discovery.group += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, schemaValidationDiscover(&svc, a, context, foreign_discovery));
    const no_job = try schemaValidationDiscover(&svc, a, context, discovery);
    defer a.free(no_job);
    try std.testing.expectEqualStrings("null", no_job);
    const work_query: @import("../metadata/schema_validation_work.zig").Query = .{ .group = request.lookup.group, .metadata_incarnation = request.lookup.metadata_incarnation };
    for ([_]operation.RequestContext{ .{}, .{ .setting_admin = true }, .{ .index_publication_authority = true }, .{ .fk_generation_publication_authority = true } }) |untrusted|
        try std.testing.expectError(error.Forbidden, schemaValidationWork(&svc, a, untrusted, work_query));
    try std.testing.expectError(error.DeadlineExceeded, schemaValidationWork(&svc, a, .{ .schema_validation_authority = true, .deadline_ns = 0 }, work_query));
    var foreign_work = work_query;
    foreign_work.group += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, schemaValidationWork(&svc, a, context, foreign_work));
    const work_page = try schemaValidationWork(&svc, a, context, work_query);
    defer a.free(work_page);
    var parsed_work = try std.json.parseFromSlice(@import("../metadata/schema_validation_work.zig").Page, a, work_page, .{});
    defer parsed_work.deinit();
    try parsed_work.value.validate(work_query);
    try std.testing.expectEqual(@as(u8, 0), parsed_work.value.len);
    // Keep the existing owner-read operation-count assertions independent.
    svc.barriers = 0;
    svc.store.control = 0;
    try std.testing.expectError(error.DeadlineExceeded, schemaValidationOwnerRead(&svc, a, .{ .schema_validation_authority = true, .deadline_ns = 0 }, request));
    svc.metadata_group_id = 22;
    try std.testing.expectError(error.InvalidGenerationPublication, schemaValidationOwnerRead(&svc, a, context, request));
    svc.metadata_group_id = 21;
    try std.testing.expectEqual(0, svc.barriers);
    svc.available = false;
    try std.testing.expectError(error.MissingMetadataStore, schemaValidationOwnerRead(&svc, a, context, request));
    svc.available = true;
    const control = try schemaValidationOwnerRead(&svc, a, context, request);
    defer a.free(control);
    try std.testing.expectEqualStrings("{}", control);
    request.projection = .inputs;
    const inputs = try schemaValidationOwnerRead(&svc, a, context, request);
    defer a.free(inputs);
    try std.testing.expectEqualSlices(u8, "\x00\xffASVI-transport-fixture", inputs);
    try std.testing.expectEqual(1, svc.store.control);
    try std.testing.expectEqual(1, svc.store.inputs);
    try std.testing.expectEqual(3, svc.barriers);
}

pub fn relationalIndexOwnerDecisionJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_publication.zig").OwnerRequest) ![]u8 {
    if (!context.index_publication_authority) return error.Forbidden;
    try request.validate();
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.relationalIndexOwnerDecisionJson(alloc, svc.metadata_group_id, request);
}

pub fn relationalIndexSourceAcceptanceStatusJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, query: @import("../metadata/relational_index_delivery_journal.zig").Query) ![]u8 {
    if (!context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try query.validate();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.relationalIndexSourceAcceptanceStatusJson(alloc, svc.metadata_group_id, query);
}
pub fn acceptRelationalIndexSource(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_delivery_journal.zig").Request) ![]u8 {
    const journal = @import("../metadata/relational_index_delivery_journal.zig");
    if (!context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.signed.verify();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.index_delivery_version);
    try svc.ensureLinearizableReadWithContext(context);
    svc.lockCatalogMutation();
    var gate_held = true;
    defer if (gate_held) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(bytes);
    if (bytes.len > journal.max_command_bytes) return error.CatalogCommandTooLarge;
    try store.validateRelationalIndexSourceAcceptance(svc.metadata_group_id, bytes);
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .accept_relational_index_delivery = bytes });
    svc.unlockCatalogMutation();
    gate_held = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.relationalIndexSourceAcceptanceStatusJson(alloc, svc.metadata_group_id, journal.Query.from(request)) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(observed);
    var parsed = std.json.parseFromSlice(journal.Status, alloc, observed, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer parsed.deinit();
    if (!(parsed.value.isAccepted(request) catch return error.MetadataMutationOutcomeUnknown)) return error.MetadataMutationOutcomeUnknown;
    return observed;
}

pub fn completeRelationalIndexSource(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("../metadata/relational_index_delivery_journal.zig").CompletionRequest) ![]u8 {
    const journal = @import("../metadata/relational_index_delivery_journal.zig");
    if (!context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.index_delivery_version);
    try svc.ensureLinearizableReadWithContext(context);
    svc.lockCatalogMutation();
    var gate_held = true;
    defer if (gate_held) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(bytes);
    if (bytes.len > journal.max_completion_bytes) return error.CatalogCommandTooLarge;
    try store.validateRelationalIndexSourceCompletion(svc.metadata_group_id, bytes);
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .complete_relational_index_source = bytes });
    svc.unlockCatalogMutation();
    gate_held = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.relationalIndexSourceAcceptanceStatusJson(alloc, svc.metadata_group_id, request.query) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(observed);
    var parsed = std.json.parseFromSlice(journal.Status, alloc, observed, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer parsed.deinit();
    if (!(parsed.value.isCompleted(request) catch return error.MetadataMutationOutcomeUnknown)) return error.MetadataMutationOutcomeUnknown;
    return observed;
}

test "relational index publication decision dispatch preserves authority gates and uncertain outcomes" {
    const publication = @import("../metadata/relational_index_publication.zig");
    const Mock = struct {
        metadata_group_id: u64 = 13,
        locked: bool = false,
        proposals: usize = 0,
        wait_failure: bool = false,
        store: Store,
        const Store = struct {
            observed: publication.DecisionStatus,
            reject: bool = false,
            pub fn validateRelationalIndexPublicationDecision(self: *@This(), _: u64, _: publication.DecidePublish) !void {
                if (self.reject) return error.GenerationPublicationChanged;
            }
            pub fn relationalIndexPublicationDecisionStatusJson(self: *@This(), alloc: std.mem.Allocator, _: u64, _: publication.DecidePublish) ![]u8 {
                return std.json.Stringify.valueAlloc(alloc, self.observed, .{});
            }
        };
        pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: operation.RequestContext, version: u16) !u16 {
            try std.testing.expectEqual(protocol.index_publication_decision_version, version);
            return version;
        }
        pub fn ensureLinearizableReadWithContext(_: *@This(), _: operation.RequestContext) !void {}
        pub fn validateTableTopologyProtocolReadinessWithContext(self: *@This(), _: operation.RequestContext, _: u16) !void {
            try std.testing.expect(self.locked);
        }
        pub fn lockCatalogMutation(self: *@This()) void {
            std.debug.assert(!self.locked);
            self.locked = true;
        }
        pub fn unlockCatalogMutation(self: *@This()) void {
            std.debug.assert(self.locked);
            self.locked = false;
        }
        pub fn projectedStore(self: *@This()) ?*Store {
            return &self.store;
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: storage.TransitionCommand) !u64 {
            try std.testing.expect(self.locked);
            try std.testing.expect(command == .decide_relational_index_publication);
            self.proposals += 1;
            if (self.store.observed.head.phase == .building) self.store.observed.head = try self.store.observed.head.decidePublish(command.decide_relational_index_publication.expected_revision);
            return 1;
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: operation.RequestContext) !void {
            try std.testing.expect(!self.locked);
            if (self.wait_failure) return error.Timeout;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const plan = try publication.testing.plan(arena.allocator());
    var prepared = try plan.prepare(std.testing.allocator);
    defer prepared.deinit();
    var head = prepared.head;
    head.phase = .building;
    head.revision = 4;
    head.completed_sources = head.owner_count;
    head.completion_revision = 3;
    for (head.counts[0..3]) |*count| count.* = head.owner_count;
    const request: publication.DecidePublish = .{ .table_id = head.table_id, .id = head.plan_id, .expected_revision = head.revision };
    var svc: Mock = .{ .store = .{ .observed = .{ .head = head } } };
    const context: operation.RequestContext = .{ .index_publication_authority = true };
    try std.testing.expectError(error.Forbidden, decideRelationalIndexPublication(&svc, std.testing.allocator, .{}, undefined));
    try std.testing.expectError(error.Forbidden, relationalIndexPublicationDecisionStatusJson(&svc, std.testing.allocator, .{}, undefined));
    try std.testing.expectError(error.DeadlineExceeded, decideRelationalIndexPublication(&svc, std.testing.allocator, .{ .index_publication_authority = true, .deadline_ns = 0 }, undefined));
    try std.testing.expectEqual(@as(usize, 0), svc.proposals);
    const bytes = try decideRelationalIndexPublication(&svc, std.testing.allocator, context, request);
    defer std.testing.allocator.free(bytes);
    try std.testing.expect(!svc.locked);
    var observed = try std.json.parseFromSlice(publication.DecisionStatus, std.testing.allocator, bytes, .{});
    defer observed.deinit();
    try std.testing.expect(try observed.value.isDecided(request));
    svc.store.reject = true;
    try std.testing.expectError(error.GenerationPublicationChanged, decideRelationalIndexPublication(&svc, std.testing.allocator, context, request));
    try std.testing.expect(!svc.locked);
    try std.testing.expectEqual(@as(usize, 1), svc.proposals);
    svc.store.reject = false;
    svc.wait_failure = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, decideRelationalIndexPublication(&svc, std.testing.allocator, context, request));
    try std.testing.expect(!svc.locked);
    svc.wait_failure = false;
    svc.store.observed.replication_pending = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, decideRelationalIndexPublication(&svc, std.testing.allocator, context, request));
    const pending_json = try relationalIndexPublicationDecisionStatusJson(&svc, std.testing.allocator, context, request);
    defer std.testing.allocator.free(pending_json);
    var pending = try std.json.parseFromSlice(publication.DecisionStatus, std.testing.allocator, pending_json, .{});
    defer pending.deinit();
    try std.testing.expect(!try pending.value.isDecided(request));
}

test "relational index publication cancellation dispatch fences authority and preserves uncertain outcomes" {
    const publication = @import("../metadata/relational_index_publication.zig");
    const Mock = struct {
        metadata_group_id: u64 = 13,
        locked: bool = false,
        proposals: usize = 0,
        wait_failure: bool = false,
        store: Store,
        const Store = struct {
            observed: publication.CancellationStatus,
            reject: bool = false,
            pub fn validateRelationalIndexCancellation(self: *@This(), _: u64, _: publication.CancelDecision) !void {
                if (self.reject) return error.GenerationPublicationChanged;
            }
            pub fn relationalIndexCancellationStatusJson(self: *@This(), alloc: std.mem.Allocator, _: u64, _: publication.CancelDecision) ![]u8 {
                return std.json.Stringify.valueAlloc(alloc, self.observed, .{});
            }
        };
        pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: operation.RequestContext, version: u16) !u16 {
            try std.testing.expectEqual(protocol.index_publication_head_version, version);
            return version;
        }
        pub fn ensureLinearizableReadWithContext(_: *@This(), _: operation.RequestContext) !void {}
        pub fn validateTableTopologyProtocolReadinessWithContext(self: *@This(), _: operation.RequestContext, _: u16) !void {
            try std.testing.expect(self.locked);
        }
        pub fn lockCatalogMutation(self: *@This()) void {
            std.debug.assert(!self.locked);
            self.locked = true;
        }
        pub fn unlockCatalogMutation(self: *@This()) void {
            std.debug.assert(self.locked);
            self.locked = false;
        }
        pub fn projectedStore(self: *@This()) ?*Store {
            return &self.store;
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: storage.TransitionCommand) !u64 {
            try std.testing.expect(self.locked);
            try std.testing.expect(command == .cancel_relational_index_publication);
            self.proposals += 1;
            const request = command.cancel_relational_index_publication;
            if (self.store.observed.head.phase == .fencing) self.store.observed.head = try self.store.observed.head.cancel(request.expected_revision, request.failure);
            return 1;
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: operation.RequestContext) !void {
            try std.testing.expect(!self.locked);
            if (self.wait_failure) return error.Timeout;
        }
    };
    const request: publication.CancelDecision = .{ .table_id = 9, .id = @splat(1), .expected_revision = 1, .failure = .duplicate_key };
    var svc: Mock = .{ .store = .{ .observed = .{ .head = .{ .table_id = 9, .plan_id = request.id, .plan_digest = @splat(2), .candidate_digest = @splat(3), .reservation_digest = @splat(4), .owner_count = 1 } } } };
    const context: operation.RequestContext = .{ .index_publication_authority = true };
    try std.testing.expectError(error.Forbidden, cancelRelationalIndexPublication(&svc, std.testing.allocator, .{}, undefined));
    try std.testing.expectError(error.Forbidden, relationalIndexCancellationStatusJson(&svc, std.testing.allocator, .{}, undefined));
    try std.testing.expectError(error.DeadlineExceeded, cancelRelationalIndexPublication(&svc, std.testing.allocator, .{ .index_publication_authority = true, .deadline_ns = 0 }, undefined));
    try std.testing.expectEqual(@as(usize, 0), svc.proposals);
    const bytes = try cancelRelationalIndexPublication(&svc, std.testing.allocator, context, request);
    defer std.testing.allocator.free(bytes);
    var status = try std.json.parseFromSlice(publication.CancellationStatus, std.testing.allocator, bytes, .{});
    defer status.deinit();
    try std.testing.expect(try status.value.isCanceled(request));
    try std.testing.expect(!svc.locked);
    svc.store.reject = true;
    try std.testing.expectError(error.GenerationPublicationChanged, cancelRelationalIndexPublication(&svc, std.testing.allocator, context, request));
    try std.testing.expectEqual(@as(usize, 1), svc.proposals);
    svc.store.reject = false;
    svc.wait_failure = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, cancelRelationalIndexPublication(&svc, std.testing.allocator, context, request));
    try std.testing.expect(!svc.locked);
    svc.wait_failure = false;
    svc.store.observed.replication_pending = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, cancelRelationalIndexPublication(&svc, std.testing.allocator, context, request));
    const pending_bytes = try relationalIndexCancellationStatusJson(&svc, std.testing.allocator, context, request);
    defer std.testing.allocator.free(pending_bytes);
    var pending = try std.json.parseFromSlice(publication.CancellationStatus, std.testing.allocator, pending_bytes, .{});
    defer pending.deinit();
    try std.testing.expect(!try pending.value.isCanceled(request));
}

test "relational index publication ACK submission fences capabilities releases mutation gates and preserves ambiguous outcomes" {
    const publication = @import("../metadata/relational_index_publication.zig");
    const Mock = struct {
        metadata_group_id: u64 = 13,
        locked: bool = false,
        proposals: usize = 0,
        waits: usize = 0,
        wait_failure: bool = false,
        store: Store,
        const Store = struct {
            observed: publication.AckStatus,
            reject: bool = false,
            pub fn validateRelationalIndexOwnerAck(self: *@This(), _: u64, _: []const u8) !void {
                if (self.reject) return error.IndexOwnerReceiptChanged;
            }
            pub fn relationalIndexOwnerAckStatusJson(self: *@This(), alloc: std.mem.Allocator, _: u64, _: publication.Ack) ![]u8 {
                return std.json.Stringify.valueAlloc(alloc, self.observed, .{});
            }
        };
        pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: operation.RequestContext, version: u16) !u16 {
            try std.testing.expectEqual(protocol.index_publication_head_version, version);
            return version;
        }
        pub fn ensureLinearizableReadWithContext(_: *@This(), _: operation.RequestContext) !void {}
        pub fn validateTableTopologyProtocolReadinessWithContext(self: *@This(), _: operation.RequestContext, _: u16) !void {
            try std.testing.expect(self.locked);
        }
        pub fn lockCatalogMutation(self: *@This()) void {
            std.debug.assert(!self.locked);
            self.locked = true;
        }
        pub fn unlockCatalogMutation(self: *@This()) void {
            std.debug.assert(self.locked);
            self.locked = false;
        }
        pub fn projectedStore(self: *@This()) ?*Store {
            return &self.store;
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: storage.TransitionCommand) !u64 {
            try std.testing.expect(self.locked);
            try std.testing.expect(command == .ack_relational_index_publication);
            self.proposals += 1;
            return 1;
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: operation.RequestContext) !void {
            try std.testing.expect(!self.locked);
            self.waits += 1;
            if (self.wait_failure) return error.Timeout;
        }
    };
    const request = try publication.testing.fencedAck();
    var svc: Mock = .{ .store = .{ .observed = try publication.testing.completeAckStatus(request) } };
    const context: operation.RequestContext = .{ .index_publication_authority = true };
    try std.testing.expectError(error.Forbidden, acknowledgeRelationalIndexOwner(&svc, std.testing.allocator, .{}, undefined));
    try std.testing.expectError(error.Forbidden, relationalIndexOwnerAckStatusJson(&svc, std.testing.allocator, .{}, undefined));
    try std.testing.expectError(error.DeadlineExceeded, acknowledgeRelationalIndexOwner(&svc, std.testing.allocator, .{ .index_publication_authority = true, .deadline_ns = 0 }, undefined));
    try std.testing.expectEqual(@as(usize, 0), svc.proposals);
    const bytes = try acknowledgeRelationalIndexOwner(&svc, std.testing.allocator, context, request);
    std.testing.allocator.free(bytes);
    try std.testing.expect(!svc.locked);
    try std.testing.expectEqual(@as(usize, 1), svc.proposals);
    svc.store.reject = true;
    try std.testing.expectError(error.IndexOwnerReceiptChanged, acknowledgeRelationalIndexOwner(&svc, std.testing.allocator, context, request));
    try std.testing.expect(!svc.locked);
    try std.testing.expectEqual(@as(usize, 1), svc.proposals);
    svc.store.reject = false;
    svc.wait_failure = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, acknowledgeRelationalIndexOwner(&svc, std.testing.allocator, context, request));
    try std.testing.expect(!svc.locked);
    svc.wait_failure = false;
    svc.store.observed = publication.testing.pendingAckStatus(request);
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, acknowledgeRelationalIndexOwner(&svc, std.testing.allocator, context, request));
    try std.testing.expect(!svc.locked);
    try std.testing.expectEqual(@as(usize, 3), svc.proposals);
    try std.testing.expectEqual(@as(usize, 3), svc.waits);
}

test "system catalog relational index publication read requires authority before a linearizable barrier" {
    const Mock = struct {
        metadata_group_id: u64 = 13,
        reads: usize = 0,
        const Store = struct {
            pub fn relationalIndexOwnerDecisionJson(_: *@This(), _: std.mem.Allocator, _: u64, _: @import("../metadata/relational_index_publication.zig").OwnerRequest) ![]u8 {
                return error.UnexpectedRead;
            }
        };
        pub fn ensureLinearizableReadWithContext(self: *@This(), _: operation.RequestContext) !void {
            self.reads += 1;
        }
        pub fn projectedStore(_: *@This()) ?*Store {
            return null;
        }
    };
    var svc: Mock = .{};
    const request: @import("../metadata/relational_index_publication.zig").OwnerRequest = .{ .table_id = 1, .group_id = 2, .id = @splat(3), .operation = .fence };
    try std.testing.expectError(error.Forbidden, relationalIndexOwnerDecisionJson(&svc, std.testing.allocator, .{}, request));
    try std.testing.expectEqual(@as(usize, 0), svc.reads);
    try std.testing.expectError(error.MissingMetadataStore, relationalIndexOwnerDecisionJson(&svc, std.testing.allocator, .{ .index_publication_authority = true }, request));
    try std.testing.expectEqual(@as(usize, 1), svc.reads);
}

pub fn mutateFkGenerationPublication(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, command: fk_publication.Command) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try command.validateShape();
    try context.ensureActive();
    if (command.plan) |plan| try plan.requireAdmissionCoverage();
    // The full generation lifecycle shares one decoder floor, including
    // recovery commands which do not repeat the immutable plan.
    const required = protocol.fk_generation_column_addition_version;
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, required);
    try svc.ensureLinearizableReadWithContext(context);
    svc.lockCatalogMutation();
    var gate_held = true;
    defer if (gate_held) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    if (command.plan) |plan| if (plan.constraint_name_guard != null)
        try store.preflightFkGenerationNameAdmission(svc.metadata_group_id, plan);
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > fk_publication.max_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_fk_generation_publication = bytes });
    svc.unlockCatalogMutation();
    gate_held = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.fkGenerationPublicationStatusJson(alloc, svc.metadata_group_id, command.child_table_id) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(observed);
    var parsed = std.json.parseFromSlice(fk_publication.Publication, alloc, observed, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer parsed.deinit();
    if (!std.mem.eql(u8, &parsed.value.plan.id, &command.plan_id) or parsed.value.revision != command.expected_revision + 1)
        return error.MetadataMutationOutcomeUnknown;
    return observed;
}

pub fn mutateFkInitialCreate(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, command: fk_publication.InitialCommand) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try command.validateShape();
    try context.ensureActive();
    const naming = if (command.plan) |plan| plan.constraint_name_admission != null else false;
    const minimum = if (command.catalog_coverage == .checks_v1) protocol.initial_fk_checks_catalog_version else if (naming) @max(protocol.fk_initial_create_version, protocol.constraint_occupancy_version) else protocol.fk_initial_create_version;
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, minimum);
    try svc.ensureLinearizableReadWithContext(context);
    svc.lockCatalogMutation();
    var gate_held = true;
    defer if (gate_held) svc.unlockCatalogMutation();
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > fk_publication.max_bytes) return error.CatalogCommandTooLarge;
    // A rejected begin must never become a failing Raft apply entry. Keep
    // the mutation lock across this exact aborting-txn preflight and proposal;
    // a competing metadata leader may still win, in which case committed
    // stale begin entries are deterministic no-ops at apply.
    try store.preflightFkInitialCreateCommand(svc.metadata_group_id, bytes);
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_fk_initial_create = bytes });
    svc.unlockCatalogMutation();
    gate_held = false;
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.fkInitialCreateStatusJson(alloc, svc.metadata_group_id, command.child_table_id) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(observed);
    var parsed = std.json.parseFromSlice(fk_publication.InitialPublication, alloc, observed, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer parsed.deinit();
    if (!std.mem.eql(u8, &parsed.value.plan.id, &command.plan_id) or parsed.value.revision != command.expected_revision + 1)
        return error.MetadataMutationOutcomeUnknown;
    return observed;
}

/// Durable draft-only policy mutation. The authorization is a body-bound
/// administrator service grant; this transition never installs an owner
/// bundle or flips the serving publication. Ambiguous outcomes require a
/// status read rather than replaying an uncertain command.
pub fn mutatePolicyDefinition(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, command: @import("antfly_local_sources").system_catalog_policies.Command) ![]u8 {
    if (!context.setting_admin) return error.Forbidden;
    try context.ensureActive();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.sql_row_policy_intent_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const current = try store.systemCatalogMeta(alloc, svc.metadata_group_id);
    if (current.revision != command.expected_revision) return error.RowPolicyCatalogChanged;
    const bytes = try store.sqlPolicyDefinitionCommandJson(alloc, svc.metadata_group_id, command);
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_sql_policies = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.systemCatalogMeta(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected_hash, .{});
    if (observed.revision != command.expected_revision + 1 or !std.mem.eql(u8, &observed.last_command, &expected_hash))
        return error.MetadataMutationOutcomeUnknown;
    return std.json.Stringify.valueAlloc(alloc, observed, .{});
}

pub fn mutateSetting(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: settings.Request) ![]u8 {
    if (!context.setting_admin) return error.Forbidden;
    try context.ensureActive();
    const required = if (request == .role_default or (request == .put and requiresRoleDefaultReaders(request.put.role_defaults))) protocol.sql_role_defaults_version else protocol.sql_setting_catalog_version;
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, required);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const database: ?[]const u8 = if (request == .role_default and request.role_default.database.len != 0) request.role_default.database else null;
    var snapshot = try store.sqlSettingMutationCut(alloc, svc.metadata_group_id, database);
    defer snapshot.deinit();
    const state = .{ .revision = snapshot.meta.revision, .next_id = snapshot.meta.next_id, .settings = snapshot.records };
    var command: settings.Command = .{ .expected_revision = state.revision, .change = undefined };
    var patched_defaults: ?[]settings.RoleDefault = null;
    defer if (patched_defaults) |owned| alloc.free(owned);
    switch (request) {
        .put => |input| {
            const prior = for (state.settings) |record| {
                if (std.ascii.eqlIgnoreCase(record.name, input.name)) break record;
            } else null;
            if (!settings.databaseBindingsRetained(input.role_defaults, prior)) return error.InvalidSettingRecord;
            if (prior) |record| if (input.matches(record)) return std.json.Stringify.valueAlloc(alloc, snapshot.meta, .{});
            const identity: settings.Identity = if (prior) |record|
                .{ .id = record.identity.id, .generation = try std.math.add(u64, record.identity.generation, 1) }
            else
                .{ .id = state.next_id, .generation = 1 };
            const record = input.record(identity);
            try record.validate();
            command.change = .{ .put = record };
        },
        .drop => |name| {
            try settings.validateName(name);
            const prior = for (state.settings) |record| {
                if (std.ascii.eqlIgnoreCase(record.name, name)) break record;
            } else return std.json.Stringify.valueAlloc(alloc, snapshot.meta, .{});
            command.change = .{ .drop = prior.identity };
        },
        .role_default => |change| {
            try change.validate();
            if (!snapshot.database_exists) return error.DatabaseNotFound;
            const existing = for (state.settings) |record| {
                if (std.ascii.eqlIgnoreCase(record.name, change.name)) break record;
            } else null;
            const prior = existing orelse blk: {
                const builtin = settings.builtinInput(change.name) orelse return error.UnknownSetting;
                // RESET of an unconfigured builtin is already its base value.
                if (change.value == null) return std.json.Stringify.valueAlloc(alloc, snapshot.meta, .{});
                break :blk builtin.record(.{ .id = state.next_id, .generation = 1 });
            };
            var bound_change = change;
            bound_change.database_id = snapshot.database_id;
            const defaults = try bound_change.applyAlloc(alloc, prior);
            patched_defaults = defaults;
            const input: settings.Input = .{ .name = prior.name, .kind = prior.kind, .policy_sensitive = prior.policy_sensitive, .session_writable = prior.session_writable, .default = prior.default, .database_defaults = prior.database_defaults, .role_defaults = defaults };
            if (input.matches(prior)) return std.json.Stringify.valueAlloc(alloc, snapshot.meta, .{});
            const record = input.record(prior.identity);
            try record.validate();
            // Keep the changed slice alive through serialization below. The
            // enclosing snapshot owns all the unchanged nested values.
            command.change = .{ .put = record };
            command.role_defaults_only = true;
        },
    }
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_sql_settings = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.systemCatalogMeta(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected_hash, .{});
    if (observed.revision != command.expected_revision + 1 or !std.mem.eql(u8, &observed.last_command, &expected_hash)) return error.MetadataMutationOutcomeUnknown;
    return std.json.Stringify.valueAlloc(alloc, observed, .{});
}

fn requiresRoleDefaultReaders(defaults: []const settings.RoleDefault) bool {
    return settings.roleDefaultsRequireReaders(defaults);
}

pub fn resolve(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, target: domain.Target) !?table_manager.TableRecord {
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.resolveSystemCatalogTable(alloc, svc.metadata_group_id, target);
}

fn listTablesJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: domain.TableList) ![]u8 {
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const result = try store.listSystemCatalogTables(alloc, svc.metadata_group_id, request);
    errdefer alloc.free(result);
    try context.ensureActive();
    return result;
}

pub fn call(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, input: @import("server_call.zig").Call) anyerror![]u8 {
    return switch (input) {
        .resolver_authority_mutate => |write| @import("../metadata/resolver_authority.zig").mutateOnService(svc, alloc, write, context),
        .resolver_catalog_read => |read| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const bytes = try store.resolverCatalogRead(alloc, svc.metadata_group_id, read);
            errdefer alloc.free(bytes);
            try context.ensureActive();
            break :blk bytes;
        },
        .lake_index_lifecycle_read => |table_id| @import("../metadata/lake_index_lifecycle.zig").readOnService(svc, alloc, table_id, context),
        .lake_index_lifecycle_work => |after| @import("../metadata/lake_index_lifecycle.zig").workOnService(svc, alloc, after, context),
        .lake_index_lifecycle_mutate => |write| blk: {
            if (!context.setting_admin) return error.Forbidden;
            try @import("../metadata/lake_index_lifecycle.zig").mutateOnService(svc, alloc, write, context);
            break :blk alloc.dupe(u8, "{}");
        },
        .setting_snapshot => |scope| settingSnapshotJson(svc, alloc, context, scope),
        .policy_snapshot => |request| policySnapshotJson(svc, alloc, context, request),
        .policy_install_snapshot => |request| policyInstallSnapshotJson(svc, alloc, context, request),
        .policy_publication_status => |table_id| policyPublicationStatusJson(svc, alloc, context, table_id),
        .policy_publication_work => |after_table_id| policyPublicationWorkJson(svc, alloc, context, after_table_id),
        .policy_publication_begin => |request| beginPolicyPublication(svc, alloc, context, request),
        .policy_definition_mutate => |command| mutatePolicyDefinition(svc, alloc, context, command),
        .policy_publication_mutate => |command| mutatePolicyPublication(svc, alloc, context, command),
        .fk_generation_publication_begin => |plan| mutateFkGenerationPublication(svc, alloc, context, .{ .plan_id = plan.id, .child_table_id = plan.child_before.table_id, .expected_revision = 0, .action = .begin, .plan = plan, .catalog_coverage = plan.catalog_coverage }),
        .fk_generation_publication_mutate => |command| mutateFkGenerationPublication(svc, alloc, context, command),
        .fk_generation_publication_status => |child_table_id| fkGenerationPublicationStatusJson(svc, alloc, context, child_table_id),
        .fk_generation_publication_work => |after_child_table_id| fkGenerationPublicationWorkJson(svc, alloc, context, after_child_table_id),
        .fk_generation_publication_decision => |request| fkGenerationPublicationDecisionJson(svc, alloc, context, request),
        .fk_generation_publication_source_decision => |request| fkGenerationPublicationSourceDecisionJson(svc, alloc, context, request),
        .relational_index_status => |request| relationalIndexStatusJson(svc, alloc, context, request),
        .relational_index_prepare => |request| prepareRelationalIndexPublication(svc, alloc, context, request),
        .relational_index_begin => |request| beginRelationalIndexPublication(svc, alloc, context, request),
        .relational_index_admission_status => |request| relationalIndexAdmissionStatusJson(svc, alloc, context, request),
        .relational_index_owner_decision => |request| relationalIndexOwnerDecisionJson(svc, alloc, context, request),
        .schema_validation_owner_read => |request| schemaValidationOwnerRead(svc, alloc, context, request),
        .schema_validation_discover => |request| schemaValidationDiscover(svc, alloc, context, request),
        .schema_validation_work => |request| schemaValidationWork(svc, alloc, context, request),
        .schema_validation_status => |request| blk: {
            if (!context.setting_admin or !context.schema_validation_authority) return error.Forbidden;
            try request.validate();
            try context.ensureActive();
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.schemaValidationStatusJson(alloc, svc.metadata_group_id, request);
        },
        .schema_validation_prepare => |request| schemaValidationPrepare(svc, alloc, context, request),
        .schema_validation_receipt_read => |request| schemaValidationReceiptRead(svc, alloc, context, request),
        .schema_validation_submit => |command| submitSchemaValidation(svc, alloc, context, command),
        .relational_index_owner_ack => |request| acknowledgeRelationalIndexOwner(svc, alloc, context, request),
        .relational_index_owner_ack_status => |request| relationalIndexOwnerAckStatusJson(svc, alloc, context, request),
        .relational_index_publish_decide => |request| decideRelationalIndexPublication(svc, alloc, context, request),
        .relational_index_publish_status => |request| relationalIndexPublicationDecisionStatusJson(svc, alloc, context, request),
        .relational_index_cancel => |request| cancelRelationalIndexPublication(svc, alloc, context, request),
        .relational_index_cancel_status => |request| relationalIndexCancellationStatusJson(svc, alloc, context, request),
        .relational_index_source_accept => |request| acceptRelationalIndexSource(svc, alloc, context, request),
        .relational_index_source_complete => |request| completeRelationalIndexSource(svc, alloc, context, request),
        .relational_index_source_accept_status => |request| relationalIndexSourceAcceptanceStatusJson(svc, alloc, context, request),
        .fk_initial_create_prepare => |request| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            if (request.generated_constraint_names.len != 0)
                _ = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.constraint_occupancy_version);
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkInitialCreatePrepareJsonWithContext(alloc, svc.metadata_group_id, request, context);
        },
        .constraint_name_prepare => |request| blk: {
            if (!context.setting_admin) return error.Forbidden;
            _ = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.constraint_occupancy_version);
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.prepareConstraintNamesJsonWithContext(alloc, svc.metadata_group_id, .{ .addition = request }, context);
        },
        .constraint_name_check => |request| blk: {
            if (!context.setting_admin) return error.Forbidden;
            _ = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.constraint_occupancy_version);
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            try store.checkConstraintNamesWithContext(alloc, svc.metadata_group_id, request, context);
            break :blk std.json.Stringify.valueAlloc(alloc, true, .{});
        },
        .fk_initial_child_decision => |request| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkInitialChildDecisionJson(alloc, svc.metadata_group_id, request);
        },
        .fk_initial_create_begin => |plan| mutateFkInitialCreate(svc, alloc, context, .{ .catalog_coverage = plan.catalog_coverage, .plan_id = plan.id, .child_table_id = plan.child.table_id, .expected_revision = 0, .action = .begin, .plan = plan }),
        .fk_initial_create_mutate => |command| mutateFkInitialCreate(svc, alloc, context, command),
        .fk_initial_create_status => |child_table_id| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkInitialCreateStatusJson(alloc, svc.metadata_group_id, child_table_id);
        },
        .fk_generation_table_locked => |table_id| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkGenerationTableLockedJson(alloc, svc.metadata_group_id, table_id);
        },
        .fk_initial_create_work => |after_child_table_id| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkInitialCreateWorkJson(alloc, svc.metadata_group_id, after_child_table_id);
        },
        .fk_initial_parent_decision => |request| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkInitialParentDecisionJson(alloc, svc.metadata_group_id, request);
        },
        // A shared internal catalog read capability cannot attest to the
        // requested physical store. Leave the durable projection available to
        // a future store-bound transport, but never serve it through this
        // generic operation.
        .fk_initial_retirement_page => error.InitialFkRetirementNotActivated,
        .store_root_enroll, .fk_initial_retirement_ack => storeRootMutation(svc, alloc, context, input),
        .store_root_enrollment_status => |identity| storeRootEnrollmentStatus(svc, alloc, context, identity),
        .fk_initial_retirement_signed_page => |request| storeRootPage(svc, alloc, context, request),
        .setting_mutate => |request| mutateSetting(svc, alloc, context, request),
        .write_validation_revision => blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk std.json.Stringify.valueAlloc(alloc, @import("../metadata/api.zig").MetadataHead{
                .metadata_group_id = svc.metadata_group_id,
                .metadata_incarnation = try svc.metadataIncarnation(),
                .metadata_epoch = try store.writeValidationRevision(svc.metadata_group_id),
            }, .{});
        },
        .write_validation => |name| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const result = try store.tableWriteValidation(alloc, svc.metadata_group_id, name);
            errdefer alloc.free(result);
            try context.ensureActive();
            break :blk result;
        },
        .table_status => |target| listTablesJson(svc, alloc, context, target.listing()),
        .list_tables => |request| listTablesJson(svc, alloc, context, request),
        .export_snapshot => blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.exportSystemCatalog(alloc, svc.metadata_group_id);
        },
        .read => |request| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.systemCatalogRead(alloc, svc.metadata_group_id, request);
        },
        .query_definition => |name| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const definition = try store.queryTableDefinition(alloc, svc.metadata_group_id, name);
            defer if (definition) |value| value.deinit(alloc);
            break :blk std.json.Stringify.valueAlloc(alloc, definition, .{});
        },
        .snapshot => snapshotJson(svc, alloc, context),
        .resolve => |target| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const table = try store.resolveSystemCatalogIdentity(alloc, svc.metadata_group_id, target);
            defer if (table) |value| value.deinit(alloc);
            break :blk std.json.Stringify.valueAlloc(alloc, table, .{});
        },
        .resolve_many => |request| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const result = try store.resolveSystemCatalogIdentitiesWithContext(alloc, svc.metadata_group_id, request, context);
            defer result.deinit(alloc);
            break :blk std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .relation_schema_mutate => |request| blk: {
            if (!context.setting_admin) return error.Forbidden;
            try context.ensureActive();
            try request.validate();
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const before = (try store.getTable(alloc, svc.metadata_group_id, request.guard.owner.table_id)) orelse return error.CatalogGenerationChanged;
            defer table_manager.freeTable(alloc, before);
            if (try tables_api.schemaVersion(before.schema_json) != request.guard.owner.schema_version) return error.CatalogGenerationChanged;
            const after = try tables_api.applySchemaMutationRecord(alloc, &before, .replace, request.schema_json);
            defer table_manager.freeTable(alloc, after);
            break :blk call(svc, alloc, context, .{ .relation_replace = .{ .guard = request.guard, .expected = before, .replacement = after } });
        },
        .relation_description_mutate => |request| blk: {
            if (!context.setting_admin) return error.Forbidden;
            try context.ensureActive();
            try request.validate();
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            if (request.column) |column| {
                const stamp = try svc.commentColumnWithContext(context, request.guard, .{ .name = column, .value = request.description });
                break :blk std.json.Stringify.valueAlloc(alloc, @import("server_call.zig").RelationReplacementResult{ .schema_version = request.guard.owner.schema_version, .stamp = stamp }, .{}) catch return error.MetadataMutationOutcomeUnknown;
            }
            const before = (try store.getTable(alloc, svc.metadata_group_id, request.guard.owner.table_id)) orelse return error.CatalogGenerationChanged;
            defer table_manager.freeTable(alloc, before);
            var after = try table_manager.cloneTable(alloc, before);
            defer table_manager.freeTable(alloc, after);
            const description = try alloc.dupe(u8, request.description);
            alloc.free(after.description);
            after.description = description;
            break :blk call(svc, alloc, context, .{ .relation_replace = .{ .guard = request.guard, .expected = before, .replacement = after } });
        },
        .relation_replace => |request| blk: {
            if (!context.setting_admin) return error.Forbidden;
            try context.ensureActive();
            try request.validate();
            try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(alloc, request.replacement.indexes_json);
            try managed_embedder.validateEmbeddingProducerOwnershipJson(alloc, request.replacement.indexes_json);
            const version = try tables_api.schemaVersion(request.replacement.schema_json);
            const stamp = try svc.replaceRelationTableDefinitionStampedWithContext(context, request.expected, request.replacement, request.guard);
            // A response allocation failure after the durable receipt cannot
            // become a replayable pre-admission error on the forwarding hop.
            break :blk std.json.Stringify.valueAlloc(alloc, @import("server_call.zig").RelationReplacementResult{ .schema_version = version, .stamp = stamp }, .{}) catch return error.MetadataMutationOutcomeUnknown;
        },
        .sql_view_mutate => |request| mutateSqlView(svc, alloc, context, request),
        .sql_sequence_prepare => blk: {
            if (!context.setting_admin) return error.Forbidden;
            try context.ensureActive();
            try svc.ensureSqlSequenceDecoderReadyWithContext(context);
            try context.ensureActive();
            break :blk alloc.dupe(u8, "null");
        },
        .sql_sequence_mutate => |request| mutateSqlSequence(svc, alloc, context, request),
        .sql_column_sequence_handoffs => |request| blk: {
            if (!context.setting_admin) return error.Forbidden;
            try context.ensureActive();
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const bytes = try store.prepareColumnSequenceHandoffs(alloc, svc.metadata_group_id, request);
            errdefer alloc.free(bytes);
            try context.ensureActive();
            break :blk bytes;
        },
        .sql_sequence_snapshot => |request| blk: {
            if (!context.setting_admin) return error.Forbidden;
            try context.ensureActive();
            try request.validate();
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const bytes = try store.sqlSequenceSnapshot(alloc, svc.metadata_group_id, request, context);
            errdefer alloc.free(bytes);
            try context.ensureActive();
            break :blk bytes;
        },
        .sql_sequence_admission_head => blk: {
            if (!context.setting_admin) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const head = try store.sqlSequenceAdmissionHead(svc.metadata_group_id);
            try context.ensureActive();
            break :blk std.json.Stringify.valueAlloc(alloc, head, .{});
        },
        .sql_sequence_outcome => |request| blk: {
            if (!context.setting_admin) return error.Forbidden;
            try request.validate();
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const outcome = try store.sqlSequenceOutcome(alloc, svc.metadata_group_id, request);
            try context.ensureActive();
            break :blk std.json.Stringify.valueAlloc(alloc, outcome, .{});
        },
        .sql_sequence_counter_mutate => |request| mutateSqlSequenceCounter(svc, alloc, context, request),
        .sql_sequence_counter_outcome => |request| blk: {
            if (!context.setting_admin) return error.Forbidden;
            try request.validate();
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const result = try store.sqlSequenceCounterOutcome(alloc, svc.metadata_group_id, request);
            try context.ensureActive();
            break :blk std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .native_memory_create => |request| blk: {
            if (!context.setting_admin) return error.Forbidden;
            var admitted = context;
            admitted.native_service = .memory;
            var owned = request;
            owned.mutation.native_service = .memory;
            break :blk mutate(svc, alloc, admitted, owned);
        },
        .mutate => |request| mutate(svc, alloc, context, request),
    };
}

/// Publish the restored physical incarnation and its logical binding together.
/// Job retries are accepted only when both projections match exactly.
pub fn restore(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, target: domain.Target, table: table_manager.TableRecord, source_ranges: []const table_manager.RangeRecord) !void {
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.system_catalog_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const meta = try store.systemCatalogMeta(alloc, svc.metadata_group_id);
    const admission = try svc.captureTableRestoreAdmission(alloc, table);
    const ranges = try @import("../metadata/table_topology_mutations.zig").deriveRestoreDestinationRanges(alloc, table, source_ranges, admission.incarnation_generation);
    defer {
        for (ranges) |range| table_manager.freeRange(alloc, range);
        alloc.free(ranges);
    }
    if (admission.already_applied) {
        const bound = (try store.resolveSystemCatalogTable(alloc, svc.metadata_group_id, target)) orelse return error.MetadataMutationOutcomeUnknown;
        defer table_manager.freeTable(alloc, bound);
        if (bound.table_id != table.table_id) return error.TableAlreadyExists;
        return svc.verifyTableCreateProjection(alloc, table, ranges);
    }
    const command: storage.SystemCatalogCommand = .{
        .expected_revision = meta.revision,
        .mutation = .{ .action = .create, .kind = .table, .database = target.database, .namespace = target.namespace, .name = target.table, .table_id = table.table_id, .storage_name = table.name },
        .topology = .{ .create = .{ .expected_transition_generation = admission.expected_transition_generation, .table = table, .ranges = ranges } },
    };
    try store.validateSystemCatalog(svc.metadata_group_id, command);
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_system_catalog = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const bound = (store.resolveSystemCatalogTable(alloc, svc.metadata_group_id, target) catch return error.MetadataMutationOutcomeUnknown) orelse return error.MetadataMutationOutcomeUnknown;
    defer table_manager.freeTable(alloc, bound);
    if (bound.table_id != table.table_id) return error.MetadataMutationOutcomeUnknown;
    svc.verifyTableCreateProjection(alloc, table, ranges) catch return error.MetadataMutationOutcomeUnknown;
}
