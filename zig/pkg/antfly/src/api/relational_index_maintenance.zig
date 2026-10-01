// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2
//! Bounded selected-owner maintenance admission. Every command is an ordinary
//! replicated transaction; local build progress is observation, never Raft state.
const std = @import("std");
const wire = @import("antfly_indexes_openapi").types;
const native = @import("../storage/db/relational_index_maintenance_contract.zig");
const status_wire = @import("../storage/db/relational_index_status_contract.zig");
const writes = @import("table_write_source.zig");
const reads = @import("table_read_source.zig");
const operation = @import("operation.zig");
const tables = @import("tables.zig");
const contract = @import("distributed_txn_contract.zig");

fn decimal(value: []const u8) !u64 {
    if (value.len == 0 or value.len > 20) return error.InvalidIndexMaintenance;
    if (value.len > 1 and value[0] == '0') return error.InvalidIndexMaintenance;
    for (value) |byte| if (byte < '0' or byte > '9') return error.InvalidIndexMaintenance;
    return std.fmt.parseInt(u64, value, 10) catch error.InvalidIndexMaintenance;
}

fn digest(value: []const u8) ![32]u8 {
    if (value.len != 64) return error.InvalidIndexMaintenance;
    for (value) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.InvalidIndexMaintenance;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, value) catch return error.InvalidIndexMaintenance;
    return result;
}

pub fn execute(alloc: std.mem.Allocator, source: anytype, reader: reads.TableReadSource, writer: writes.TableWriteSource, table_name: []const u8, index_name: []const u8, action: native.Action, body: []const u8, request: operation.RequestContext) ![]u8 {
    try request.ensureActive();
    const control = @import("index_maintenance_control.zig").Control{ .request = request };
    if (body.len > 128 * 1024) return error.InvalidIndexMaintenance;
    var parsed = std.json.parseFromSlice(wire.IndexMaintenanceRequest, alloc, body, .{}) catch return error.InvalidIndexMaintenance;
    defer parsed.deinit();
    const input = parsed.value;
    const schema_version = std.math.cast(u32, input.schema_version) orelse return error.InvalidIndexMaintenance;
    if (input.owners.len == 0 or input.owners.len > 128) return error.InvalidIndexMaintenance;
    const table_id = try decimal(input.table_id);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const temporary = arena.allocator();
    var snapshot = (try source.adminSnapshot()) orelse return error.Unavailable;
    defer source.freeAdminSnapshot(&snapshot);
    const table = tables.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    if (table.table_id != table_id) return error.PreparedGenerationChanged;
    if (table.restore_backup_id.len != 0 or table.relational_retirement_json.len != 0) return error.TableTransitionActive;
    var schema = try @import("../schema/mod.zig").parseValidatedTableSchema(temporary, table.schema_json);
    defer schema.deinit(temporary);
    if (schema.storage_mode != .relational) return error.MethodNotAllowed;
    if (schema.version != input.schema_version) return error.PreparedGenerationChanged;
    const comparison = try @import("relational_index_status.zig").expectedComparison(temporary, schema, index_name);
    var selected = std.AutoHashMap(u64, void).init(temporary);
    var commands: std.ArrayList(native.Command) = .empty;
    for (input.owners) |proof| {
        const group_id = try decimal(proof.group_id);
        const entry = try selected.getOrPut(group_id);
        if (entry.found_existing) return error.InvalidIndexMaintenance;
        var selected_range: ?@import("../common/topology_records.zig").RangeRecord = null;
        for (snapshot.ranges) |range| if (range.table_id == table_id and range.group_id == group_id) {
            if (selected_range != null or range.restore_backup_id.len != 0) return error.TopologyChanged;
            selected_range = range;
        };
        const range = selected_range orelse return error.TopologyChanged;
        const command: native.Command = .{
            .action = action,
            .table_id = table_id,
            .owner_group_id = group_id,
            .schema_version = schema_version,
            .index_name = index_name,
            .generation = try decimal(proof.generation),
            .slot = std.math.cast(u32, proof.slot) orelse return error.InvalidIndexMaintenance,
            .owner = try digest(proof.owner),
            .comparison = try digest(proof.comparison),
            .expected_progress_digest = try digest(proof.progress_digest),
            .expected_maintenance_epoch = try decimal(proof.maintenance_epoch),
            .routing_key = range.start_key,
        };
        try command.validate();
        if (!std.mem.eql(u8, &command.comparison, &comparison)) return error.PreparedGenerationChanged;
        try commands.append(temporary, command);
    }
    // Allocate response before any durable admission. Errors can still leave a
    // prefix admitted; the exact request is its own bounded resume token.
    const acknowledged = try temporary.alloc([]const u8, input.owners.len);
    for (input.owners, acknowledged) |proof, *group| group.* = proof.group_id;
    const response = try std.json.Stringify.valueAlloc(alloc, wire.IndexMaintenanceResponse{ .acknowledged_groups = acknowledged }, .{});
    errdefer alloc.free(response);
    const lookup = try std.json.Stringify.valueAlloc(temporary, status_wire.Request{ .name = index_name, .schema_version = schema_version }, .{});
    for (commands.items) |command| {
        try request.ensureActive();
        var current = (try source.adminSnapshot()) orelse return error.Unavailable;
        defer source.freeAdminSnapshot(&current);
        const current_table = tables.findTableByName(&current, table_name) orelse return error.TableNotFound;
        if (current_table.table_id != table_id or !std.mem.eql(u8, current_table.schema_json, table.schema_json) or current_table.restore_backup_id.len != 0 or current_table.relational_retirement_json.len != 0) return error.PreparedGenerationChanged;
        const range = for (current.ranges) |range| {
            if (range.table_id == table_id and range.group_id == command.owner_group_id and std.mem.eql(u8, range.start_key, command.routing_key) and range.restore_backup_id.len == 0) break range;
        } else return error.TopologyChanged;
        var observed = (try reader.lookup(alloc, table_name, command.routing_key, .{ .relational_index_status_json = lookup, .execution_deadline_ns = request.deadline_ns, .execution_io = request.deadline_io, .cancellation = request.cancellation }, .read_index)) orelse return error.Unavailable;
        defer observed.deinit(alloc);
        if (observed.json.len > status_wire.max_response_bytes) return error.ResourceBudgetExceeded;
        var status = try std.json.parseFromSlice(status_wire.Status, alloc, observed.json, .{});
        defer status.deinit();
        try validateObservation(command, status.value, range.end_key orelse "");
        const fingerprint = command.fingerprint();
        const txn_id: [16]u8 = fingerprint[0..16].*;
        if (std.mem.eql(u8, &status.value.last_maintenance_request, &fingerprint)) {
            _ = writer.acknowledgeTransactionCommit(alloc, txn_id, command.owner_group_id, table_name) catch |err| switch (err) {
                error.TxnNotFound => null,
                else => return err,
            };
            continue;
        }
        try request.ensureActive();
        const mutations = [_]contract.TableCommitRequest{.{ .table_name = table_name, .relational_schema_version = command.schema_version, .relational_index_maintenance = command }};
        const outcome = (writer.commitTransactionWithIdAndCancellation(alloc, txn_id, 1, &mutations, .write, control.cancellation()) catch |err| {
            try request.ensureActive();
            return err;
        }) orelse return error.MethodNotAllowed;
        switch (outcome) {
            .conflict => return error.PreparedGenerationChanged,
            .committed => |committed| {
                if (committed.visibility_pending) return error.Unavailable;
                // The native ticket receipt now provides replay identity. Do not
                // retain an unbounded second copy in transaction terminal state.
                _ = try writer.acknowledgeTransactionCommit(alloc, txn_id, command.owner_group_id, table_name);
            },
        }
    }
    return response;
}

fn validateObservation(command: native.Command, status: status_wire.Status, range_end: []const u8) !void {
    if (status.table_id != command.table_id or status.schema_version != command.schema_version or status.generation != command.generation or status.slot != command.slot or !std.mem.eql(u8, &status.owner, &command.owner) or !std.mem.eql(u8, &status.comparison, &command.comparison) or !std.mem.eql(u8, status.range_start, command.routing_key) or !std.mem.eql(u8, status.range_end, range_end)) return error.PreparedGenerationChanged;
    if (std.mem.eql(u8, &status.last_maintenance_request, &command.fingerprint())) return;
    if (status.maintenance_epoch != command.expected_maintenance_epoch or !std.mem.eql(u8, &status.progress_digest, &command.expected_progress_digest)) return error.PreparedGenerationChanged;
    if (status.state != .failed and !(command.action == .repair and status.state == .ready)) return error.PreparedGenerationChanged;
}

test "index maintenance requires canonical decimal and digest observations" {
    try std.testing.expectEqual(@as(u64, 18446744073709551615), try decimal("18446744073709551615"));
    try std.testing.expectEqual(@as(u64, 0), try decimal("0"));
    try std.testing.expectError(error.InvalidIndexMaintenance, decimal("01"));
    try std.testing.expectError(error.InvalidIndexMaintenance, decimal("+1"));
    try std.testing.expectError(error.InvalidIndexMaintenance, decimal("18446744073709551616"));
    try std.testing.expectError(error.InvalidIndexMaintenance, digest("AA" ** 32));
    try std.testing.expectEqual(@as([32]u8, @splat(0xaa)), try digest("aa" ** 32));
}

test "index maintenance resumes partial acknowledgements and fences stale owners" {
    const alloc = std.testing.allocator;
    const db_types = @import("../storage/db/types.zig");
    const metadata = @import("../metadata/api.zig");
    const Fixture = struct {
        const schema_json =
            \\{"version":2,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"by_id","keys":[{"column":"id"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        statuses: [2]status_wire.Status,
        commits: usize = 0,
        acknowledgements: usize = 0,
        fail_second: bool = true,
        cancelled: bool = false,
        cancel_after_first: bool = false,
        changed_topology: bool = false,
        pub fn adminSnapshot(self: *@This()) !?metadata.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(&[_]@import("../common/topology_records.zig").TableRecord{.{ .table_id = 7, .name = "rows", .schema_json = schema_json }}),
                .ranges = if (self.changed_topology) &.{} else @constCast(&[_]@import("../common/topology_records.zig").RangeRecord{
                    .{ .table_id = 7, .group_id = 11, .start_key = "", .end_key = "\x00\xff" },
                    .{ .table_id = 7, .group_id = 12, .start_key = "\x00\xff", .end_key = null },
                }),
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        pub fn freeAdminSnapshot(_: *@This(), _: *metadata.AdminSnapshot) void {}
        fn lookup(ptr: *anyopaque, allocator: std.mem.Allocator, _: []const u8, key: []const u8, _: db_types.LookupOptions, consistency: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(.read_index, consistency);
            return .{ .version = 0, .json = try std.json.Stringify.valueAlloc(allocator, self.statuses[@intFromBool(key.len != 0)], .{}) };
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.ScanResponse {
            return error.TestUnexpectedResult;
        }
        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_types.SearchRequest, _: @import("../raft/read_gate.zig").ReadConsistency) !?@import("query_response.zig").QueryResponse {
            return error.TestUnexpectedResult;
        }
        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_types.BatchRequest) !?void {
            return error.TestUnexpectedResult;
        }
        fn commit(ptr: *anyopaque, _: std.mem.Allocator, txn_id: [16]u8, begin: u64, mutations: []const contract.TableCommitRequest, level: db_types.SyncLevel, cancellation: db_types.CancellationToken) !?contract.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (cancellation.isCancelled()) return error.Canceled;
            try std.testing.expectEqual(@as(u64, 1), begin);
            try std.testing.expectEqual(.write, level);
            try std.testing.expectEqual(@as(usize, 1), mutations.len);
            const command = mutations[0].relational_index_maintenance.?;
            const fingerprint = command.fingerprint();
            try std.testing.expectEqual(fingerprint[0..16].*, txn_id);
            const slot: usize = @intFromBool(command.owner_group_id == 12);
            if (slot == 1 and self.fail_second) return error.Unavailable;
            self.commits += 1;
            self.statuses[slot].maintenance_epoch += 1;
            self.statuses[slot].last_maintenance_request = fingerprint;
            self.statuses[slot].state = .building;
            self.statuses[slot].progress_digest = @splat(99);
            if (self.cancel_after_first) self.cancelled = true;
            return .{ .committed = .{ .participant_count = 1 } };
        }
        fn acknowledge(ptr: *anyopaque, _: std.mem.Allocator, _: [16]u8, _: u64, _: []const u8) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.acknowledgements += 1;
            return {};
        }
        fn cancelledFn(ptr: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            return self.cancelled;
        }
    };
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, Fixture.schema_json);
    defer parsed.deinit(alloc);
    const comparison = try @import("relational_index_status.zig").expectedComparison(alloc, parsed, "by_id");
    const initial = status_wire.Status{ .table_id = 7, .schema_version = 2, .generation = 8, .slot = 1, .catalog = @splat(0), .comparison = comparison, .owner = @splat(3), .range_start = "", .range_end = "\x00\xff", .state = .failed, .failure = .invalid_row, .rows_scanned = 4, .progress_digest = @splat(4), .maintenance_epoch = 0, .last_maintenance_request = @splat(0) };
    var fixture = Fixture{ .statuses = .{ initial, initial } };
    fixture.statuses[1].range_start = "\x00\xff";
    fixture.statuses[1].range_end = "";
    const reader = reads.TableReadSource{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = Fixture.scan, .query = Fixture.query } };
    const writer = writes.TableWriteSource{ .ptr = &fixture, .vtable = &.{ .batch = Fixture.batch, .commit_transaction_with_id_with_cancellation = Fixture.commit, .acknowledge_transaction_commit = Fixture.acknowledge } };
    const owner = std.fmt.bytesToHex(initial.owner, .lower);
    const comparison_hex = std.fmt.bytesToHex(comparison, .lower);
    const progress_hex = std.fmt.bytesToHex(initial.progress_digest, .lower);
    const proofs = [_]wire.IndexMaintenanceOwnerProof{
        .{ .group_id = "11", .generation = "8", .slot = 1, .owner = &owner, .comparison = &comparison_hex, .progress_digest = &progress_hex, .maintenance_epoch = "0" },
        .{ .group_id = "12", .generation = "8", .slot = 1, .owner = &owner, .comparison = &comparison_hex, .progress_digest = &progress_hex, .maintenance_epoch = "0" },
    };
    const body = try std.json.Stringify.valueAlloc(alloc, wire.IndexMaintenanceRequest{ .table_id = "7", .schema_version = 2, .owners = @constCast(&proofs) }, .{});
    defer alloc.free(body);
    try std.testing.expectError(error.Unavailable, execute(alloc, &fixture, reader, writer, "rows", "by_id", .retry, body, .{}));
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    fixture.fail_second = false;
    const resumed = try execute(alloc, &fixture, reader, writer, "rows", "by_id", .retry, body, .{});
    defer alloc.free(resumed);
    try std.testing.expectEqual(@as(usize, 2), fixture.commits);
    const repeated = try execute(alloc, &fixture, reader, writer, "rows", "by_id", .retry, body, .{});
    defer alloc.free(repeated);
    try std.testing.expectEqual(@as(usize, 2), fixture.commits);
    try std.testing.expectError(error.PreparedGenerationChanged, execute(alloc, &fixture, reader, writer, "rows", "by_id", .repair, body, .{}));
    fixture.changed_topology = true;
    try std.testing.expectError(error.TopologyChanged, execute(alloc, &fixture, reader, writer, "rows", "by_id", .retry, body, .{}));
    fixture.changed_topology = false;
    fixture.statuses[0] = initial;
    fixture.cancel_after_first = true;
    try std.testing.expectError(error.Canceled, execute(alloc, &fixture, reader, writer, "rows", "by_id", .retry, body, .{ .cancellation = .{ .ptr = &fixture, .is_cancelled_fn = Fixture.cancelledFn } }));
    try std.testing.expectEqual(@as(usize, 3), fixture.commits);
}
