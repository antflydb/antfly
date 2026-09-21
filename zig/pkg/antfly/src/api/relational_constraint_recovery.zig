// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Administrator-only retry adapter. Each owner resets its exact failed
//! checkpoint through durable 2PC; retries after partial progress are safe.
const std = @import("std");
const reads = @import("table_read_source.zig");
const writes = @import("table_writes.zig");
const records = @import("../common/topology_records.zig");
const contract = @import("distributed_txn_contract.zig");
const activation = @import("../storage/db/relational_integrity_activation_contract.zig");
const RequestContext = @import("operation.zig").RequestContext;

pub fn retry(
    alloc: std.mem.Allocator,
    reader: reads.TableReadSource,
    writer: writes.TableWriteSource,
    tables: []const records.TableRecord,
    ranges: []const records.RangeRecord,
    request: contract.TableCommitRequest,
    control: RequestContext,
) !void {
    if (request.writes.len != 0 or request.deletes.len != 0 or request.transforms.len != 0) return error.InvalidBatchRequest;
    const version = request.relational_schema_version orelse return error.InvalidBatchRequest;
    const table_id = for (tables) |table| {
        if (std.mem.eql(u8, table.name, request.table_name)) break table.table_id;
    } else return error.TableNotFound;
    var owners: std.ArrayList(records.RangeRecord) = .empty;
    defer owners.deinit(alloc);
    for (ranges) |range| if (range.table_id == table_id) {
        if (range.restore_backup_id.len != 0) return error.TopologyChanged;
        try owners.append(alloc, range);
    };
    if (owners.items.len == 0 or owners.items.len > 4096) return error.TopologyChanged;
    std.mem.sort(records.RangeRecord, owners.items, {}, struct {
        fn less(_: void, a: records.RangeRecord, b: records.RangeRecord) bool {
            return std.mem.order(u8, a.start_key, b.start_key) == .lt;
        }
    }.less);
    var next: ?[]const u8 = "";
    for (owners.items) |owner| {
        if (!std.mem.eql(u8, next orelse return error.TopologyChanged, owner.start_key)) return error.TopologyChanged;
        if (owner.end_key) |end| if (std.mem.order(u8, owner.start_key, end) != .lt) return error.TopologyChanged;
        next = owner.end_key;
    }
    if (next != null) return error.TopologyChanged;
    const json = try std.json.Stringify.valueAlloc(alloc, .{ .mode = "retry", .schema_version = version }, .{});
    defer alloc.free(json);
    for (owners.items) |owner| {
        try control.ensureActive();
        var response = (try reader.lookup(alloc, request.table_name, owner.start_key, .{
            .relational_activation_json = json,
            .execution_deadline_ns = control.deadline_ns,
            .execution_io = control.deadline_io,
            .cancellation = control.cancellation,
        }, .read_index)) orelse continue;
        defer response.deinit(alloc);
        var command = try std.json.parseFromSlice(activation.Command, alloc, response.json, .{ .allocate = .alloc_always });
        defer command.deinit();
        if (!command.value.retry or !std.mem.eql(u8, command.value.routing_key, owner.start_key)) return error.ConstraintActivationOwnerChanged;
        const outcome = (try writer.commitBatchWithCancellation(alloc, &.{.{
            .table_name = request.table_name,
            .relational_schema_version = version,
            .relational_activation = command.value,
        }}, .write, control.cancellation)) orelse return error.ConstraintActivationUnavailable;
        switch (outcome) {
            .committed => {},
            .conflict => return error.ConstraintActivationChanged,
        }
    }
}
