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

//! One source validation quantum under a metadata-authorized owner binding.
//! Begin/cancel use the same guarded 2PC key as pages, so a canceled job cannot
//! overtake a prepared parent attachment. No public validity bit changes here.
const std = @import("std");
const local = @import("antfly_local_sources");
const activation = local.storage_db_relational_integrity_activation_contract;
const owner_contract = local.storage_db_relational_schema_owner_contract;
const operation = local.api_operation;
pub const Result = enum { pending, ready, failed };

pub fn step(a: std.mem.Allocator, reader: local.api_table_read_source.TableReadSource, writer: local.api_table_write_source.TableWriteSource, tables: []const local.common_topology_records.TableRecord, ranges: []const local.common_topology_records.RangeRecord, range: local.common_topology_records.RangeRecord, binding: owner_contract.Binding, inputs: []const u8, physical_root: u128, cancel: bool, request: operation.RequestContext) !Result {
    const control = try request.platformDeadline();
    try control.ensureActive();
    if (binding.purpose != .foreign_key_source or range.table_id != binding.fence.namespace.table_id or range.group_id != binding.fence.owner_group_id or
        (binding.authority == .native and physical_root == 0)) return error.SchemaValidationJobChanged;
    const table = for (tables) |value| {
        if (value.table_id == range.table_id) break value;
    } else return error.TableNotFound;
    if (!std.mem.eql(u8, &owner_contract.hash(table.schema_json), &binding.before_public)) return error.SchemaValidationJobChanged;
    var current = try reader.lookup(a, table.name, range.start_key, .{ .relational_activation_json = "{\"mode\":\"validation_status\"}", .execution_deadline_ns = control.deadline_ns, .cancellation = control.cancellation }, .read_index);
    defer if (current) |*value| value.deinit(a);
    var parsed: ?std.json.Parsed(activation.ForeignKeyValidationRecord) = null;
    defer if (parsed) |*value| value.deinit();
    if (current) |value| parsed = try std.json.parseFromSlice(activation.ForeignKeyValidationRecord, a, value.json, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    const prior: ?activation.ForeignKeyValidationRecord = if (parsed) |value| value.value else null;
    var next: activation.ForeignKeyValidationRecord = undefined;
    var scope: activation.ForeignKeyValidationScope = undefined;
    if (prior) |record| {
        try record.validate();
        if (std.mem.eql(u8, &record.binding.id, &binding.id)) {
            if (!std.meta.eql(record.binding, binding)) return error.SchemaValidationJobChanged;
            // Cancellation may retire inert history copied from an older
            // root. It grants no ready/failed attestation or install authority.
            if (cancel and record.progress.state == .invalid) return .failed;
            if (record.physical_root != (if (binding.authority == .native) physical_root else 0)) return error.SchemaValidationRuntimeRecoveryRequired;
            if (record.progress.state == .invalid) return .failed;
            if (!cancel) {
                if (record.progress.state == .enforced) return .ready;
                _ = try local.api_relational_activation_worker.runForeignKeyValidationPageControlled(a, reader, writer, tables, ranges, range, binding.id, control);
                return .pending;
            }
            next = record;
            next.revision = try std.math.add(u64, record.revision, 1);
            next.progress.state = .invalid;
            next.progress.failure = "Canceled";
            scope = .{ .binding = binding, .physical_root = record.physical_root, .action = .cancel };
        } else {
            if (record.progress.state != .invalid or record.binding.fence.admission_epoch >= binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
            if (cancel) return .failed;
        }
    } else if (cancel) return .failed;
    const beginning = prior == null or !std.mem.eql(u8, &prior.?.binding.id, &binding.id);
    if (beginning) {
        if (inputs.len == 0 or !std.mem.eql(u8, &owner_contract.hash(inputs), &binding.inputs)) return error.SchemaValidationJobChanged;
        var status = (try reader.lookup(a, table.name, range.start_key, .{ .relational_activation_json = "{\"mode\":\"status\"}", .execution_deadline_ns = control.deadline_ns, .cancellation = control.cancellation }, .read_index)) orelse return error.ConstraintActivationUnavailable;
        defer status.deinit(a);
        var observed = try std.json.parseFromSlice(struct { schema_version: u32, generation_set: [32]u8, owner: [32]u8, state: activation.State }, a, status.json, .{ .ignore_unknown_fields = true });
        defer observed.deinit();
        if (observed.value.schema_version != binding.before_version or !std.mem.eql(u8, &observed.value.owner, &binding.owner)) return error.SchemaValidationJobChanged;
        if (observed.value.state != .enforced) return .pending;
        next = .{ .binding = binding, .physical_root = if (binding.authority == .native) physical_root else 0, .revision = 1, .progress = .{ .generation_set = observed.value.generation_set, .owner = binding.owner, .schema_version = binding.before_version, .phase = .foreign_key } };
        scope = .{ .binding = binding, .physical_root = next.physical_root, .action = .begin, .inputs = .{ .bytes = inputs } };
    }
    const expected = if (prior) |record| try record.encode(a) else null;
    defer if (expected) |bytes| a.free(bytes);
    const encoded = try next.encode(a);
    defer a.free(encoded);
    const command: activation.Command = .{ .routing_key = range.start_key, .expected = expected, .next = encoded, .validation = scope };
    const commit: local.api_distributed_txn_contract.TableCommitRequest = .{ .table_name = table.name, .relational_schema_version = binding.before_version, .relational_integrity_generation_set = next.progress.generation_set, .relational_activation = command };
    try control.ensureActive();
    const outcome = (try writer.commitBatchWithCancellation(a, &.{commit}, .write, control.cancellation)) orelse return error.ConstraintActivationUnavailable;
    return switch (outcome) {
        .committed, .conflict => .pending,
    };
}
