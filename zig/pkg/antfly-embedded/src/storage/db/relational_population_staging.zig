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

//! Query output uses the ordinary restore owner and its atomic row/cursor
//! transaction. No table is published here and no query is replayed on recovery.
const std = @import("std");
const contract = @import("relational_population_contract.zig");
const staging = @import("restore_staging_contract.zig");
const types = @import("types.zig");

pub const Input = union(enum) { page: contract.Page, complete: contract.Completion };
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    phase: staging.Phase,
    batch: ?types.BatchRequest,
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn prepare(alloc: std.mem.Allocator, target: anytype, scope: staging.Scope, input: Input) !Prepared {
    try scope.validate();
    const binding = scope.population orelse return error.InvalidRelationPopulation;
    var arena: std.heap.ArenaAllocator = .init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    const raw = (try target.core.getStoreValue(a, staging.key)) orelse return error.RestoreStagingScopeChanged;
    var loaded = try staging.Progress.decode(a, raw);
    defer loaded.deinit();
    const before = loaded.value;
    if (!std.mem.eql(u8, &scope.digest(), &before.scope.digest())) return error.RestoreStagingScopeChanged;
    if (before.phase == .reserved) return error.RestoreStagingInProgress;
    if (before.phase == .canceled or before.phase == .published) return error.RestoreStagingScopeChanged;
    const cursor = before.population orelse return error.InvalidRelationPopulation;
    var after = before;
    var writes: []types.BatchWrite = &.{};
    var timestamps: []staging.Timestamp = &.{};
    const delivery: contract.Delivery = switch (input) {
        .page => |page| page: {
            if (before.phase != .importing) return error.RelationPopulationChanged;
            switch (try cursor.admit(page)) {
                .replay => return .{ .arena = arena, .phase = before.phase, .batch = null },
                .apply => |next| after.population = next,
            }
            writes = try a.alloc(types.BatchWrite, page.rows.len);
            timestamps = try a.alloc(staging.Timestamp, page.rows.len);
            for (page.rows, writes, timestamps, 0..) |row, *write, *timestamp, ordinal| {
                const key = try a.dupe(u8, &contract.rowKey(binding.partition, page.first_row + ordinal));
                write.* = .{ .key = key, .value = row.document, .json_null_fields = row.json_null_fields };
                timestamp.* = .{ .key = key, .timestamp = binding.write_version };
            }
            break :page .{ .page = .{ .sequence = page.sequence, .first_row = page.first_row, .previous = page.previous } };
        },
        .complete => |receipt| complete: {
            after.population = try cursor.seal(receipt);
            if (cursor.sealed) return .{ .arena = arena, .phase = before.phase, .batch = null };
            if (before.phase != .importing) return error.RelationPopulationChanged;
            after.phase = .imported;
            after.rows_complete = true;
            break :complete .{ .complete = receipt };
        },
    };
    after.rows = after.population.?.rows;
    after.logical_digest = after.population.?.chain;
    const next = try after.encode(a);
    return .{ .arena = arena, .phase = after.phase, .batch = .{
        .writes = writes,
        .sync_level = .write,
        .restore_staging = .{ .import_page = .{ .expected = staging.digest(raw), .next = next, .scope = scope.digest(), .timestamps = timestamps, .population = delivery } },
    } };
}

/// Called inside the same native admission as the ordinary prepared writes.
/// Hash original typed JSON, before storage normalization changes its encoding.
pub fn validate(before: staging.Progress, after: staging.Progress, delivery: contract.Delivery, writes: []const types.BatchWrite, delete_count: usize) !void {
    const cursor = before.population orelse return error.InvalidRelationPopulation;
    if (before.scope.population == null or delete_count != 0 or writes.len > contract.max_page_rows or
        before.phase != .importing or before.rows_complete or !before.source_generation_proofs_complete or
        !before.artifacts_complete or !after.artifacts_complete or !after.source_generation_proofs_complete or
        before.cursor.len != 0 or after.cursor.len != 0 or before.artifact_cursor.len != 0 or after.artifact_cursor.len != 0 or
        before.projection_cursor.len != 0 or after.projection_cursor.len != 0 or before.rewrite != null or after.rewrite != null)
        return error.InvalidRelationPopulation;
    const expected: contract.Cursor = switch (delivery) {
        .page => |header| page: {
            var rows: [contract.max_page_rows]contract.Row = undefined;
            for (writes, rows[0..writes.len], 0..) |write, *row, ordinal| {
                const key = contract.rowKey(before.scope.population.?.partition, std.math.add(u64, header.first_row, ordinal) catch return error.InvalidRelationPopulation);
                if (!std.mem.eql(u8, write.key, &key)) return error.InvalidRelationPopulation;
                row.* = .{ .document = write.value, .json_null_fields = write.json_null_fields };
            }
            const page: contract.Page = .{ .binding = cursor.binding, .sequence = header.sequence, .first_row = header.first_row, .previous = header.previous, .rows = rows[0..writes.len] };
            break :page switch (try cursor.admit(page)) {
                .apply => |next| next,
                .replay => return error.RelationPopulationChanged,
            };
        },
        .complete => |receipt| complete: {
            if (writes.len != 0) return error.InvalidRelationPopulation;
            break :complete try cursor.seal(receipt);
        },
    };
    if (!std.meta.eql(expected, after.population orelse return error.InvalidRelationPopulation) or
        after.rows != expected.rows or !std.mem.eql(u8, &after.logical_digest, &expected.chain) or
        after.rows_complete != expected.sealed or after.phase != (if (expected.sealed) staging.Phase.imported else staging.Phase.importing))
        return error.InvalidRelationPopulation;
}
