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

//! Synchronous query output delivery into the shared unpublished owner lane.
//! The coordinator holds the unique request-owned capture until this returns.
//! Recovery may inspect/seal recorded output, but must never reopen its query.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.storage_db_relational_population_contract;
const staging = local.storage_db_restore_staging_contract;
const catalog = local.sql_catalog;
const owners = @import("restore_owner.zig");
const operation = local.api_operation;

pub const Destination = struct {
    port: owners.Port,
    table: []const u8,
    group: u64,
    scope: staging.Scope,
};

/// One retained producer feeds its selected owner. Routing/partition selection
/// belongs to the catalog coordinator; this function never publishes a table
/// or reconstructs an interrupted producer from durable row counts.
pub fn deliver(a: std.mem.Allocator, destination: Destination, source: catalog.PopulationSource, context: operation.RequestContext) !contract.Cursor {
    const binding = destination.scope.population orelse return error.InvalidRelationPopulation;
    if (!std.mem.eql(u8, &binding.definition, &source.definition) or !std.mem.eql(u8, &binding.source_definition, &source.source_definition)) return error.RelationPopulationChanged;
    const initial: owners.Request = .{ .scope = destination.scope, .action = .status };
    const before = try destination.port.execute(a, destination.table, destination.group, initial, context);
    if (before.population) |cursor| {
        try cursor.validate();
        if (!std.mem.eql(u8, &cursor.binding, &try binding.digest())) return error.RelationPopulationChanged;
        if (cursor.sealed and (before.phase == .imported or before.phase == .validated or before.phase == .published)) return cursor;
    }
    // Even zero committed rows do not mean a query was never evaluated. The
    // durable reserved->importing transition consumes the fresh-producer lane.
    if (before.phase != .reserved) return error.SourceSnapshotIncomplete;
    var cursor = try contract.Cursor.init(binding);
    const begun = try acknowledge(a, destination, .{ .scope = destination.scope, .action = .begin }, cursor, context);
    if (begun.phase != .importing) return error.RelationPopulationChanged;
    while (try source.next(source.ptr, contract.max_page_rows)) |rows| {
        const page: contract.Page = .{ .binding = cursor.binding, .sequence = cursor.pages, .first_row = cursor.rows, .previous = cursor.chain, .rows = rows };
        const admitted = try cursor.admit(page);
        const next = switch (admitted) {
            .apply => |value| value,
            .replay => return error.RelationPopulationChanged,
        };
        const receipt = try acknowledge(a, destination, .{ .scope = destination.scope, .action = .import_page, .population_page = page }, next, context);
        if (receipt.phase != .importing) return error.RelationPopulationChanged;
        cursor = next;
    }
    // Finishing checks cancellation and upstream EOF before the durable seal.
    // There is deliberately no fallible work after a verified sealed receipt.
    try source.finish(source.ptr);
    const completion = cursor.completion();
    const sealed = try cursor.seal(completion);
    const receipt = try acknowledge(a, destination, .{ .scope = destination.scope, .action = .import_page, .population_complete = completion }, sealed, context);
    if (receipt.phase != .imported) return error.RelationPopulationChanged;
    return sealed;
}

fn acknowledge(a: std.mem.Allocator, destination: Destination, request: owners.Request, expected: contract.Cursor, context: operation.RequestContext) !owners.Response {
    const response = destination.port.execute(a, destination.table, destination.group, request, context) catch |send_error| blk: {
        // Never ask the source for another page after an uncertain response.
        // A read-indexed exact receipt can resolve a lost ACK without redoing
        // expression evaluation or mutating the receiver again.
        const observed = destination.port.execute(a, destination.table, destination.group, request.statusRead(), context) catch return send_error;
        if (!std.meta.eql(observed.population orelse return send_error, expected)) return send_error;
        break :blk observed;
    };
    if (!std.meta.eql(response.population orelse return error.InvalidSqlBackendResponse, expected) or response.rows != expected.rows) return error.RelationPopulationChanged;
    return response;
}
