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

//! Bounded physical source pages for online FK validation. The returned
//! continuation is proposed work only; row guards, parent attachments and this
//! exact predecessor must commit through the existing distributed transaction.
const std = @import("std");
const contract = @import("relational_integrity_activation_contract.zig");
const activation = @import("relational_integrity_activation.zig");
const validation = @import("foreign_key_validation.zig");
const input_contract = @import("relational_schema_owner_contract.zig");
const rows = @import("relational_rows.zig");
const A = std.mem.Allocator;

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    rows: rows.Page,
    command: contract.Command,
    constraints: []const []const u8,

    pub fn deinit(self: *@This()) void {
        self.rows.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn prepare(a: A, io: ?std.Io, core: anytype, physical_root: u128, job_id: [16]u8, budget: rows.Budget, control: validation.Control) !?Page {
    try control.ensureActive();
    try budget.validate();
    const captured = block: {
        core.lockApplyShared();
        defer core.unlockApplyShared();
        var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
        errdefer view.release();
        break :block .{ .view = view, .read = try core.store.beginReadTxn(), .authenticated = core.store.valuesAreAuthenticated() };
    };
    var view = captured.view;
    defer view.release();
    var reader = try rows.Reader.openSnapshot(a, captured.read, captured.authenticated, view, null, .{ .include_primary_digest = true }, 0);
    defer reader.deinit();
    const raw_record = reader.read.get(contract.validation_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const observed = try contract.ForeignKeyValidationRecord.decode(raw_record);
    if (observed.binding.authority == .native and observed.physical_root != physical_root) return error.SchemaValidationRuntimeRecoveryRequired;
    if (!std.mem.eql(u8, &observed.binding.id, &job_id)) return error.SchemaValidationJobChanged;
    if (observed.progress.state != .validating) return null;
    var arena = std.heap.ArenaAllocator.init(a);
    var transferred = false;
    defer if (!transferred) arena.deinit();
    const owned = arena.allocator();
    const expected = try owned.dupe(u8, raw_record);
    const before = try contract.ForeignKeyValidationRecord.decode(expected);
    const raw_inputs = try reader.read.get(contract.validation_inputs_key);
    if (!std.mem.eql(u8, &input_contract.hash(raw_inputs), &before.binding.inputs)) return error.SchemaValidationJobChanged;
    const parts = try input_contract.inputParts(raw_inputs);
    const config = try std.json.parseFromSliceLeaky(validation.SourceConfig, owned, parts[3], .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    try config.validate();
    try activation.requireValidationDefinition(&reader.read, before.binding);
    if (reader.active.version() != before.binding.before_version) return error.SchemaValidationJobChanged;
    const public = (reader.active.validator() orelse return error.ConstraintNotFound).schema;
    const definitions = try public.relationalForeignKeyDefinitions(owned);
    const names = try owned.alloc([]const u8, config.parents.len);
    var fields: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (config.parents, names) |scope, *name| {
        try control.ensureActive();
        const definition = for (definitions) |foreign| {
            if (std.mem.eql(u8, foreign.name, scope.constraint_name)) break foreign;
        } else return error.SchemaValidationJobChanged;
        if (!definition.not_valid) return error.SchemaValidationJobChanged;
        name.* = try owned.dupe(u8, definition.name);
        for (definition.child_columns) |field| try fields.put(owned, field, {});
    }
    reader.fields = fields.keys();
    if (before.progress.cursor.len != 0 and (std.mem.order(u8, before.progress.cursor, reader.lower) == .lt or
        std.mem.order(u8, before.progress.cursor, reader.upper) != .lt)) return error.InvalidSchemaValidationJob;
    try reader.after.appendSlice(a, before.progress.cursor);
    var page_rows = try reader.nextPage(a, io, budget);
    errdefer page_rows.deinit();
    try control.ensureActive();
    var next = before;
    next.revision = try std.math.add(u64, next.revision, 1);
    next.progress.rows_scanned = try std.math.add(u64, next.progress.rows_scanned, page_rows.rows.len);
    next.progress.cursor = if (page_rows.more) try owned.dupe(u8, reader.after.items) else "";
    if (!page_rows.more) next.progress.state = .enforced;
    const command: contract.Command = .{
        .routing_key = try activation.routingKey(owned, &reader.read),
        .expected = expected,
        .next = try next.encode(owned),
        .validation = .{ .binding = before.binding, .physical_root = before.physical_root, .action = .page },
    };
    transferred = true;
    return .{ .arena = arena, .rows = page_rows, .command = command, .constraints = names };
}
