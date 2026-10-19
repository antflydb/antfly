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

//! Immutable candidate preparation shared by distinct owner authorities.
//! This compiler owns its artifacts but grants no permission or receipt.
const std = @import("std");
const Allocator = std.mem.Allocator;
const schema_api = @import("../../schema/mod.zig");
const registry = @import("schema_registry.zig");
const physical = @import("relational_index_catalog.zig");
const jobs = @import("relational_index_owner_job.zig");
const source_authority = @import("../source_authority.zig");
const candidate_mod = @import("relational_index_candidate.zig");
const inputs = @import("relational_index_inputs.zig");
const integrity = @import("relational_integrity.zig");

pub const PreparedCandidate = struct {
    candidate: candidate_mod.Candidate,
    addition: physical.PrivateAddition,
    unique: struct { tuple: @import("relational_index_keys.zig").TuplePlan, generation: integrity.Generation, nulls_not_distinct: bool },

    pub fn deinit(self: *PreparedCandidate) void {
        self.unique.tuple.deinit();
        self.addition.deinit();
        self.candidate.deinit();
    }

    pub fn init(alloc: Allocator, before: registry.SchemaView, txn: anytype, bytes: []const u8, kind: source_authority.Kind) !PreparedCandidate {
        const view = try inputs.decode(bytes);
        const namespace = (try @import("doc_identity.zig").loadNamespaceTxn(txn)) orelse return error.IdentityNamespaceMismatch;
        _ = try source_authority.require(txn, kind, @import("online_source_contract.zig").namespaceBytes(namespace));
        var candidate = try candidate_mod.Candidate.prepare(alloc, namespace.table_id, view.before_json, view.after_json, view.predecessor, view.name);
        errdefer candidate.deinit();
        var after_schema = try schema_api.parseValidatedTableSchema(alloc, view.after_json);
        defer after_schema.deinit(alloc);
        const runtime = try schema_api.deriveRuntimeTableSchema(alloc, after_schema);
        const epoch = registry.Epoch.createOwned(alloc, runtime) catch |err| {
            @import("../schema.zig").freeSchema(alloc, runtime);
            return err;
        };
        var after: registry.SchemaView = .{ .epoch = epoch };
        defer after.release();
        var declaration_arena = std.heap.ArenaAllocator.init(alloc);
        defer declaration_arena.deinit();
        const definitions = (try after_schema.relationalIndexDefinitions(declaration_arena.allocator())) orelse return error.InvalidIndexPublicationCandidate;
        var existing = try physical.load(alloc, txn);
        defer if (existing) |*value| value.deinit();
        const job = try jobs.load(txn);
        var addition = if (job != null and job.?.phase == .reserved)
            try jobs.reopenPhysical(alloc, txn, before, after, view.name)
        else
            try physical.PrivateAddition.init(alloc, before, after, if (existing) |*value| value else null, definitions, view.name);
        errdefer addition.deinit();
        const rules = try after_schema.relationalUniqueDefinitions(declaration_arena.allocator());
        const unique = for (rules) |rule| {
            if (std.mem.eql(u8, rule.name, view.name)) break rule;
        } else return error.InvalidIndexPublicationCandidate;
        if (unique.deferrable or unique.timing != .immediate or unique.expressions.len != 0 or
            unique.where_expressions.len != 0 or unique.without_overlaps_period != null) return error.InvalidIndexPublicationCandidate;
        const keys = if (unique.keys.len != 0) unique.keys else blk: {
            const result = try declaration_arena.allocator().alloc(@import("../relational_index.zig").RelationalIndexKey, unique.columns.len);
            for (result, unique.columns) |*entry, column| entry.* = .{ .column = column };
            break :blk result;
        };
        const generation = (candidate.next.catalog.find(.unique, view.name) orelse return error.InvalidIndexPublicationCandidate).generation;
        const allocation = try @import("relational_integrity_catalog.zig").privateIndexAllocationFromReservation(candidate.reservation.value, inputs.digest(candidate.reservation.value), namespace.table_id);
        if (!std.mem.eql(u8, &allocation.generation, &generation)) return error.InvalidIndexPublicationCandidate;
        const tuple = try @import("relational_index_keys.zig").TuplePlan.init(alloc, after.tableSchema().*, after.physicalLayout(), keys);
        return .{ .candidate = candidate, .addition = addition, .unique = .{ .tuple = tuple, .generation = generation, .nulls_not_distinct = unique.nulls_not_distinct } };
    }
};
