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

//! Owned document effects prepared without a live DB or writer admission.
//! Mutable primary decisions (predicates, identities, timestamps, transform
//! bases) remain in the atomic commit. A transformed value can consume a plan
//! only when it exactly equals the captured input.
const std = @import("std");
const mapper = @import("document_mapper.zig");
const manager_mod = @import("catalog/index_manager.zig");
const types = @import("types.zig");
const identity = @import("doc_identity.zig");
const graph_fields = @import("graph_field_plan.zig");
const Allocator = std.mem.Allocator;

pub const Plan = struct {
    alloc: Allocator,
    inputs: std.heap.ArenaAllocator,
    catalog: manager_mod.IndexManager.WritePlanSnapshotView,
    namespace: identity.Namespace,
    incarnation: u128,
    rows: std.StringHashMapUnmanaged(Entry) = .empty,
    const Entry = struct { value: []const u8, template: mapper.ExtractedWrite, extracted: ?mapper.ExtractedWrite = null };

    /// Called under the short catalog lifetime fence. No borrowed DB pointer
    /// or primary snapshot survives this capture.
    pub fn capture(a: Allocator, database: anytype) !?Plan {
        var schema = database.core.acquireSchemaView();
        defer if (schema) |*view| view.release();
        if (schema) |view| if (view.storageMode() == .relational) return null;
        return .{
            .alloc = a,
            .inputs = std.heap.ArenaAllocator.init(a),
            .catalog = try database.core.index_manager.acquireWritePlanSnapshot(),
            .namespace = database.core.identity_namespace,
            .incarnation = database.root_incarnation,
        };
    }

    pub fn deinit(self: *Plan) void {
        var entries = self.rows.valueIterator();
        while (entries.next()) |entry| {
            if (entry.extracted) |*row| row.deinit(self.alloc);
            entry.template.deinit(self.alloc);
        }
        self.rows.deinit(self.alloc);
        self.catalog.release();
        self.inputs.deinit();
        self.* = undefined;
    }

    pub fn prepare(self: *Plan, request: types.BatchRequest) !void {
        // Preparation is optional acceleration, with explicit request-local
        // limits. Larger atomic batches retain the ordinary authoritative path.
        if (request.writes.len > 8192) return;
        var input_bytes: usize = 0;
        for (request.writes) |write| {
            input_bytes +|= write.key.len +| write.value.len;
            if (input_bytes > 8 * 1024 * 1024) return;
        }
        const a = self.alloc;
        const owned = self.inputs.allocator();
        const plan = self.catalog.plan();
        const parse_fields = plan.graph_fields.len != 0 or plan.dense_fields.len != 0 or plan.sparse_fields.len != 0;
        var deleted: std.StringHashMapUnmanaged(void) = .empty;
        defer deleted.deinit(a);
        for (request.deletes) |key| try deleted.put(a, key, {});
        var ordinal = request.writes.len;
        while (ordinal != 0) {
            ordinal -= 1;
            const write = request.writes[ordinal];
            // Match atomic request coalescing: final writes win, then deletes.
            // Do not reject an invalid JSON write discarded by that ordering.
            if (self.rows.contains(write.key) or deleted.contains(write.key)) continue;
            // Control and typed-row effects retain their authoritative paths.
            if ((write.key.len != 0 and write.key[0] == 0) or write.json_null_fields.len != 0) continue;
            const key = try owned.dupe(u8, write.key);
            const value = try owned.dupe(u8, write.value);
            var row = try mapper.extractWrite(a, key, value);
            errdefer row.deinit(a);
            if (parse_fields) {
                var parsed = try std.json.parseFromSlice(std.json.Value, a, value, .{});
                defer parsed.deinit();
                try graph_fields.fromSnapshot(plan.graph_fields, a, key, parsed.value, &row);
                try plan.appendIndexFieldEmbeddingsFromParsedToExtractedWrite(a, key, parsed.value, &row);
            }
            const entry = try self.rows.getOrPut(a, key);
            std.debug.assert(!entry.found_existing);
            entry.value_ptr.* = .{ .value = value, .template = row };
        }
        try self.prepareAttempt();
    }

    /// Replenish mutable commit copies outside admission after a pre-commit
    /// refusal. Expensive parsing and field projection stay in the immutable
    /// template, even when capacity contention causes repeated attempts.
    pub fn prepareAttempt(self: *Plan) !void {
        var entries = self.rows.valueIterator();
        while (entries.next()) |entry| {
            if (entry.extracted == null) entry.extracted = try entry.template.clone(self.alloc);
        }
    }

    /// Exact durable catalog contents, namespace and incarnation authorize
    /// reuse across a serving-runtime refresh. Process-local counters alone
    /// cannot establish equality between two independently opened runtimes.
    pub fn matches(self: *const Plan, database: anytype) !bool {
        if (!std.meta.eql(self.namespace, database.core.identity_namespace) or self.incarnation != database.root_incarnation) return false;
        var schema = database.core.acquireSchemaView();
        defer if (schema) |*view| view.release();
        if (schema) |view| if (view.storageMode() == .relational) return false;
        var current = try database.core.index_manager.acquireWritePlanSnapshot();
        defer current.release();
        const expected = self.catalog.plan().artifact_catalogs orelse return false;
        return current.plan().matchesArtifactInventory(expected);
    }

    /// Transfer allocations into the ordinary commit cleanup domain. Plans
    /// from another allocator are never consumed. Failed optimistic attempts
    /// may rebuild using the existing authoritative preparation path.
    pub fn take(self: *Plan, a: Allocator, key: []const u8, value: []const u8) ?mapper.ExtractedWrite {
        if (a.ptr != self.alloc.ptr or a.vtable != self.alloc.vtable) return null;
        const entry = self.rows.getPtr(key) orelse return null;
        if (!std.mem.eql(u8, entry.value, value)) return null;
        const row = entry.extracted orelse return null;
        entry.extracted = null;
        return row;
    }
};
