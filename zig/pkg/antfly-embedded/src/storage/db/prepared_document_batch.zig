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
const persistent = @import("../persistent.zig");
const native_publication = @import("../native_publication.zig");
const docstore = @import("../docstore.zig");

pub const Plan = struct {
    alloc: Allocator,
    inputs: std.heap.ArenaAllocator,
    catalog: manager_mod.IndexManager.WritePlanSnapshotView,
    namespace: identity.Namespace,
    incarnation: u128,
    schema: ?@import("schema_registry.zig").SchemaView = null,
    rows: std.StringHashMapUnmanaged(Entry) = .empty,
    text: std.ArrayListUnmanaged(Text) = .empty,
    text_delete_keys: []const []const u8 = &.{},
    text_guards: []manager_mod.OwnedForegroundTextPublication.Guard = &.{},
    text_candidates: []*persistent.PersistentIndex.PreparedNativePublication = &.{},
    text_selected: []usize = &.{},
    const Text = struct { config: manager_mod.ForegroundTextProjection, built: manager_mod.PreparedTextMapperPublication, publication: ?*manager_mod.OwnedForegroundTextPublication = null };
    const Entry = struct { value: []const u8, template: mapper.ExtractedWrite, extracted: ?mapper.ExtractedWrite = null };

    /// Called under the short catalog lifetime fence. No borrowed DB pointer
    /// or primary snapshot survives this capture.
    pub fn capture(a: Allocator, database: anytype) !?Plan {
        var schema = database.core.acquireSchemaView();
        defer if (schema) |*view| view.release();
        if (schema) |view| if (view.storageMode() == .relational) return null;
        const catalog = try database.core.index_manager.acquireWritePlanSnapshot();
        return .{
            .alloc = a,
            .inputs = std.heap.ArenaAllocator.init(a),
            .catalog = catalog,
            .schema = if (schema) |view| view.clone() else null,
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
        for (self.text.items) |*item| {
            if (item.publication) |publication| publication.deinit();
            item.built.deinit();
        }
        self.text.deinit(self.alloc);
        self.alloc.free(self.text_delete_keys);
        self.alloc.free(self.text_guards);
        self.alloc.free(self.text_candidates);
        self.alloc.free(self.text_selected);
        self.rows.deinit(self.alloc);
        self.catalog.release();
        if (self.schema) |*schema| schema.release();
        self.inputs.deinit();
        self.* = undefined;
    }

    pub fn prepare(self: *Plan, request: types.BatchRequest) !void {
        // Preparation is optional acceleration, with explicit request-local
        // limits. Larger atomic batches retain the ordinary authoritative path.
        if (request.writes.len > 8192 or request.deletes.len > 8192) return;
        var input_bytes: usize = 0;
        for (request.writes) |write| {
            input_bytes +|= write.key.len +| write.value.len;
            if (input_bytes > 8 * 1024 * 1024) return;
        }
        for (request.deletes) |key| {
            input_bytes +|= key.len;
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
        try self.prepareForegroundText(request);
        try self.prepareAttempt();
    }

    fn prepareForegroundText(self: *Plan, request: types.BatchRequest) !void {
        // A transform's final value depends on its validated primary read set.
        // Until that owned read set is captured, only exact authored postimages
        // can take this path. The canonical journal retains all other effects.
        if (request.transforms.len != 0 or self.catalog.plan().foreground_text.len == 0 or (self.rows.count() == 0 and request.deletes.len == 0)) return;
        const a = self.alloc;
        var docs: std.ArrayListUnmanaged(mapper.MapperDoc) = .empty;
        defer docs.deinit(a);
        var delete_keys: std.ArrayListUnmanaged([]const u8) = .empty;
        defer delete_keys.deinit(a);
        var entries = self.rows.iterator();
        while (entries.next()) |entry| {
            const value = (try entry.value_ptr.template.logicalJson()) orelse return;
            try docs.append(a, .{ .key = entry.key_ptr.*, .value = value, .root = entry.value_ptr.template.prepared_text_root });
            try delete_keys.append(a, entry.key_ptr.*);
        }
        std.mem.sort(mapper.MapperDoc, docs.items, {}, struct {
            fn less(_: void, left: mapper.MapperDoc, right: mapper.MapperDoc) bool {
                return std.mem.order(u8, left.key, right.key) == .lt;
            }
        }.less);
        for (request.deletes) |key| try delete_keys.append(a, try self.inputs.allocator().dupe(u8, key));
        self.text_delete_keys = try delete_keys.toOwnedSlice(a);
        const count = self.catalog.plan().foreground_text.len;
        self.text_guards = try a.alloc(manager_mod.OwnedForegroundTextPublication.Guard, count);
        self.text_candidates = try a.alloc(*persistent.PersistentIndex.PreparedNativePublication, count);
        self.text_selected = try a.alloc(usize, count);
        for (self.catalog.plan().foreground_text) |config| {
            var projection = try config.project(a, docs.items);
            defer projection.deinit();
            var built = (try projection.prepareDetached(a, 0, projection.input_end)) orelse continue;
            errdefer built.deinit();
            try self.text.append(a, .{ .config = config, .built = built });
        }
    }

    /// Called under a short current-runtime read/lifetime fence after posting
    /// construction. This captures native aliases and exact metadata witnesses,
    /// then releases every mutable index reference before writer admission.
    pub fn stageForegroundText(self: *Plan, database: anytype) !void {
        if (self.text.items.len == 0 or !try self.matches(database)) return;
        const previous = database.core.store.lastReplaySequence(0);
        var current = try database.core.index_manager.acquireWritePlanSnapshot();
        defer current.release();
        for (self.text.items) |*item| {
            if (item.publication) |publication| publication.deinit();
            item.publication = null;
            const config = for (current.plan().foreground_text) |config| {
                if (item.config.matches(config)) break config;
            } else continue;
            item.built.stageNative(config.stager) catch |err| switch (err) {
                error.EnrichmentWaitCanceled, error.FileBusy => continue,
                else => return err,
            };
            item.publication = database.core.index_manager.prepareOwnedForegroundTextPublication(database.core.store, item.config, &item.built, self.text_delete_keys, previous) catch |err| switch (err) {
                error.EnrichmentWaitCanceled, error.FileBusy => null,
                else => return err,
            };
        }
    }

    fn matchesEffects(self: *const Plan, request: types.BatchRequest) bool {
        var count: usize = 0;
        for (request.writes, 0..) |write, i| {
            for (request.deletes) |key| {
                if (std.mem.eql(u8, key, write.key)) break;
            } else {
                for (request.writes[i + 1 ..]) |later| {
                    if (std.mem.eql(u8, later.key, write.key)) break;
                } else {
                    const expected = self.rows.get(write.key) orelse return false;
                    if (!std.mem.eql(u8, expected.value, write.value)) return false;
                    count += 1;
                }
            }
        }
        if (count != self.rows.count()) return false;
        // Extra captured keys are replacement writes; every effective explicit
        // delete must also be covered by this publication's deletion plan.
        for (request.deletes) |key| {
            for (self.text_delete_keys) |captured| {
                if (std.mem.eql(u8, key, captured)) break;
            } else return false;
        }
        for (self.text_delete_keys) |key| {
            if (self.rows.contains(key)) continue;
            for (request.deletes) |actual| {
                if (std.mem.eql(u8, key, actual)) break;
            } else return false;
        }
        return true;
    }

    /// Bind identities from this primary transaction's authoritative postimage.
    /// One native commit publishes all eligible indexes and the primary batch.
    /// Every bound handle is destroyed before releasing its live target fence.
    pub fn commitForegroundText(self: *Plan, manager: *manager_mod.IndexManager, sequence: u64, request: types.BatchRequest, store: *docstore.DocStore, identity_writes: []const docstore.KVPair, primary: native_publication.Committer) !bool {
        if (self.text.items.len == 0 or !self.matchesEffects(request)) return false;
        var selected: usize = 0;
        defer {
            for (self.text_selected[0..selected], self.text_guards[0..selected]) |index, *guard| {
                self.text.items[index].publication.?.deinit();
                self.text.items[index].publication = null;
                guard.release();
            }
        }
        for (self.text.items, 0..) |*item, index| {
            const publication = item.publication orelse continue;
            if (publication.sequence != sequence) continue;
            const guard = (try publication.bind(manager)) orelse continue;
            self.text_selected[selected] = index;
            self.text_guards[selected] = guard;
            self.text_candidates[selected] = publication.native;
            selected += 1;
        }
        if (selected == 0) return false;
        var read = try store.beginReadTxn();
        defer read.abort();
        const IdentityView = struct {
            read: *@TypeOf(read),
            writes: []const docstore.KVPair,
            pub fn get(view: *@This(), key: []const u8) ![]const u8 {
                var index = view.writes.len;
                while (index != 0) {
                    index -= 1;
                    const write = view.writes[index];
                    if (std.mem.eql(u8, write.key, key)) return write.value;
                }
                return view.read.get(key);
            }
        };
        var identities: IdentityView = .{ .read = &read, .writes = identity_writes };
        for (self.text_selected[0..selected]) |index| {
            for (self.text.items[index].built.staged) |*segment| {
                const ordinals = if (segment.ordinals) |*value| value else return error.InvalidDocIdentity;
                for (ordinals.keys, ordinals.assigned) |key, *assigned| assigned.* = (try identity.lookupOrdinalTxn(self.alloc, &identities, key)) orelse return error.InvalidDocIdentity;
                try ordinals.bind(ordinals.assigned);
            }
        }
        try persistent.PersistentIndex.commitNativePublications(self.text_candidates[0..selected], primary);
        for (self.text_selected[0..selected], self.text_guards[0..selected]) |index, *guard| self.text.items[index].publication.?.activate(guard);
        return true;
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
        // This request was bound to a specific namespace at capture. Falling
        // back on a replacement table/database would silently retarget it.
        if (!std.meta.eql(self.namespace, database.core.identity_namespace) or self.incarnation != database.root_incarnation) return error.TableNotFound;
        var schema = database.core.acquireSchemaView();
        defer if (schema) |*view| view.release();
        if (schema) |view| if (view.storageMode() == .relational) return false;
        if ((self.schema == null) != (schema == null)) return false;
        if (self.schema) |captured| {
            const actual = schema.?;
            if (captured.epoch != actual.epoch and !try @import("../schema.zig").schemasEqual(self.alloc, captured.tableSchema().*, actual.tableSchema().*)) return false;
        }
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
