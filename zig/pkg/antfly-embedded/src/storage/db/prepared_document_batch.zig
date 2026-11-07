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
const mutation_effects = @import("mutation_effects.zig");
const owned_transform_inputs = @import("owned_transform_inputs.zig");

pub const Plan = struct {
    alloc: Allocator,
    inputs: std.heap.ArenaAllocator,
    catalog: manager_mod.IndexManager.WritePlanSnapshotView,
    namespace: identity.Namespace,
    incarnation: u128,
    schema: ?@import("schema_registry.zig").SchemaView = null,
    rows: std.StringHashMapUnmanaged(Entry) = .empty,
    value_authorities: @import("../document_mutation_revision.zig").PreparedValues = .{},
    text: std.ArrayListUnmanaged(Text) = .empty,
    text_delete_keys: []const []const u8 = &.{},
    text_guards: []manager_mod.OwnedForegroundTextPublication.Guard = &.{},
    text_candidates: []*persistent.PersistentIndex.PreparedNativePublication = &.{},
    text_selected: []usize = &.{},
    explicit_deletes: std.StringHashMapUnmanaged(void) = .empty,
    effects_seen: std.StringHashMapUnmanaged(void) = .empty,
    ordinal_postimages: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    effect_request: ?mutation_effects.OwnedRequest = null,
    transform_base_keys: [][]const u8 = &.{},
    transform_inputs: ?owned_transform_inputs.Inputs = null,
    resolved_effects: ?mutation_effects.CoalescedKeyValueRequest(types.BatchWrite) = null,
    transform_capture_declined: bool = false,
    needs_reprepare: bool = false,
    const Text = struct { config: manager_mod.ForegroundTextProjection, built: manager_mod.PreparedTextMapperPublication, publication: ?*manager_mod.OwnedForegroundTextPublication = null };
    const Entry = struct { value: []const u8, stored_value: ?[]u8 = null, ordinal_key: []const u8 = &.{}, template: mapper.ExtractedWrite, extracted: ?mapper.ExtractedWrite = null };

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
            if (entry.stored_value) |body| self.alloc.free(body);
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
        self.explicit_deletes.deinit(self.alloc);
        self.effects_seen.deinit(self.alloc);
        self.ordinal_postimages.deinit(self.alloc);
        self.rows.deinit(self.alloc);
        self.value_authorities.deinit(self.alloc);
        if (self.resolved_effects) |*effects| effects.deinit(self.alloc);
        if (self.transform_inputs) |*inputs| inputs.deinit();
        self.alloc.free(self.transform_base_keys);
        if (self.effect_request) |*request| request.deinit();
        self.catalog.release();
        if (self.schema) |*schema| schema.release();
        self.inputs.deinit();
        self.* = undefined;
    }

    pub fn prepare(self: *Plan, request: types.BatchRequest) !void {
        if (request.transforms.len != 0 and !self.transform_capture_declined) {
            if (self.effect_request != null) return;
            if (request.writes.len > 8192 or request.deletes.len > 8192 or request.transforms.len > 8192) return;
            var bytes: usize = 0;
            var operations: usize = 0;
            for (request.writes) |write| {
                if (write.json_null_fields.len != 0) return;
                bytes +|= write.key.len +| write.value.len;
            }
            for (request.deletes) |key| bytes +|= key.len;
            for (request.transforms) |transform| {
                if (transform.key.len != 0 and transform.key[0] == 0) return;
                bytes +|= transform.key.len;
                operations +|= transform.operations.len;
                for (transform.operations) |operation| bytes +|= operation.path.len +| if (operation.value_json) |value| value.len else 0;
            }
            if (bytes > 8 * 1024 * 1024 or operations > 8192) return;
            self.effect_request = try mutation_effects.OwnedRequest.init(self.alloc, request.writes, request.deletes, request.transforms);
            self.transform_base_keys = try owned_transform_inputs.requiredBaseKeys(self.alloc, &self.effect_request.?);
            if (self.transform_base_keys.len == 0) {
                self.transform_inputs = owned_transform_inputs.Inputs.empty(self.alloc);
                try self.finishOwnedTransforms(request);
            }
            return;
        }
        try self.preparePostimages(request);
    }

    fn preparePostimages(self: *Plan, request: types.BatchRequest) !void {
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
            const stored_value: ?[]u8 = if (row.cleaned_value != null and plan.vector_store_fields.len != 0)
                (try mapper.stripTopLevelFieldsAlloc(a, row.cleaned_value.?, plan.vector_store_fields)) orelse try a.dupe(u8, "{}")
            else
                null;
            errdefer if (stored_value) |body| a.free(body);
            if (row.cleaned_value) |body| {
                const physical = try @import("../internal_keys.zig").documentKeyAlloc(a, key);
                defer a.free(physical);
                try self.value_authorities.add(a, physical, stored_value orelse body);
            }
            const entry = try self.rows.getOrPut(a, key);
            std.debug.assert(!entry.found_existing);
            entry.value_ptr.* = .{ .value = value, .stored_value = stored_value, .template = row };
        }
        try self.prepareForegroundText(request);
        try self.prepareAttempt();
    }

    pub fn needsTransformCapture(self: *const Plan) bool {
        return self.effect_request != null and self.transform_inputs == null and !self.transform_capture_declined;
    }

    /// The caller holds only the short current namespace/lifetime fence. No
    /// read transaction, runtime or borrowed body escapes this bounded copy.
    pub fn captureTransformInputs(self: *Plan, database: anytype) !void {
        if (!self.needsTransformCapture()) return;
        if (!try self.matches(database)) {
            self.transform_capture_declined = true;
            return;
        }
        self.transform_inputs = owned_transform_inputs.Inputs.capture(self.alloc, database.core.store, self.transform_base_keys, 8 * 1024 * 1024) catch |err| switch (err) {
            error.Unsupported, error.OwnedPreparationTooLarge, error.ResourceBudgetExceeded, error.OutOfMemory => blk: {
                self.transform_capture_declined = true;
                break :blk null;
            },
            else => return err,
        };
    }

    /// Resolve, parse, map and tokenize the owned postimages after capture has
    /// released admission. The authoritative commit later validates their bases.
    pub fn finishOwnedTransforms(self: *Plan, request: types.BatchRequest) !void {
        if (self.effect_request == null or self.resolved_effects != null) return;
        const inputs = if (self.transform_inputs) |*value| value else {
            if (self.transform_capture_declined) try self.preparePostimages(request);
            return;
        };
        inputs.seal();
        const owned = &self.effect_request.?;
        self.resolved_effects = try mutation_effects.coalesceCapturedKeyValueRequest(self.alloc, types.BatchWrite, owned.writes, owned.deletes, owned.transforms, &inputs.snapshot);
        var resolved = request;
        resolved.writes = self.resolved_effects.?.writes;
        resolved.deletes = self.resolved_effects.?.deletes;
        resolved.transforms = &.{};
        try self.preparePostimages(resolved);
    }

    pub fn resolvedFor(self: *Plan, request: types.BatchRequest) ?*const mutation_effects.CoalescedKeyValueRequest(types.BatchWrite) {
        const owned = if (self.effect_request) |*value| value else return null;
        if (!owned.matches(request.writes, request.deletes, request.transforms)) return null;
        return if (self.resolved_effects) |*effects| effects else null;
    }

    pub fn validateOwnedTransforms(self: *Plan, store: *docstore.DocStore) !void {
        const inputs = if (self.transform_inputs) |*value| value else return error.InvalidDerivedApplyState;
        inputs.validate(store) catch |err| {
            if (err == error.OwnedPreparationStale) self.needs_reprepare = true;
            return err;
        };
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
            entry.value_ptr.ordinal_key = try @import("../internal_keys.zig").identityDocToOrdinalKeyAlloc(self.inputs.allocator(), entry.key_ptr.*);
            try self.ordinal_postimages.put(a, entry.value_ptr.ordinal_key, null);
            const value = (try entry.value_ptr.template.logicalJson()) orelse return;
            try docs.append(a, .{ .key = entry.key_ptr.*, .value = value, .root = entry.value_ptr.template.prepared_text_root });
            try delete_keys.append(a, entry.key_ptr.*);
        }
        std.mem.sort(mapper.MapperDoc, docs.items, {}, struct {
            fn less(_: void, left: mapper.MapperDoc, right: mapper.MapperDoc) bool {
                return std.mem.order(u8, left.key, right.key) == .lt;
            }
        }.less);
        for (request.deletes) |key| {
            const owned = try self.inputs.allocator().dupe(u8, key);
            try delete_keys.append(a, owned);
            try self.explicit_deletes.put(a, owned, {});
        }
        // Final validation must neither allocate nor scan the remaining input
        // for every row. These bounds cover only captured keys; unknown keys
        // are rejected before inserting into the scratch set.
        try self.effects_seen.ensureTotalCapacity(a, @intCast(self.rows.count() + self.explicit_deletes.count()));
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

    /// Copy resident artifacts and prepare their authoritative ordinal columns
    /// using captured resource/file identities. No live DB or Registry
    /// callback is needed. Large artifacts use independent bounded commands.
    pub fn stageIndependentForegroundText(self: *Plan) !void {
        var budget: @import("../immutable_artifact.zig").AdmissionBudget = .{ .remaining_ns = 5 * std.time.ns_per_s };
        return self.stageIndependentForegroundTextWithBudget(&budget);
    }

    pub fn stageIndependentForegroundTextWithBudget(self: *Plan, budget: *@import("../immutable_artifact.zig").AdmissionBudget) !void {
        for (self.text.items) |*item| item.built.stageDetachedNativeWithBudget(item.config.stager, budget) catch |err| switch (err) {
            error.Unsupported, error.ResourceBudgetExceeded, error.OutOfMemory => {},
            else => return err,
        };
    }

    /// Capture staged artifacts, finite metadata witnesses and immutable index
    /// inputs under the current-runtime lifetime fence. The caller finishes
    /// replacement preparation after releasing this fence.
    pub fn captureForegroundText(self: *Plan, database: anytype) !void {
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
                error.EnrichmentWaitCanceled, error.EnrichmentSourceChanged, error.FileBusy, error.StaleArtifactConstruction, error.Unsupported => continue,
                else => return err,
            };
            item.publication = database.core.index_manager.captureOwnedForegroundTextPublication(database.core.store, item.config, &item.built, self.text_delete_keys, previous) catch |err| switch (err) {
                error.EnrichmentWaitCanceled, error.EnrichmentSourceChanged, error.FileBusy, error.StaleArtifactConstruction, error.Unsupported => null,
                else => return err,
            };
        }
    }

    /// Finish private replacements after releasing current-runtime admission.
    pub fn finishForegroundText(self: *Plan) !void {
        for (self.text.items) |*item| {
            const publication = item.publication orelse continue;
            publication.finish(&item.built, self.text_delete_keys) catch |err| switch (err) {
                error.EnrichmentWaitCanceled, error.EnrichmentSourceChanged, error.FileBusy, error.StaleArtifactConstruction, error.Unsupported => {
                    publication.deinit();
                    item.publication = null;
                },
                else => return err,
            };
        }
    }

    /// Convenience for callers already responsible for their runtime lifetime.
    pub fn stageForegroundText(self: *Plan, database: anytype) !void {
        try self.captureForegroundText(database);
        try self.finishForegroundText();
    }

    /// Compare coalesced authored effects in linear expected time without
    /// allocation. Final writes win, then explicit deletes, as in preparation.
    pub fn matchesEffects(self: *Plan, request: types.BatchRequest) bool {
        self.effects_seen.clearRetainingCapacity();
        var deletes: usize = 0;
        for (request.deletes) |key| {
            if (!self.explicit_deletes.contains(key)) return false;
            const entry = self.effects_seen.getOrPutAssumeCapacity(key);
            if (!entry.found_existing) deletes += 1;
        }
        if (deletes != self.explicit_deletes.count()) return false;
        var count: usize = 0;
        var cursor = request.writes.len;
        while (cursor != 0) {
            cursor -= 1;
            const write = request.writes[cursor];
            if (self.effects_seen.contains(write.key)) continue;
            const expected = self.rows.get(write.key) orelse return false;
            if (!std.mem.eql(u8, expected.value, write.value)) return false;
            self.effects_seen.putAssumeCapacity(write.key, {});
            count += 1;
        }
        return count == self.rows.count();
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
        const previous = store.lastReplaySequence(0);
        if (sequence <= previous) return false;
        for (self.text.items, 0..) |*item, index| {
            const publication = item.publication orelse continue;
            if (publication.previous_sequence != previous) continue;
            const guard = (try publication.bind(manager)) orelse continue;
            self.text_selected[selected] = index;
            self.text_guards[selected] = guard;
            self.text_candidates[selected] = publication.native.?;
            selected += 1;
            try publication.bindSequence(sequence);
        }
        if (selected == 0) return false;
        var read = try store.beginReadTxn();
        defer read.abort();
        // Only captured document-to-ordinal keys participate. The map and
        // encoded keys were allocated before admission. Scanning metadata once
        // avoids an O(documents * identity writes) overlay lookup in the gate.
        var values = self.ordinal_postimages.valueIterator();
        while (values.next()) |value| value.* = null;
        for (identity_writes) |write| if (self.ordinal_postimages.getPtr(write.key)) |value| {
            value.* = write.value;
        };
        for (self.text_selected[0..selected]) |index| {
            for (self.text.items[index].built.staged) |*segment| {
                const ordinals = if (segment.ordinals) |*value| value else return error.InvalidDocIdentity;
                for (ordinals.keys, ordinals.assigned) |key, *assigned| {
                    const entry = self.rows.get(key) orelse return error.InvalidDocIdentity;
                    const raw = self.ordinal_postimages.get(entry.ordinal_key).? orelse try read.get(entry.ordinal_key);
                    if (raw.len != @sizeOf(u32)) return error.InvalidDocIdentity;
                    assigned.* = std.mem.readInt(u32, raw[0..4], .big);
                }
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

    /// Exact stored postimage, including catalog-selected vector stripping.
    /// Borrowed only through the synchronous authoritative commit.
    pub fn storedValueFor(self: *const Plan, key: []const u8, value: []const u8) ?[]const u8 {
        const entry = self.rows.get(key) orelse return null;
        if (!std.mem.eql(u8, entry.value, value)) return null;
        return entry.stored_value orelse entry.template.cleaned_value;
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
