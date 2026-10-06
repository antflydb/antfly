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

//! Request preparation, pinned planning, row scratch and generated memo work.
//! Receivers borrow local resources; lifetime and scheduling remain with DB.

const ant_json = @import("antfly-json");
const appendOwnedKey = @import("owned_keys.zig").appendOwnedKey;
const build_options = @import("build_options");
const builtin = @import("builtin");
const chunker_mod = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("enrichment/chunker_stub.zig")
else
    @import("enrichment/chunker.zig");
const db_query_graph = @import("query/graph_exec.zig");
const derived_types = @import("derived/derived_types.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const execution_resources = @import("execution_resources.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");
const json_helpers = @import("../../api/json_helpers.zig");
const mapper = @import("document_mapper.zig");
const merge_state_mod = @import("merge_state.zig");
const public_table_schema = @import("../../schema/mod.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_store = @import("relational_store.zig");
const schema_mod = @import("../schema.zig");
const schema_registry_mod = @import("schema_registry.zig");
const std = @import("std");
const transactions_mod = @import("../transactions.zig");
const transform_mod = @import("transform.zig");
const types = @import("types.zig");

pub fn ImplementationFor(comptime S: type, comptime D: type) type {
    return struct {
        const Implementation = S;
        const Allocator = execution_resources.Allocator;
        const ChunkCacheEntry = execution_resources.ChunkCacheEntry;
        const ChunkEmbeddingSource = execution_resources.ChunkEmbeddingSource;
        const GeneratedBatchWritePlan = execution_resources.GeneratedBatchWritePlan;
        const GeneratedDenseMemoJob = execution_resources.GeneratedDenseMemoJob;
        const GeneratedEmbeddingMemo = execution_resources.GeneratedEmbeddingMemo;
        const GeneratedPrecomputeMode = execution_resources.GeneratedPrecomputeMode;
        const GeneratedSparseMemoJob = execution_resources.GeneratedSparseMemoJob;
        const OverwriteProbeEntry = execution_resources.OverwriteProbeEntry;
        const PendingArtifactWriteIndex = execution_resources.PendingArtifactWriteIndex;
        const PendingChunkDeleteIndex = execution_resources.PendingChunkDeleteIndex;
        const PrecomputeAssetProducerBatchItem = execution_resources.PrecomputeAssetProducerBatchItem;
        const PrecomputedCoverageCandidate = execution_resources.PrecomputedCoverageCandidate;
        const PrecomputedCoverageOutcome = execution_resources.PrecomputedCoverageOutcome;
        const PrecomputedGeneratedBatch = execution_resources.PrecomputedGeneratedBatch;
        const PreparedRowAllocator = execution_resources.PreparedRowAllocator;
        const clearChunkEmbeddingSourceList = execution_resources.clearChunkEmbeddingSourceList;
        const collectChunkEmbeddingSourcesFromStore = D.collectChunkEmbeddingSourcesFromStore;
        const collectChunkEmbeddingSourcesFromWrites = D.collectChunkEmbeddingSourcesFromWrites;
        const generated_embed_default_batch_bytes = D.generated_embed_default_batch_bytes;
        const generated_embed_default_batch_items = D.generated_embed_default_batch_items;
        const Execution = S.Execution;
        const InlineChunkEmbeddingCleanup = S.InlineChunkEmbeddingCleanup;
        const appendDerivedSparseEmbeddingForConsumers = S.appendDerivedSparseEmbeddingForConsumers;
        const appendEmbeddingArtifactWrite = S.appendEmbeddingArtifactWrite;
        const appendGeneratedEnrichmentRef = S.appendGeneratedEnrichmentRef;
        const appendGraphTransformDelete = S.appendGraphTransformDelete;
        const appendGraphTransformWrite = S.appendGraphTransformWrite;
        const appendPrecomputedArtifactCoverageOutcomes = S.appendPrecomputedArtifactCoverageOutcomes;
        const appendPrecomputedCoverageCandidate = S.appendPrecomputedCoverageCandidate;
        const appendPrecomputedEmbeddingCoverageOutcomes = S.appendPrecomputedEmbeddingCoverageOutcomes;
        const appendSparseEmbeddingArtifactWrite = S.appendSparseEmbeddingArtifactWrite;
        const appendStalePrecomputedChunkEmbeddingDeletes = S.appendStalePrecomputedChunkEmbeddingDeletes;
        const chunkEmbeddingSourcesForRequest = S.chunkEmbeddingSourcesForRequest;
        const clearPrecomputeAssetProducerBatchItems = S.clearPrecomputeAssetProducerBatchItems;
        const computeAssetRequestDerived = S.computeAssetRequestDerived;
        const computeChunkRequestDerived = S.computeChunkRequestDerived;
        const computeDenseRequestDerived = S.computeDenseRequestDerived;
        const computeSparseRequestDerived = S.computeSparseRequestDerived;
        const containsName = S.containsName;
        const deinitOwnedGraphEdgeDelete = S.deinitOwnedGraphEdgeDelete;
        const deinitOwnedGraphEdgeWrite = S.deinitOwnedGraphEdgeWrite;
        const embeddingArtifactKeyForBaseAlloc = S.embeddingArtifactKeyForBaseAlloc;
        const encodeStoreLookupKeyWithPinnedSchemaAlloc = S.encodeStoreLookupKeyWithPinnedSchemaAlloc;
        const flushPrecomputeAssetProducerBatch = S.flushPrecomputeAssetProducerBatch;
        const generatedConsumerSetsEqual = S.generatedConsumerSetsEqual;
        const getenv = S.getenv;
        const indexPendingArtifactWrites = S.indexPendingArtifactWrites;
        const isMetadataKey = S.isMetadataKey;
        const precomputedEmbeddingCoverageOutcome = S.precomputedEmbeddingCoverageOutcome;
        const renderSourceTemplateText = S.renderSourceTemplateText;
        const requestArtifactName = S.requestArtifactName;
        const requestEmbeddingName = S.requestEmbeddingName;
        const requestUsesChunkSource = S.requestUsesChunkSource;
        const requestUsesPinnedMaterializedChunkArtifact = S.requestUsesPinnedMaterializedChunkArtifact;
        const sliceContainsDocKeyPrefix = S.sliceContainsDocKeyPrefix;
        const sliceContainsKey = S.sliceContainsKey;
        const sliceContainsKeyPrefix = S.sliceContainsKeyPrefix;
        const sliceContainsWriteKey = S.sliceContainsWriteKey;
        const sliceContainsWriteKeyPrefix = S.sliceContainsWriteKeyPrefix;
        const splitShadowRequiresMaterializedDerivedBatch = S.splitShadowRequiresMaterializedDerivedBatch;
        const storedOrPendingEmbeddingSourceHash = S.storedOrPendingEmbeddingSourceHash;
        const validateDocumentExtractionInlineSourcesSnapshotParsed = S.validateDocumentExtractionInlineSourcesSnapshotParsed;
        pub fn CoalescedKeyValueRequest(comptime T: type) type {
            return struct {
                pub const Entry = struct {
                    json_null_fields: []const []const u8 = &.{},
                    key: []const u8,
                    value: ?[]const u8 = null,
                    kind: enum { write, delete },
                    owned_key: bool = false,
                    owned_value: bool = false,
                };

                entries: []Entry = &.{},
                writes: []T = &.{},
                deletes: [][]const u8 = &.{},
                graph_writes: std.ArrayListUnmanaged(types.GraphEdgeWrite) = .empty,
                graph_deletes: std.ArrayListUnmanaged(types.GraphEdgeDelete) = .empty,

                pub fn deinit(self: *@This(), alloc: Allocator) void {
                    for (self.entries) |entry| {
                        if (entry.owned_key) alloc.free(@constCast(entry.key));
                        if (entry.owned_value) alloc.free(@constCast(entry.value.?));
                    }
                    if (self.entries.len > 0) alloc.free(self.entries);
                    if (self.writes.len > 0) alloc.free(self.writes);
                    if (self.deletes.len > 0) alloc.free(self.deletes);
                    for (self.graph_writes.items) |*write| deinitOwnedGraphEdgeWrite(alloc, write);
                    self.graph_writes.deinit(alloc);
                    for (self.graph_deletes.items) |*delete| deinitOwnedGraphEdgeDelete(alloc, delete);
                    self.graph_deletes.deinit(alloc);
                    self.* = .{};
                }
            };
        }

        pub const GeneratedWriteReadSnapshot = struct {
            pub const revisions = @import("../document_mutation_revision.zig");
            pub const Entry = struct { key: revisions.Key, revision: u64 };
            entries: []Entry = &.{},
            pub fn deinit(self: *@This(), alloc: Allocator) void {
                alloc.free(self.entries);
                self.* = .{};
            }
        };

        pub const PreparedMergeArtifacts = struct {
            effects: []const @import("merge_page_contract.zig").IntegrityEffect,
            receiver_binding: ?@import("artifact_inventory.zig").Binding = null,
            arena: ?std.heap.ArenaAllocator = null,

            pub fn deinit(self: *@This()) void {
                if (self.arena) |*arena| arena.deinit();
            }
        };

        pub const TransformReadSnapshot = struct {
            pub const Entry = struct {
                key: []const u8,
                value: ?[]u8,
                expected_version: u64,
            };

            entries: []Entry = &.{},
            positions: std.StringHashMapUnmanaged(usize) = .empty,

            pub fn deinit(self: *@This(), alloc: Allocator) void {
                for (self.entries) |entry| if (entry.value) |value| alloc.free(value);
                if (self.entries.len > 0) alloc.free(self.entries);
                self.positions.deinit(alloc);
                self.* = .{};
            }

            pub fn valueFor(self: *const @This(), key: []const u8) ?[]const u8 {
                const index = self.positions.get(key) orelse return null;
                return self.entries[index].value;
            }
        };

        pub fn captureGeneratedWriteReadSnapshot(self: anytype, alloc: Allocator, writes: []const types.BatchWrite) !GeneratedWriteReadSnapshot {
            const revisions = GeneratedWriteReadSnapshot.revisions;
            const entries = try alloc.alloc(GeneratedWriteReadSnapshot.Entry, writes.len);
            errdefer alloc.free(entries);
            var txn = try self.core.store.beginReadTxn();
            defer txn.abort();
            for (writes, entries) |write, *entry| {
                const key = revisions.keyForPhysical(write.key) orelse try revisions.keyForDocumentAlloc(alloc, write.key);
                entry.* = .{ .key = key, .revision = try revisions.load(&txn, key) };
            }
            return .{ .entries = entries };
        }

        pub fn captureTransformReadSnapshot(
            self: anytype,
            alloc: Allocator,
            comptime T: type,
            writes: []const T,
            deletes: []const []const u8,
            transforms: []const types.DocumentTransform,
        ) !TransformReadSnapshot {
            var result = TransformReadSnapshot{};
            errdefer result.deinit(alloc);
            if (transforms.len == 0) return result;

            // Only transforms whose first base comes from storage need a read-set
            // predicate. Writes/deletes and an earlier transform establish an
            // entirely request-local base.
            var request_keys = std.StringHashMapUnmanaged(void).empty;
            defer request_keys.deinit(alloc);
            for (writes) |write| try request_keys.put(alloc, write.key, {});
            for (deletes) |key| try request_keys.put(alloc, key, {});

            var keys = std.ArrayListUnmanaged([]const u8).empty;
            defer keys.deinit(alloc);
            for (transforms) |transform| {
                if (request_keys.contains(transform.key)) continue;
                try request_keys.put(alloc, transform.key, {});
                try keys.append(alloc, transform.key);
            }
            if (keys.items.len == 0) return result;

            // Read values and version timestamps from the same MVCC snapshot. This
            // replaces two point transactions per transform and allows transform
            // parsing/expansion to proceed without holding the DB-wide apply lock.
            var read_txn = try self.core.store.beginReadTxn();
            defer read_txn.abort();
            var schema_view = self.core.acquireSchemaView();
            defer if (schema_view) |*view| view.release();
            const entries = try alloc.alloc(TransformReadSnapshot.Entry, keys.items.len);
            var initialized: usize = 0;
            errdefer {
                for (entries[0..initialized]) |entry| if (entry.value) |value| alloc.free(value);
                alloc.free(entries);
            }
            try result.positions.ensureTotalCapacity(alloc, std.math.cast(u32, keys.items.len) orelse
                return error.InvalidBatchRequest);
            for (keys.items, 0..) |key, index| {
                const store_key = try encodeStoreLookupKeyWithPinnedSchemaAlloc(self, alloc, key, schema_view);
                defer alloc.free(store_key);
                const raw = read_txn.get(store_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                const value = if (raw) |stored| blk: {
                    if (!internal_keys.isRelationalRowKey(store_key))
                        break :blk try alloc.dupe(u8, stored);
                    const version = try relational_store.rowSchemaVersion(stored);
                    if (schema_view) |view| if (view.version() == version) {
                        const row = if (self.core.store.valuesAreAuthenticated())
                            try relational_row_codec.ordinalRowViewTrusted(
                                stored,
                                view.tableSchema().*,
                                view.physicalLayout(),
                            )
                        else
                            try relational_row_codec.ordinalRowView(
                                stored,
                                view.tableSchema().*,
                                view.physicalLayout(),
                            );
                        break :blk try row.reconstructValueAlloc(alloc);
                    };
                    var historical = (try self.core.acquireSchemaVersionView(version)) orelse
                        return error.UnknownSchemaVersion;
                    defer historical.release();
                    const row = if (self.core.store.valuesAreAuthenticated())
                        try relational_row_codec.ordinalRowViewTrusted(
                            stored,
                            historical.tableSchema().*,
                            historical.physicalLayout(),
                        )
                    else
                        try relational_row_codec.ordinalRowView(
                            stored,
                            historical.tableSchema().*,
                            historical.physicalLayout(),
                        );
                    break :blk try row.reconstructValueAlloc(alloc);
                } else null;
                errdefer if (value) |owned| alloc.free(owned);
                const expected_version = if (internal_keys.isInternalUserKey(key))
                    0
                else blk: {
                    const timestamp_key = try makeTimestampKey(alloc, key);
                    defer alloc.free(timestamp_key);
                    const timestamp = read_txn.get(timestamp_key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    break :blk if (timestamp) |bytes|
                        if (bytes.len >= @sizeOf(u64)) std.mem.readInt(u64, bytes[0..8], .little) else 0
                    else
                        0;
                };
                entries[index] = .{
                    .key = key,
                    .value = value,
                    .expected_version = expected_version,
                };
                initialized += 1;
                result.positions.putAssumeCapacity(key, index);
            }
            result.entries = entries;
            return result;
        }

        pub fn coalesceKeyValueRequest(
            self: anytype,
            alloc: Allocator,
            comptime T: type,
            writes: []const T,
            deletes: []const []const u8,
            transforms: []const types.DocumentTransform,
            transform_snapshot: ?*const TransformReadSnapshot,
        ) !CoalescedKeyValueRequest(T) {
            // SQL mutations lower to complete replacements. A document transform
            // cannot retain the provenance of JSON null after reconstructing JSON.
            if (transforms.len != 0) for (writes) |write| if (write.json_null_fields.len != 0) return error.UnsupportedTransformOperation;
            var result = CoalescedKeyValueRequest(T){};
            var order = std.ArrayListUnmanaged(CoalescedKeyValueRequest(T).Entry).empty;
            defer order.deinit(alloc);
            errdefer {
                // Before toOwnedSlice, the list still owns its full capacity.
                // Release fields here and let order.deinit free that exact backing
                // allocation; never pass the shorter items slice to alloc.free.
                for (order.items) |entry| {
                    if (entry.owned_key) alloc.free(@constCast(entry.key));
                    if (entry.owned_value) alloc.free(@constCast(entry.value.?));
                }
                result.deinit(alloc);
            }

            var positions = std.StringHashMapUnmanaged(usize){};
            defer positions.deinit(alloc);

            for (writes) |write| {
                const gop = try positions.getOrPut(alloc, write.key);
                if (!gop.found_existing) {
                    gop.value_ptr.* = order.items.len;
                    try order.append(alloc, .{
                        .key = write.key,
                        .value = write.value,
                        .json_null_fields = write.json_null_fields,
                        .kind = .write,
                    });
                    continue;
                }
                const entry = &order.items[gop.value_ptr.*];
                if (entry.owned_value) alloc.free(@constCast(entry.value.?));
                if (entry.owned_key) alloc.free(@constCast(entry.key));
                setCoalescedEntryToBorrowedWrite(T, entry, write);
            }

            for (deletes) |key| {
                const gop = try positions.getOrPut(alloc, key);
                if (!gop.found_existing) {
                    gop.value_ptr.* = order.items.len;
                    try order.append(alloc, .{
                        .key = key,
                        .kind = .delete,
                    });
                    continue;
                }
                const entry = &order.items[gop.value_ptr.*];
                if (entry.owned_value) alloc.free(@constCast(entry.value.?));
                entry.owned_value = false;
                resetCoalescedEntryToDelete(T, entry);
            }

            for (transforms) |transform| {
                const maybe_index = positions.get(transform.key);
                const base_json = blk: {
                    if (maybe_index) |entry_index| {
                        const entry = order.items[entry_index];
                        break :blk if (entry.kind == .write) entry.value.? else null;
                    }
                    if (transform_snapshot) |read_snapshot| break :blk read_snapshot.valueFor(transform.key);
                    // Borrowed mutation execution always supplies a pinned read
                    // set; it has no foreground lookup capability to fall back to.
                    if (comptime @TypeOf(self) == *Execution) return error.PreparedReadSetChanged;
                    break :blk try self.get(alloc, transform.key);
                };
                defer if (maybe_index == null and transform_snapshot == null) {
                    if (base_json) |body| alloc.free(body);
                };

                var document_operations = std.ArrayListUnmanaged(types.TransformOp).empty;
                defer document_operations.deinit(alloc);
                var graph_operations = std.ArrayListUnmanaged(struct {
                    op: types.TransformOpType,
                    path: transform_mod.GraphProjectionPath,
                    value_json: []const u8,
                }).empty;
                defer graph_operations.deinit(alloc);
                for (transform.operations) |operation| {
                    const graph_path = try transform_mod.graphProjectionPath(operation.path);
                    if (graph_path) |path| {
                        switch (operation.op) {
                            .push, .pull, .add_to_set => {},
                            else => return error.UnsupportedTransformOperation,
                        }
                        try graph_operations.append(alloc, .{
                            .op = operation.op,
                            .path = path,
                            .value_json = operation.value_json orelse return error.InvalidArgument,
                        });
                    } else {
                        try document_operations.append(alloc, operation);
                    }
                }

                const graph_writes_start = result.graph_writes.items.len;
                const graph_deletes_start = result.graph_deletes.items.len;
                for (graph_operations.items) |operation| {
                    switch (operation.op) {
                        .push, .add_to_set => try appendGraphTransformWrite(
                            alloc,
                            &result.graph_writes,
                            &result.graph_deletes,
                            transform.key,
                            operation.path,
                            operation.value_json,
                        ),
                        .pull => try appendGraphTransformDelete(
                            alloc,
                            &result.graph_writes,
                            &result.graph_deletes,
                            transform.key,
                            operation.path,
                            operation.value_json,
                        ),
                        else => unreachable,
                    }
                }

                const document_transform: types.DocumentTransform = .{
                    .key = transform.key,
                    .operations = document_operations.items,
                    .upsert = transform.upsert,
                };

                // A non-upsert transform against an absent (or same-batch deleted)
                // document is a no-op, but malformed document operations remain
                // errors independent of state. Graph operands were validated while
                // constructing their pending deltas above.
                if (base_json == null and !transform.upsert) {
                    try transform_mod.validateDocumentTransform(alloc, document_transform);
                    for (result.graph_writes.items[graph_writes_start..]) |*write| {
                        deinitOwnedGraphEdgeWrite(alloc, write);
                    }
                    result.graph_writes.shrinkRetainingCapacity(graph_writes_start);
                    for (result.graph_deletes.items[graph_deletes_start..]) |*delete| {
                        deinitOwnedGraphEdgeDelete(alloc, delete);
                    }
                    result.graph_deletes.shrinkRetainingCapacity(graph_deletes_start);
                    continue;
                }

                // Even a graph-only transform remains a logical document write:
                // preserve source version/timestamp and change-journal semantics
                // while applying the projected edge as a delta. The mapper sees
                // the unchanged stripped document and therefore does not clear
                // the graph generation.
                const resolved = try transform_mod.resolveDocumentTransform(alloc, base_json, document_transform);

                const resolved_document = resolved orelse continue;
                var resolved_document_owned = true;
                defer if (resolved_document_owned) alloc.free(resolved_document);

                const gop = try positions.getOrPut(alloc, transform.key);
                if (!gop.found_existing) {
                    gop.value_ptr.* = order.items.len;
                    const owned_key = try alloc.dupe(u8, transform.key);
                    errdefer alloc.free(owned_key);
                    try order.append(alloc, .{
                        .key = owned_key,
                        .value = resolved_document,
                        .kind = .write,
                        .owned_key = true,
                        .owned_value = true,
                    });
                    resolved_document_owned = false;
                    continue;
                }

                const entry = &order.items[gop.value_ptr.*];
                try setCoalescedEntryToOwnedWrite(T, alloc, entry, transform.key, resolved_document);
                resolved_document_owned = false;
            }

            var write_count: usize = 0;
            var delete_count: usize = 0;
            for (order.items) |entry| {
                switch (entry.kind) {
                    .write => write_count += 1,
                    .delete => delete_count += 1,
                }
            }

            const final_entries = try order.toOwnedSlice(alloc);
            result.entries = final_entries;
            if (write_count > 0) result.writes = try alloc.alloc(T, write_count);
            if (delete_count > 0) result.deletes = try alloc.alloc([]const u8, delete_count);

            var write_index: usize = 0;
            var delete_index: usize = 0;
            for (final_entries) |entry| {
                switch (entry.kind) {
                    .write => {
                        result.writes[write_index] = .{
                            .key = entry.key,
                            .value = entry.value.?,
                            .json_null_fields = entry.json_null_fields,
                        };
                        write_index += 1;
                    },
                    .delete => {
                        result.deletes[delete_index] = entry.key;
                        delete_index += 1;
                    },
                }
            }
            return result;
        }

        pub fn generatedPrecomputeModeForSyncLevel(sync_level: types.SyncLevel) GeneratedPrecomputeMode {
            return switch (sync_level) {
                .enrichments, .full_index => .all,
                .propose, .write, .full_text => .none,
            };
        }

        pub fn prepareMergeArtifactEffects(self: anytype, alloc: Allocator, req: types.BatchRequest) !PreparedMergeArtifacts {
            const pages = @import("merge_page_contract.zig");
            const graph = @import("online_graph_artifacts.zig");
            const inventory = @import("artifact_inventory.zig");
            const page = req.merge_page orelse return .{ .effects = &.{} };
            const unchanged: PreparedMergeArtifacts = .{ .effects = page.artifact_effects };
            const has_graph = for (page.artifact_effects) |effect| {
                if (graph.isKey(effect.key)) break true;
            } else false;
            if (!has_graph) return unchanged;
            if (page.source.artifact_catalog == null or page.source.artifact_catalog.?.effect_protocol != 15) return error.InvalidMergePage;
            var txn = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
            defer txn.abort();
            const raw_state = txn.get(merge_state_mod.key) catch |err| switch (err) {
                error.NotFound => return unchanged,
                else => return err,
            };
            var state = try merge_state_mod.decodeAlloc(alloc, raw_state);
            defer state.deinit(alloc);
            if (!merge_state_mod.copyAllowed(state, req.merge_replication.?)) return unchanged;
            const raw_progress = txn.get(pages.key) catch |err| switch (err) {
                error.NotFound => return unchanged,
                else => return err,
            };
            var progress = try pages.decode(alloc, raw_progress);
            defer progress.deinit();
            if (!progress.value.matches(req.merge_replication.?) or !progress.value.source.eql(page.source)) return unchanged;
            // Replays must remain marker-only even after catalog removal/rebuild.
            if (try pages.plan(progress.value, req) == .replay) return unchanged;
            const observed = try inventory.status(alloc, &txn, @import("online_source_contract.zig").namespaceBytes(self.core.identity_namespace));
            if (!observed.ready or observed.ordered == null) return error.ArtifactCatalogDrift;
            const io = self.backend_runtime.io() orelse std.Options.debug_io;
            try self.local_execution.merge_artifact_layout_mutex.lock(io);
            defer self.local_execution.merge_artifact_layout_mutex.unlock(io);
            const layout = try self.local_execution.merge_artifact_layout.get(self.alloc, &txn, progress.value, observed.ordered.?, try inventory.catalogs(&txn));
            var arena = std.heap.ArenaAllocator.init(alloc);
            errdefer arena.deinit();
            const owned = arena.allocator();
            const rebound = try owned.alloc(pages.IntegrityEffect, page.artifact_effects.len);
            for (page.artifact_effects, rebound) |effect, *target| {
                if (graph.isKey(effect.key)) {
                    const changed = try layout.rebind(owned, effect.key, effect.value);
                    target.* = .{ .key = changed.key, .value = changed.value };
                } else target.* = effect;
            }
            // The apply fence rechecks this exact immutable catalog. Neither the
            // request nor its source-byte digest is changed by physical rebinding.
            return .{ .effects = rebound, .receiver_binding = observed.ordered.?, .arena = arena };
        }

        pub fn projectedBatchLsmAdmissionBytes(req: types.BatchRequest) u64 {
            var payload_bytes: u64 = 0;
            var operations: u64 = 0;
            for (req.merge_artifacts) |write| {
                payload_bytes +|= @intCast(write.key.len);
                payload_bytes +|= @intCast(write.value.len);
                operations +|= 1;
            }
            if (req.merge_page) |page| for (page.artifact_effects) |effect| {
                payload_bytes +|= effect.key.len;
                if (effect.value) |value| payload_bytes +|= value.len;
                // The receiver's revision witness is committed beside every
                // imported value/tombstone, including an absent postimage.
                payload_bytes +|= @sizeOf(@TypeOf(@import("artifact_publication.zig").artifactRevisionKey(@splat(0), ""))) + @import("artifact_publication.zig").Position.encoded_len;
                operations +|= 2;
            };
            if (req.merge_page) |page| for (page.provenance_effects) |effect| {
                payload_bytes +|= effect.key.len;
                if (effect.value) |value| payload_bytes +|= value.len;
                payload_bytes +|= @import("source_proof_batch.zig").witness_prefix.len + 24 + 32 + 32 + 32;
                operations +|= 2;
            };
            for (req.writes) |write| {
                payload_bytes +|= @intCast(write.key.len);
                payload_bytes +|= @intCast(write.value.len);
                operations +|= 1;
            }
            for (req.deletes) |key| {
                payload_bytes +|= @intCast(key.len);
                operations +|= 1;
            }
            for (req.transforms) |transform| {
                payload_bytes +|= @intCast(transform.key.len);
                operations +|= 1;
                for (transform.operations) |operation| {
                    payload_bytes +|= @intCast(operation.path.len);
                    if (operation.value_json) |value| payload_bytes +|= @intCast(value.len);
                    operations +|= 1;
                }
            }
            for (req.graph_writes) |write| {
                payload_bytes +|= @intCast(write.index_name.len);
                payload_bytes +|= @intCast(write.source.len);
                payload_bytes +|= @intCast(write.target.len);
                payload_bytes +|= @intCast(write.edge_type.len);
                payload_bytes +|= @intCast(write.edge_id.len +| write.owner_document.len);
                payload_bytes +|= @intCast(write.metadata_json.len);
                operations +|= 1;
            }
            for (req.graph_deletes) |delete| {
                payload_bytes +|= @intCast(delete.index_name.len);
                payload_bytes +|= @intCast(delete.source.len);
                payload_bytes +|= @intCast(delete.target.len);
                payload_bytes +|= @intCast(delete.edge_type.len);
                payload_bytes +|= @intCast(delete.edge_id.len +| delete.owner_document.len);
                operations +|= 1;
            }
            for (req.predicates) |predicate| payload_bytes +|= @intCast(predicate.key.len);

            // Primary rows are accompanied by internal identity/timestamp/replay
            // records, allocator capacity, and the memtable hash index. Two times
            // encoded payload plus a per-operation allowance is deliberately a
            // conservative preflight; the backend still performs the exact guard
            // immediately before WAL append.
            return (payload_bytes *| 2) +| (operations *| 512);
        }

        pub fn resetCoalescedEntryToDelete(comptime T: type, entry: *CoalescedKeyValueRequest(T).Entry) void {
            if (entry.owned_value) {
                // Caller frees previous owned value before switching the entry.
                entry.owned_value = false;
            }
            entry.value = null;
            entry.kind = .delete;
        }

        pub fn setCoalescedEntryToBorrowedWrite(comptime T: type, entry: *CoalescedKeyValueRequest(T).Entry, write: T) void {
            entry.key = write.key;
            entry.value = write.value;
            entry.json_null_fields = write.json_null_fields;
            entry.kind = .write;
            entry.owned_key = false;
            entry.owned_value = false;
        }

        pub fn setCoalescedEntryToOwnedWrite(
            comptime T: type,
            alloc: Allocator,
            entry: *CoalescedKeyValueRequest(T).Entry,
            key: []const u8,
            value: []u8,
        ) !void {
            if (!entry.owned_key) {
                entry.key = try alloc.dupe(u8, key);
                entry.owned_key = true;
            }
            if (entry.owned_value) alloc.free(@constCast(entry.value.?));
            entry.value = value;
            entry.kind = .write;
            entry.owned_value = true;
        }

        pub fn storedDocumentValuesEqual(alloc: Allocator, lhs: []const u8, rhs: []const u8) bool {
            if (std.mem.eql(u8, lhs, rhs)) return true;

            var lhs_parsed = ant_json.parseFromSlice(ant_json.Value, alloc, lhs, .{}) catch return false;
            defer lhs_parsed.deinit();
            var rhs_parsed = ant_json.parseFromSlice(ant_json.Value, alloc, rhs, .{}) catch return false;
            defer rhs_parsed.deinit();
            return json_helpers.jsonValuesEqual(lhs_parsed.value, rhs_parsed.value);
        }

        pub fn validateGeneratedWriteReadSnapshot(self: anytype, read_snapshot: GeneratedWriteReadSnapshot) !void {
            if (read_snapshot.entries.len == 0) return;
            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();
            for (read_snapshot.entries) |entry| {
                if (try GeneratedWriteReadSnapshot.revisions.load(&txn, entry.key) != entry.revision)
                    return error.PreparedReadSetChanged;
            }
        }

        pub fn validatePreparedSchemaViewLocked(self: anytype, prepared: ?schema_registry_mod.SchemaView) !void {
            if (prepared) |view| {
                if (!self.core.isSchemaViewCurrent(view)) return error.PreparedGenerationChanged;
            } else {
                var current = self.core.acquireSchemaView();
                defer if (current) |*view| view.release();
                if (current != null) return error.PreparedGenerationChanged;
            }
        }

        pub fn validateTransformReadSnapshot(self: anytype, read_snapshot: TransformReadSnapshot) !void {
            if (read_snapshot.entries.len == 0) return;
            const predicates = try self.alloc.alloc(transactions_mod.VersionPredicate, read_snapshot.entries.len);
            defer self.alloc.free(predicates);
            for (read_snapshot.entries, predicates) |entry, *predicate| predicate.* = .{
                .key = entry.key,
                .expected_version = entry.expected_version,
            };
            self.core.checkVersionPredicates(predicates, null) catch |err| switch (err) {
                error.VersionConflict => return error.PreparedReadSetChanged,
                else => return err,
            };
        }

        pub fn attachPreparedUpsertDocumentProjections(
            alloc: Allocator,
            batch: *derived_types.DerivedBatch,
            req: types.BatchRequest,
            extracted: []const mapper.ExtractedWrite,
        ) !void {
            const PreparedProjection = struct {
                cleaned: ?[]const u8,
                root: ?std.json.Value,
                source_bytes: usize,
                schema_version: u32,
                write_plan_generation: u64,
            };
            var prepared_by_key = std.StringHashMapUnmanaged(PreparedProjection){};
            defer prepared_by_key.deinit(alloc);

            for (req.writes, 0..) |write, i| {
                if (!extracted[i].hasDocument()) continue;
                const cleaned = if (extracted[i].prepared_text_root == null) try extracted[i].logicalJson() else extracted[i].cleaned_value;
                try prepared_by_key.put(alloc, write.key, .{
                    .cleaned = cleaned,
                    .root = extracted[i].prepared_text_root,
                    .source_bytes = extracted[i].prepared_text_source_bytes,
                    .schema_version = extracted[i].prepared_schema_version,
                    .write_plan_generation = extracted[i].prepared_write_plan_generation,
                });
            }

            for (batch.documents) |*const_doc| {
                const doc: *derived_types.DerivedDocument = @constCast(const_doc);
                if (doc.action != .upsert or doc.cleaned_value != null) continue;
                const prepared = prepared_by_key.get(doc.key) orelse continue;
                if (prepared.root) |root| {
                    doc.prepared_text_root = root;
                    doc.prepared_text_source_bytes = if (prepared.source_bytes != 0) prepared.source_bytes else if (prepared.cleaned) |bytes| bytes.len else 1;
                    doc.prepared_schema_version = prepared.schema_version;
                    doc.prepared_write_plan_generation = prepared.write_plan_generation;
                } else {
                    // Document-mode and fallback rows have no retained typed root.
                    doc.cleaned_value = try alloc.dupe(u8, prepared.cleaned.?);
                }
            }
        }

        pub fn augmentExtractedWriteWithGraphFieldEdges(
            self: anytype,
            alloc: Allocator,
            key: []const u8,
            doc_value: []const u8,
            extracted: *mapper.ExtractedWrite,
        ) !void {
            if (!self.core.hasGraphIndexes() or !extracted.hasDocument()) return;

            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc_value, .{});
            defer parsed.deinit();
            return try augmentExtractedWriteWithGraphFieldEdgesParsed(self, alloc, key, parsed.value, extracted);
        }

        pub fn augmentExtractedWriteWithGraphFieldEdgesFromSnapshotParsed(plan: index_manager_mod.IndexManager.WritePlanSnapshot, alloc: Allocator, key: []const u8, root: std.json.Value, extracted: *mapper.ExtractedWrite) !void {
            return @import("graph_field_plan.zig").fromSnapshot(plan.graph_fields, alloc, key, root, extracted);
        }

        pub fn augmentExtractedWriteWithGraphFieldEdgesParsed(self: anytype, alloc: Allocator, key: []const u8, root: std.json.Value, extracted: *mapper.ExtractedWrite) !void {
            return @import("graph_field_plan.zig").fromCatalog(self.core, alloc, key, root, extracted);
        }

        pub fn buildOverwrittenDocKeys(
            alloc: Allocator,
            writes: []const types.BatchWrite,
            overwritten_flags: []const bool,
        ) ![]const []const u8 {
            var keys = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (keys.items) |key| alloc.free(@constCast(key));
                keys.deinit(alloc);
            }

            for (writes, 0..) |write, i| {
                if (!overwritten_flags[i]) continue;
                try appendOwnedKey(alloc, &keys, write.key);
            }

            return try keys.toOwnedSlice(alloc);
        }

        pub fn clearGeneratedDenseMemoJobs(alloc: Allocator, jobs: *std.ArrayListUnmanaged(GeneratedDenseMemoJob)) void {
            for (jobs.items) |job| alloc.free(@constCast(job.text));
            jobs.clearRetainingCapacity();
        }

        pub fn clearGeneratedSparseMemoJobs(alloc: Allocator, jobs: *std.ArrayListUnmanaged(GeneratedSparseMemoJob)) void {
            for (jobs.items) |job| alloc.free(@constCast(job.text));
            jobs.clearRetainingCapacity();
        }

        pub fn encodeTimestampValue(alloc: Allocator, timestamp_ns: u64) ![]u8 {
            const buf = try alloc.alloc(u8, 8);
            std.mem.writeInt(u64, buf[0..8], timestamp_ns, .little);
            return buf;
        }

        pub fn extractStringField(alloc: Allocator, doc_value: []const u8, field_name: []const u8) !?[]u8 {
            const parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc_value, .{});
            defer parsed.deinit();
            if (parsed.value != .object) return null;
            const field = parsed.value.object.get(field_name) orelse return null;
            if (field != .string) return null;
            return try alloc.dupe(u8, field.string);
        }

        pub fn flushGeneratedDenseChunkBatch(
            alloc: Allocator,
            runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            dense_embedder: embedder_mod.DenseEmbedder,
            embedding_name: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            dense_embeddings: anytype,
            sources: []const ChunkEmbeddingSource,
            source_indexes: *std.ArrayListUnmanaged(usize),
            chunk_texts: *std.ArrayListUnmanaged([]const u8),
            consumer_indexes: []const []const u8,
            comptime appendForConsumers: anytype,
        ) !void {
            if (chunk_texts.items.len == 0) return;

            const vectors = try enrichment_runtime_mod.embedDenseBatchTracked(runtime, consumer_indexes, alloc, dense_embedder, embedding_name, chunk_texts.items, request.expected_dims);
            defer embedder_mod.freeDenseEmbeddingBatch(alloc, vectors);
            if (vectors.len != source_indexes.items.len) return error.InvalidEmbeddingResponse;

            for (source_indexes.items, vectors) |source_index, vector| {
                const source = sources[source_index];
                const artifact_key = try appendEmbeddingArtifactWrite(
                    alloc,
                    artifact_writes,
                    source.key,
                    request.doc_key,
                    embedding_name,
                    request.source_field,
                    source.key,
                    .{ .generated = enrichment_artifact_codec.hashEmbeddingSource(source.text, request.producer_json) },
                    vector,
                );
                defer alloc.free(artifact_key);
                try appendForConsumers(alloc, dense_embeddings, source.key, request.doc_key, artifact_key, vector, consumer_indexes);
            }

            chunk_texts.clearRetainingCapacity();
            source_indexes.clearRetainingCapacity();
        }

        pub fn flushGeneratedDenseChunkSourceBatch(
            alloc: Allocator,
            db: anytype,
            runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            dense_embedder: embedder_mod.DenseEmbedder,
            embedding_name: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            dense_embeddings: anytype,
            sources: *std.ArrayListUnmanaged(ChunkEmbeddingSource),
            consumer_indexes: []const []const u8,
            skip_unchanged_artifacts: bool,
            pending_lookup: ?*const PendingArtifactWriteIndex,
            comptime appendForConsumers: anytype,
        ) !void {
            if (sources.items.len == 0) return;
            defer clearChunkEmbeddingSourceList(alloc, sources);

            var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
            defer chunk_texts.deinit(alloc);
            var source_indexes = std.ArrayListUnmanaged(usize).empty;
            defer source_indexes.deinit(alloc);

            for (sources.items, 0..) |source, i| {
                const source_hash = enrichment_artifact_codec.hashEmbeddingSource(source.text, request.producer_json);
                const artifact_key = try embeddingArtifactKeyForBaseAlloc(alloc, source.key, embedding_name);
                defer alloc.free(artifact_key);
                if (skip_unchanged_artifacts) {
                    if (try storedOrPendingEmbeddingSourceHash(db, pending_lookup, artifact_key)) |existing_hash| {
                        if (existing_hash == source_hash) {
                            try appendForConsumers(alloc, dense_embeddings, source.key, request.doc_key, artifact_key, &.{}, consumer_indexes);
                            continue;
                        }
                    }
                }
                try chunk_texts.append(alloc, source.text);
                try source_indexes.append(alloc, i);
            }

            try flushGeneratedDenseChunkBatch(alloc, runtime, dense_embedder, embedding_name, request, artifact_writes, dense_embeddings, sources.items, &source_indexes, &chunk_texts, consumer_indexes, appendForConsumers);
        }

        pub fn flushGeneratedDenseMemoJobs(
            self: anytype,
            memo: *GeneratedEmbeddingMemo,
            jobs: *std.ArrayListUnmanaged(GeneratedDenseMemoJob),
        ) !void {
            if (jobs.items.len == 0) return;
            defer clearGeneratedDenseMemoJobs(memo.alloc, jobs);
            const runtime = self.enrichment_runtime orelse return;
            const embedder = runtime.config.dense_embedder orelse return;
            const texts = try memo.alloc.alloc([]const u8, jobs.items.len);
            defer memo.alloc.free(texts);
            for (jobs.items, texts) |job, *text| text.* = job.text;
            const first = jobs.items[0].request;
            const vectors = try enrichment_runtime_mod.embedDenseBatchTracked(
                runtime,
                first.consumer_indexes,
                memo.alloc,
                embedder,
                requestEmbeddingName(first),
                texts,
                first.expected_dims,
            );
            defer embedder_mod.freeDenseEmbeddingBatch(memo.alloc, vectors);
            for (jobs.items, vectors) |job, vector| try memo.putDenseCopy(job.key, vector);
        }

        pub fn flushGeneratedSparseChunkBatch(
            alloc: Allocator,
            runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            sparse_embedder: embedder_mod.SparseEmbedder,
            embedding_name: []const u8,
            semantic_producer: []const u8,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
            sources: []const ChunkEmbeddingSource,
            source_indexes: *std.ArrayListUnmanaged(usize),
            chunk_texts: *std.ArrayListUnmanaged([]const u8),
            consumer_indexes: []const []const u8,
        ) !void {
            if (chunk_texts.items.len == 0) return;

            const sparse_batch = try enrichment_runtime_mod.embedSparseBatchTracked(runtime, consumer_indexes, alloc, sparse_embedder, embedding_name, chunk_texts.items);
            defer embedder_mod.freeSparseEmbeddingBatch(alloc, sparse_batch);
            if (sparse_batch.len != source_indexes.items.len) return error.InvalidEmbeddingResponse;

            for (source_indexes.items, sparse_batch) |source_index, sparse| {
                const source = sources[source_index];
                const artifact_key = try appendSparseEmbeddingArtifactWrite(
                    alloc,
                    artifact_writes,
                    source.key,
                    embedding_name,
                    .{ .generated = enrichment_artifact_codec.hashEmbeddingSource(source.text, semantic_producer) },
                    sparse.indices,
                    sparse.values,
                );
                defer alloc.free(artifact_key);
                try appendDerivedSparseEmbeddingForConsumers(alloc, sparse_embeddings, source.key, artifact_key, sparse.indices, sparse.values, consumer_indexes);
            }

            chunk_texts.clearRetainingCapacity();
            source_indexes.clearRetainingCapacity();
        }

        pub fn flushGeneratedSparseChunkSourceBatch(
            alloc: Allocator,
            db: anytype,
            runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            sparse_embedder: embedder_mod.SparseEmbedder,
            embedding_name: []const u8,
            semantic_producer: []const u8,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
            sources: *std.ArrayListUnmanaged(ChunkEmbeddingSource),
            consumer_indexes: []const []const u8,
            pending_lookup: *const PendingArtifactWriteIndex,
        ) !void {
            if (sources.items.len == 0) return;
            defer clearChunkEmbeddingSourceList(alloc, sources);

            var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
            defer chunk_texts.deinit(alloc);
            var source_indexes = std.ArrayListUnmanaged(usize).empty;
            defer source_indexes.deinit(alloc);

            for (sources.items, 0..) |source, i| {
                const source_hash = enrichment_artifact_codec.hashEmbeddingSource(source.text, semantic_producer);
                const artifact_key = try embeddingArtifactKeyForBaseAlloc(alloc, source.key, embedding_name);
                defer alloc.free(artifact_key);
                if (try storedOrPendingEmbeddingSourceHash(db, pending_lookup, artifact_key)) |existing_hash| {
                    if (existing_hash == source_hash) {
                        try appendDerivedSparseEmbeddingForConsumers(alloc, sparse_embeddings, source.key, artifact_key, &.{}, &.{}, consumer_indexes);
                        continue;
                    }
                }
                try chunk_texts.append(alloc, source.text);
                try source_indexes.append(alloc, i);
            }

            try flushGeneratedSparseChunkBatch(alloc, runtime, sparse_embedder, embedding_name, semantic_producer, artifact_writes, sparse_embeddings, sources.items, &source_indexes, &chunk_texts, consumer_indexes);
        }

        pub fn flushGeneratedSparseMemoJobs(
            self: anytype,
            memo: *GeneratedEmbeddingMemo,
            jobs: *std.ArrayListUnmanaged(GeneratedSparseMemoJob),
        ) !void {
            if (jobs.items.len == 0) return;
            defer clearGeneratedSparseMemoJobs(memo.alloc, jobs);
            const runtime = self.enrichment_runtime orelse return;
            const embedder = runtime.config.sparse_embedder orelse return;
            const texts = try memo.alloc.alloc([]const u8, jobs.items.len);
            defer memo.alloc.free(texts);
            for (jobs.items, texts) |job, *text| text.* = job.text;
            const first = jobs.items[0].request;
            const vectors = try enrichment_runtime_mod.embedSparseBatchTracked(
                runtime,
                first.consumer_indexes,
                memo.alloc,
                embedder,
                requestEmbeddingName(first),
                texts,
            );
            defer embedder_mod.freeSparseEmbeddingBatch(memo.alloc, vectors);
            for (jobs.items, vectors) |job, vector| try memo.putSparseCopy(job.key, vector);
        }

        pub const freeDocumentExtractionUnitDescriptors = execution_resources.freeDocumentExtractionUnitDescriptors;

        pub const freeOwnedConstKeySlice = execution_resources.freeOwnedConstKeySlice;

        pub fn freeOwnedKeySlice(alloc: Allocator, keys: [][]u8) void {
            for (keys) |key| alloc.free(key);
            alloc.free(keys);
        }

        pub fn generatedEmbedBatchBytes() usize {
            if (comptime builtin.os.tag == .freestanding) return generated_embed_default_batch_bytes;
            const raw = getenv("ANTFLY_ENRICHMENT_EMBED_BATCH_BYTES") orelse return generated_embed_default_batch_bytes;
            if (raw.len == 0) return generated_embed_default_batch_bytes;
            const parsed = std.fmt.parseUnsigned(usize, raw, 10) catch return generated_embed_default_batch_bytes;
            return @max(@as(usize, 1), parsed);
        }

        pub fn generatedEmbedBatchItems() usize {
            if (comptime builtin.os.tag == .freestanding) return generated_embed_default_batch_items;
            const raw = getenv("ANTFLY_ENRICHMENT_EMBED_BATCH_ITEMS") orelse return generated_embed_default_batch_items;
            if (raw.len == 0) return generated_embed_default_batch_items;
            const parsed = std.fmt.parseUnsigned(usize, raw, 10) catch return generated_embed_default_batch_items;
            return @max(@as(usize, 1), parsed);
        }

        pub fn makeTimestampKey(alloc: Allocator, key: []const u8) ![]u8 {
            return try internal_keys.ttlKeyAlloc(alloc, key);
        }

        pub fn overwriteProbeLessThan(_: void, lhs: OverwriteProbeEntry, rhs: OverwriteProbeEntry) bool {
            return std.mem.order(u8, lhs.key, rhs.key) == .lt;
        }

        pub fn parsePatternRfc3339ToNs(text: []const u8) !?u64 {
            return try db_query_graph.parsePatternRfc3339ToNs(text);
        }

        pub fn planGeneratedEnrichmentsForRows(
            alloc: Allocator,
            req: types.BatchRequest,
            extracted: []const mapper.ExtractedWrite,
            write_plan: index_manager_mod.IndexManager.WritePlanSnapshot,
        ) !GeneratedBatchWritePlan {
            if (req.writes.len != extracted.len) return error.InvalidArgument;
            const rows = try alloc.alloc(
                index_manager_mod.IndexManager.WritePlanSnapshot.BorrowedGeneratedRowPlan,
                req.writes.len,
            );
            var plan = GeneratedBatchWritePlan{ .alloc = alloc, .rows = rows };
            errdefer plan.deinit();
            for (req.writes, extracted) |write, row| {
                if (!row.hasDocument()) {
                    rows[plan.initialized] = .{ .alloc = alloc, .requests = &.{} };
                    plan.initialized += 1;
                    continue;
                }
                rows[plan.initialized] = try write_plan.planGeneratedEnrichmentsForRowBorrowed(
                    alloc,
                    write.key,
                    row,
                );
                plan.initialized += 1;
            }
            return plan;
        }

        pub fn prepareGeneratedEnrichments(
            self: anytype,
            alloc: Allocator,
            req: types.BatchRequest,
            extracted: []const mapper.ExtractedWrite,
            precompute_mode: GeneratedPrecomputeMode,
            force_generated_artifact_names: []const []const u8,
            preplanned: ?*const GeneratedBatchWritePlan,
            generated_memo: ?*GeneratedEmbeddingMemo,
        ) !PrecomputedGeneratedBatch {
            if (preplanned == null and !self.core.hasGeneratedEnrichmentTargets()) return .{};
            if (preplanned) |plan| if (plan.rows.len != req.writes.len) return error.InvalidArgument;
            var fallback_write_plan = if (preplanned == null)
                try self.core.index_manager.acquireWritePlanSnapshot()
            else
                null;
            defer if (fallback_write_plan) |*view| view.release();
            if (preplanned) |plan| if (generated_memo) |memo| {
                try prewarmGeneratedDenseMemo(self, req, extracted, precompute_mode, plan, memo);
                try prewarmGeneratedSparseMemo(self, req, extracted, precompute_mode, plan, memo);
            };

            var artifact_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
            errdefer {
                for (artifact_writes.items) |write| {
                    alloc.free(@constCast(write.key));
                    alloc.free(@constCast(write.value));
                }
                artifact_writes.deinit(alloc);
            }
            var artifact_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (artifact_delete_keys.items) |key| alloc.free(@constCast(key));
                artifact_delete_keys.deinit(alloc);
            }
            var pending_deletes = PendingChunkDeleteIndex{};
            defer pending_deletes.deinit(alloc);
            var pending_writes = PendingArtifactWriteIndex{};
            defer pending_writes.deinit(alloc);
            var indexed_write_count: usize = 0;
            var documents = std.ArrayListUnmanaged(derived_types.DerivedDocument).empty;
            // ArrayList.items may be shorter than its backing allocation. Release
            // owned fields item-by-item, then let the list free its exact capacity.
            errdefer {
                for (documents.items) |doc| derived_types.deinitDerivedDocument(alloc, doc);
                documents.deinit(alloc);
            }
            var dense_embeddings = std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite).empty;
            errdefer {
                for (dense_embeddings.items) |embedding|
                    derived_types.deinitDerivedDenseEmbedding(alloc, embedding);
                dense_embeddings.deinit(alloc);
            }
            var sparse_embeddings = std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite).empty;
            errdefer {
                for (sparse_embeddings.items) |embedding|
                    derived_types.deinitDerivedSparseEmbedding(alloc, embedding);
                sparse_embeddings.deinit(alloc);
            }
            var planned = std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRef).empty;
            errdefer {
                for (planned.items) |request| enrichment_types.freeGeneratedRef(alloc, request);
                planned.deinit(alloc);
            }
            var coverage_outcomes = std.ArrayListUnmanaged(PrecomputedCoverageOutcome).empty;
            errdefer {
                for (coverage_outcomes.items) |outcome| outcome.deinit(alloc);
                coverage_outcomes.deinit(alloc);
            }
            var coverage_candidates = std.ArrayListUnmanaged(PrecomputedCoverageCandidate).empty;
            defer {
                for (coverage_candidates.items) |candidate| candidate.deinit(alloc);
                coverage_candidates.deinit(alloc);
            }
            var deferred_asset_producer_items = std.ArrayListUnmanaged(PrecomputeAssetProducerBatchItem).empty;
            defer {
                clearPrecomputeAssetProducerBatchItems(alloc, &deferred_asset_producer_items);
                deferred_asset_producer_items.deinit(alloc);
            }

            for (req.writes, 0..) |_, i| {
                if (!extracted[i].hasDocument()) continue;
                var owned_row_plan: ?GeneratedBatchWritePlan = null;
                defer if (owned_row_plan) |*plan| plan.deinit();
                const generated = if (preplanned) |plan|
                    plan.rows[i].requests
                else blk: {
                    owned_row_plan = try planGeneratedEnrichmentsForRows(
                        alloc,
                        .{ .writes = req.writes[i .. i + 1] },
                        extracted[i .. i + 1],
                        fallback_write_plan.?.plan().*,
                    );
                    break :blk owned_row_plan.?.rows[0].requests;
                };
                if (generated.len == 0) continue;

                var chunk_cache = std.ArrayListUnmanaged(ChunkCacheEntry).empty;
                defer {
                    for (chunk_cache.items) |entry| {
                        alloc.free(entry.key);
                        chunker_mod.freeChunks(alloc, entry.chunks);
                    }
                    chunk_cache.deinit(alloc);
                }
                var inline_embedding_cleanup = InlineChunkEmbeddingCleanup{};
                defer inline_embedding_cleanup.deinit(alloc);

                const document_execution: ?*enrichment_runtime_mod.PrecommitDocumentExecution = if (self.enrichment_runtime) |runtime|
                    if (precompute_mode == .all and enrichment_runtime_mod.PrecommitDocumentExecution.useful(generated)) try enrichment_runtime_mod.PrecommitDocumentExecution.create(runtime, generated, (try extracted[i].logicalJson()).?) else null
                else
                    null;
                defer if (document_execution) |execution| execution.destroy();
                for (generated, 0..) |request, request_index| {
                    if (document_execution) |execution| execution.select(request_index);
                    if (!try shouldPrecomputeGeneratedRequest(self, precompute_mode, request)) {
                        try appendGeneratedEnrichmentRef(alloc, &planned, request);
                        continue;
                    }

                    // Templates may inspect arbitrary root data; document extraction
                    // also consumes configured source-metadata fields. Plain field
                    // consumers render only their requested ordinal, never a vector
                    // or JSON payload belonging to another consumer.
                    const cleaned = (if (request.source_template.len > 0 or request.kind == .asset)
                        try extracted[i].logicalJson()
                    else
                        try extracted[i].logicalJsonForField(request.source_field)).?;

                    switch (request.kind) {
                        .asset => {
                            // Synchronous asset production settles graph/full_text
                            // consumer coverage in the same commit. Classification is
                            // by THIS request's artifact key (a mid-batch producer
                            // flush may append other requests' writes): its write is
                            // produced, its delete alone is intentional no-output,
                            // and a deferred prompt producer touches neither here —
                            // it settles in the batch flush below or through replay.
                            const writes_before = artifact_writes.items.len;
                            const deletes_before = artifact_delete_keys.items.len;
                            try computeAssetRequestDerived(
                                alloc,
                                self,
                                cleaned,
                                request,
                                &artifact_writes,
                                &artifact_delete_keys,
                                &documents,
                                &dense_embeddings,
                                &sparse_embeddings,
                                &deferred_asset_producer_items,
                                containsName(force_generated_artifact_names, requestArtifactName(request)),
                                document_execution,
                                &coverage_outcomes,
                            );
                            const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, request.doc_key, "asset", requestArtifactName(request));
                            defer alloc.free(asset_key);
                            if (sliceContainsWriteKey(artifact_writes.items[writes_before..], asset_key)) {
                                try appendPrecomputedArtifactCoverageOutcomes(self, alloc, &coverage_outcomes, request, .produced);
                            } else if (sliceContainsKey(artifact_delete_keys.items[deletes_before..], asset_key)) {
                                try appendPrecomputedArtifactCoverageOutcomes(self, alloc, &coverage_outcomes, request, .skipped);
                            }
                        },
                        .chunk_text => {
                            const writes_before = artifact_writes.items.len;
                            const docs_before = documents.items.len;
                            const deletes_before = artifact_delete_keys.items.len;
                            try computeChunkRequestDerived(
                                alloc,
                                self,
                                cleaned,
                                request,
                                &artifact_writes,
                                &artifact_delete_keys,
                                &documents,
                                &chunk_cache,
                            );
                            // Chunk ids vary; within this request's appended slice
                            // only its own chunk rows can carry the doc's chunk-kind
                            // prefix, so that prefix classifies exactly.
                            const chunk_prefix = try internal_keys.artifactTypePrefixAlloc(alloc, request.doc_key, "chunk");
                            defer alloc.free(chunk_prefix);
                            if (sliceContainsWriteKeyPrefix(artifact_writes.items[writes_before..], chunk_prefix) or
                                sliceContainsDocKeyPrefix(documents.items[docs_before..], chunk_prefix))
                            {
                                try appendPrecomputedArtifactCoverageOutcomes(self, alloc, &coverage_outcomes, request, .produced);
                            } else if (sliceContainsKeyPrefix(artifact_delete_keys.items[deletes_before..], chunk_prefix)) {
                                try appendPrecomputedArtifactCoverageOutcomes(self, alloc, &coverage_outcomes, request, .skipped);
                            }
                        },
                        .dense_embedding => {
                            try pending_deletes.extend(alloc, artifact_delete_keys.items);
                            try indexPendingArtifactWrites(alloc, &pending_writes, artifact_writes.items, &indexed_write_count);
                            const before = dense_embeddings.items.len;
                            computeDenseRequestDerived(alloc, self, cleaned, request, &artifact_writes, &pending_deletes.keys, &dense_embeddings, &chunk_cache, generated_memo, &pending_writes) catch |err| switch (err) {
                                error.MissingDenseEmbedder => {
                                    try appendGeneratedEnrichmentRef(alloc, &planned, request);
                                    continue;
                                },
                                else => return err,
                            };
                            try appendStalePrecomputedChunkEmbeddingDeletes(alloc, self, cleaned, request, &chunk_cache, &pending_writes, pending_deletes.forDoc(request.doc_key), &artifact_delete_keys, &inline_embedding_cleanup);
                            try appendPrecomputedCoverageCandidate(
                                alloc,
                                &coverage_candidates,
                                request,
                                dense_embeddings.items.len > before,
                            );
                        },
                        .sparse_embedding => {
                            try pending_deletes.extend(alloc, artifact_delete_keys.items);
                            try indexPendingArtifactWrites(alloc, &pending_writes, artifact_writes.items, &indexed_write_count);
                            const before = sparse_embeddings.items.len;
                            computeSparseRequestDerived(alloc, self, cleaned, request, &artifact_writes, &pending_deletes.keys, &sparse_embeddings, &chunk_cache, generated_memo, &pending_writes) catch |err| switch (err) {
                                error.MissingSparseEmbedder => {
                                    try appendGeneratedEnrichmentRef(alloc, &planned, request);
                                    continue;
                                },
                                else => return err,
                            };
                            try appendStalePrecomputedChunkEmbeddingDeletes(alloc, self, cleaned, request, &chunk_cache, &pending_writes, pending_deletes.forDoc(request.doc_key), &artifact_delete_keys, &inline_embedding_cleanup);
                            try appendPrecomputedCoverageCandidate(
                                alloc,
                                &coverage_candidates,
                                request,
                                sparse_embeddings.items.len > before,
                            );
                        },
                    }
                }
                try inline_embedding_cleanup.flush(alloc, self, req.writes[i].key, &artifact_delete_keys);
            }

            try flushPrecomputeAssetProducerBatch(alloc, self, &deferred_asset_producer_items, &artifact_writes, &documents, &coverage_outcomes);

            // Resolve coverage only after every deferred producer has contributed its
            // manifest. This keeps terminal outcomes in the same primary commit while
            // preserving genuinely unresolved requests for replay.
            for (coverage_candidates.items) |candidate| {
                // A terminal no-output document revision retires this producer's
                // previous artifact in the same commit as its coverage outcome.
                // Ordinary replacements retain their member identity, and unresolved
                // or chunk-source work must not be treated as an absent embedding.
                if (!candidate.produced and candidate.request.input_kind == .document and
                    (try precomputedEmbeddingCoverageOutcome(self, alloc, candidate.request, artifact_writes.items, false)) == .skipped)
                {
                    const key = try embeddingArtifactKeyForBaseAlloc(alloc, candidate.request.doc_key, requestEmbeddingName(candidate.request));
                    errdefer alloc.free(key);
                    try artifact_delete_keys.append(alloc, key);
                }
                if (!try appendPrecomputedEmbeddingCoverageOutcomes(
                    self,
                    alloc,
                    &coverage_outcomes,
                    candidate.request,
                    artifact_writes.items,
                    candidate.produced,
                )) try appendGeneratedEnrichmentRef(alloc, &planned, candidate.request);
            }

            var result = PrecomputedGeneratedBatch{};
            errdefer result.deinit(alloc);
            result.artifact_writes = try artifact_writes.toOwnedSlice(alloc);
            result.artifact_delete_keys = try artifact_delete_keys.toOwnedSlice(alloc);
            result.documents = try documents.toOwnedSlice(alloc);
            result.dense_embeddings = try dense_embeddings.toOwnedSlice(alloc);
            result.sparse_embeddings = try sparse_embeddings.toOwnedSlice(alloc);
            result.generated_enrichment_refs = try planned.toOwnedSlice(alloc);
            result.coverage_outcomes = try coverage_outcomes.toOwnedSlice(alloc);
            return result;
        }

        pub fn preparePreservedEmbeddingSources(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: []const types.BatchWrite,
            pending_deletes: *const std.StringHashMapUnmanaged(void),
            cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
            shared_pending_writes: ?*const PendingArtifactWriteIndex,
        ) !?[]ChunkEmbeddingSource {
            var sources = std.ArrayListUnmanaged(ChunkEmbeddingSource).empty;
            var keep = false;
            defer if (!keep) {
                clearChunkEmbeddingSourceList(alloc, &sources);
                sources.deinit(alloc);
            };
            if (requestUsesChunkSource(request)) {
                if (requestUsesPinnedMaterializedChunkArtifact(request)) {
                    var local_pending_writes = if (shared_pending_writes == null)
                        try PendingArtifactWriteIndex.init(alloc, artifact_writes)
                    else
                        PendingArtifactWriteIndex{};
                    defer local_pending_writes.deinit(alloc);
                    const pending_writes = shared_pending_writes orelse &local_pending_writes;
                    var seen = std.StringHashMapUnmanaged(void).empty;
                    defer seen.deinit(alloc);
                    try collectChunkEmbeddingSourcesFromWrites(alloc, &sources, &seen, pending_writes.chunkWritesForDoc(request.doc_key), request.doc_key, requestArtifactName(request), request.source_field);
                    try collectChunkEmbeddingSourcesFromStore(alloc, db.core.store, &sources, &seen, request.doc_key, requestArtifactName(request), request.source_field, &pending_writes.values, pending_deletes);
                } else {
                    var chunks_created: usize = 0;
                    sources = .fromOwnedSlice(try chunkEmbeddingSourcesForRequest(alloc, db, doc_value, request, cache, &chunks_created));
                }
            } else {
                const text = if (request.source_template.len != 0)
                    try renderSourceTemplateText(alloc, db, request.source_template, doc_value)
                else
                    try extractStringField(alloc, doc_value, request.source_field);
                if (text) |value| {
                    var owned = true;
                    defer if (owned) alloc.free(value);
                    if (value.len != 0) {
                        const key = try alloc.dupe(u8, request.doc_key);
                        errdefer alloc.free(key);
                        try sources.append(alloc, .{ .key = key, .text = value });
                        owned = false;
                    }
                }
            }
            for (sources.items) |source| {
                const artifact_key = try embeddingArtifactKeyForBaseAlloc(alloc, source.key, requestEmbeddingName(request));
                defer alloc.free(artifact_key);
                const metadata = db.core.store.getArtifactMetadata(artifact_key) catch |err| switch (err) {
                    error.NotFound => return null,
                    else => return err,
                };
                const expected_kind: enrichment_artifact_codec.Kind = if (request.kind == .dense_embedding) .dense_embedding else .sparse_embedding;
                if (metadata.header.kind != expected_kind or metadata.sourceHash() != enrichment_artifact_codec.hashEmbeddingSource(source.text, request.producer_json) or
                    (request.kind == .dense_embedding and metadata.dense_dimensions != request.expected_dims)) return null;
            }
            const result = try sources.toOwnedSlice(alloc);
            keep = true;
            return result;
        }

        pub fn prewarmGeneratedDenseMemo(
            self: anytype,
            req: types.BatchRequest,
            extracted: []const mapper.ExtractedWrite,
            precompute_mode: GeneratedPrecomputeMode,
            plan: *const GeneratedBatchWritePlan,
            memo: *GeneratedEmbeddingMemo,
        ) !void {
            const runtime = self.enrichment_runtime orelse return;
            const embedder = runtime.config.dense_embedder orelse return;
            var jobs = std.ArrayListUnmanaged(GeneratedDenseMemoJob).empty;
            defer {
                clearGeneratedDenseMemoJobs(memo.alloc, &jobs);
                jobs.deinit(memo.alloc);
            }
            var pending = std.AutoHashMapUnmanaged(GeneratedEmbeddingMemo.Key, void).empty;
            defer pending.deinit(memo.alloc);
            var batch_bytes: usize = 0;
            const max_items = generatedEmbedBatchItems();
            const max_bytes = generatedEmbedBatchBytes();

            for (req.writes, 0..) |_, row_index| {
                for (plan.rows[row_index].requests) |request| {
                    if (request.kind != .dense_embedding or request.input_kind != .document) continue;
                    if (!try shouldPrecomputeGeneratedRequest(self, precompute_mode, request)) continue;
                    if (request.source_template.len > 0 and embedder.supportsParts()) continue;
                    const cleaned = (if (request.source_template.len > 0) try extracted[row_index].logicalJson() else try extracted[row_index].logicalJsonForField(request.source_field)) orelse continue;
                    const source_text = if (request.source_template.len > 0)
                        renderSourceTemplateText(memo.alloc, self, request.source_template, cleaned) catch |err| switch (err) {
                            error.OutOfMemory, error.PermanentPromptFailure, error.TransientPromptFailure => return err,
                            else => null,
                        }
                    else
                        try extractStringField(memo.alloc, cleaned, request.source_field);
                    const text = source_text orelse continue;
                    if (text.len == 0) {
                        memo.alloc.free(text);
                        continue;
                    }
                    var text_owned = true;
                    errdefer if (text_owned) memo.alloc.free(text);
                    const key_value = GeneratedEmbeddingMemo.key(
                        .dense_embedding,
                        requestEmbeddingName(request),
                        request.producer_json,
                        request.execution_json,
                        request.expected_dims,
                        text,
                    );
                    if (memo.dense.contains(key_value) or pending.contains(key_value)) {
                        memo.alloc.free(text);
                        text_owned = false;
                        continue;
                    }
                    if (try prewarmGeneratedMemoFromArtifact(self, memo, request, text, key_value)) {
                        memo.alloc.free(text);
                        text_owned = false;
                        continue;
                    }
                    const incompatible = if (jobs.items.len == 0) false else blk: {
                        const first = jobs.items[0].request;
                        break :blk first.expected_dims != request.expected_dims or
                            !std.mem.eql(u8, requestEmbeddingName(first), requestEmbeddingName(request)) or
                            !std.mem.eql(u8, first.producer_json, request.producer_json) or
                            !std.mem.eql(u8, first.execution_json, request.execution_json) or
                            !generatedConsumerSetsEqual(first.consumer_indexes, request.consumer_indexes);
                    };
                    if (incompatible or (jobs.items.len > 0 and
                        (jobs.items.len >= max_items or batch_bytes + text.len > max_bytes)))
                    {
                        try flushGeneratedDenseMemoJobs(self, memo, &jobs);
                        pending.clearRetainingCapacity();
                        batch_bytes = 0;
                    }
                    try pending.put(memo.alloc, key_value, {});
                    try jobs.append(memo.alloc, .{ .key = key_value, .text = text, .request = request });
                    text_owned = false;
                    batch_bytes += text.len;
                    if (jobs.items.len >= max_items or batch_bytes >= max_bytes) {
                        try flushGeneratedDenseMemoJobs(self, memo, &jobs);
                        pending.clearRetainingCapacity();
                        batch_bytes = 0;
                    }
                }
            }
            try flushGeneratedDenseMemoJobs(self, memo, &jobs);
        }

        pub fn prewarmGeneratedMemoFromArtifact(
            self: anytype,
            memo: *GeneratedEmbeddingMemo,
            request: enrichment_types.GeneratedEnrichmentRequest,
            text: []const u8,
            key_value: GeneratedEmbeddingMemo.Key,
        ) !bool {
            if (!memo.reuse_stored_artifacts) return false;
            const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(memo.alloc, request.doc_key, requestEmbeddingName(request));
            defer memo.alloc.free(artifact_key);
            const raw = self.core.store.get(memo.alloc, artifact_key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            defer memo.alloc.free(raw);
            const source_hash = enrichment_artifact_codec.sourceHash(raw) catch return false;
            if (source_hash == null or source_hash.? != enrichment_artifact_codec.hashEmbeddingSource(text, request.producer_json)) return false;
            switch (request.kind) {
                .dense_embedding => {
                    const dims = enrichment_artifact_codec.decodeDenseEmbeddingDims(raw) catch return false;
                    if (dims != request.expected_dims) return false;
                    const vector = enrichment_artifact_codec.decodeDenseEmbeddingAlloc(memo.alloc, raw) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => return false,
                    };
                    var owned = true;
                    defer if (owned) memo.alloc.free(vector);
                    for (vector) |value| if (!std.math.isFinite(value)) return false;
                    _ = try memo.adoptDense(key_value, vector);
                    owned = false;
                },
                .sparse_embedding => {
                    var vector = enrichment_artifact_codec.decodeSparseEmbeddingAlloc(memo.alloc, raw) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => return false,
                    };
                    var owned = true;
                    defer if (owned) vector.deinit(memo.alloc);
                    for (vector.values) |value| if (!std.math.isFinite(value)) return false;
                    _ = try memo.adoptSparse(key_value, .{ .indices = vector.indices, .values = vector.values });
                    owned = false;
                },
                else => return false,
            }
            return true;
        }

        pub fn prewarmGeneratedSparseMemo(
            self: anytype,
            req: types.BatchRequest,
            extracted: []const mapper.ExtractedWrite,
            precompute_mode: GeneratedPrecomputeMode,
            plan: *const GeneratedBatchWritePlan,
            memo: *GeneratedEmbeddingMemo,
        ) !void {
            if (self.enrichment_runtime == null or self.enrichment_runtime.?.config.sparse_embedder == null) return;
            var jobs = std.ArrayListUnmanaged(GeneratedSparseMemoJob).empty;
            defer {
                clearGeneratedSparseMemoJobs(memo.alloc, &jobs);
                jobs.deinit(memo.alloc);
            }
            var pending = std.AutoHashMapUnmanaged(GeneratedEmbeddingMemo.Key, void).empty;
            defer pending.deinit(memo.alloc);
            var batch_bytes: usize = 0;
            const max_items = generatedEmbedBatchItems();
            const max_bytes = generatedEmbedBatchBytes();

            for (req.writes, 0..) |_, row_index| {
                for (plan.rows[row_index].requests) |request| {
                    if (request.kind != .sparse_embedding or request.input_kind != .document) continue;
                    if (!try shouldPrecomputeGeneratedRequest(self, precompute_mode, request)) continue;
                    const cleaned = (if (request.source_template.len > 0) try extracted[row_index].logicalJson() else try extracted[row_index].logicalJsonForField(request.source_field)) orelse continue;
                    const source_text = if (request.source_template.len > 0)
                        renderSourceTemplateText(memo.alloc, self, request.source_template, cleaned) catch |err| switch (err) {
                            error.OutOfMemory, error.PermanentPromptFailure, error.TransientPromptFailure => return err,
                            else => null,
                        }
                    else
                        try extractStringField(memo.alloc, cleaned, request.source_field);
                    const text = source_text orelse continue;
                    if (text.len == 0) {
                        memo.alloc.free(text);
                        continue;
                    }
                    var text_owned = true;
                    errdefer if (text_owned) memo.alloc.free(text);
                    const key_value = GeneratedEmbeddingMemo.key(
                        .sparse_embedding,
                        requestEmbeddingName(request),
                        request.producer_json,
                        request.execution_json,
                        0,
                        text,
                    );
                    if (memo.sparse.contains(key_value) or pending.contains(key_value)) {
                        memo.alloc.free(text);
                        text_owned = false;
                        continue;
                    }
                    if (try prewarmGeneratedMemoFromArtifact(self, memo, request, text, key_value)) {
                        memo.alloc.free(text);
                        text_owned = false;
                        continue;
                    }
                    const incompatible = if (jobs.items.len == 0) false else blk: {
                        const first = jobs.items[0].request;
                        break :blk !std.mem.eql(u8, requestEmbeddingName(first), requestEmbeddingName(request)) or
                            !std.mem.eql(u8, first.producer_json, request.producer_json) or
                            !std.mem.eql(u8, first.execution_json, request.execution_json) or
                            !generatedConsumerSetsEqual(first.consumer_indexes, request.consumer_indexes);
                    };
                    if (incompatible or (jobs.items.len > 0 and
                        (jobs.items.len >= max_items or batch_bytes + text.len > max_bytes)))
                    {
                        try flushGeneratedSparseMemoJobs(self, memo, &jobs);
                        pending.clearRetainingCapacity();
                        batch_bytes = 0;
                    }
                    try pending.put(memo.alloc, key_value, {});
                    try jobs.append(memo.alloc, .{ .key = key_value, .text = text, .request = request });
                    text_owned = false;
                    batch_bytes += text.len;
                    if (jobs.items.len >= max_items or batch_bytes >= max_bytes) {
                        try flushGeneratedSparseMemoJobs(self, memo, &jobs);
                        pending.clearRetainingCapacity();
                        batch_bytes = 0;
                    }
                }
            }
            try flushGeneratedSparseMemoJobs(self, memo, &jobs);
        }

        pub fn requireRelationalConsumerFields(prepared: *mapper.PreparedRelationalWrite, schema: schema_mod.TableSchema, plan: ?index_manager_mod.IndexManager.WritePlanSnapshot, retain_text: bool) !void {
            if (prepared.extracted.logical_source == null) return;
            if (retain_text) {
                if (plan) |snapshot| {
                    if (snapshot.text_requires_root or !snapshot.has_text_consumers) {
                        try prepared.requireLogicalRoot();
                    } else for (snapshot.text_fields) |field| {
                        // Preserve both literal dotted keys and nested-path roots;
                        // the established text selector decides their semantics.
                        try prepared.requireLogicalField(field);
                        if (std.mem.indexOfScalar(u8, field, '.')) |dot| try prepared.requireLogicalField(field[0..dot]);
                    }
                } else try prepared.requireLogicalRoot();
            }
            if (schema.ttl_duration_ns != 0) try prepared.requireLogicalField(schema.ttl_field);
            if (plan) |snapshot| {
                for (snapshot.graph_fields) |graph_field| for (graph_field.edges) |edge| try prepared.requireLogicalField(edge.field_name);
                for (snapshot.sparse_fields) |field| try prepared.requireLogicalField(field.field_name);
                for (snapshot.dense_fields) |field| {
                    const row = prepared.extracted.logical_source.?.row;
                    if (row.ordinalForName(field.field_name)) |ordinal| if (try row.findCell(ordinal)) |cell| if (cell.is_null or cell.is_dense_vector) continue;
                    try prepared.requireLogicalField(field.field_name);
                }
                for (snapshot.generated_templates) |request| if (request.kind == .asset and request.source_template.len == 0) try prepared.requireLogicalField(request.source_field);
            }
            if (retain_text) {
                prepared.extracted.prepared_text_root = prepared.parsedValue();
                prepared.extracted.prepared_text_source_bytes = mapper.estimateJsonValueRetainedBytes(prepared.parsedValue());
            }
        }

        pub fn resolveWriteTimestampForSchemaValue(
            schema: schema_mod.TableSchema,
            fallback_timestamp_ns: u64,
            value: std.json.Value,
        ) !u64 {
            if (schema.ttl_duration_ns == 0) return fallback_timestamp_ns;
            const root = switch (value) {
                .object => |object| object,
                else => return fallback_timestamp_ns,
            };
            const ttl_value = root.get(schema.ttl_field) orelse return fallback_timestamp_ns;
            if (ttl_value == .null) return fallback_timestamp_ns;
            return try ttlTimestampNsFromJsonValue(ttl_value);
        }

        pub fn resolveWriteTimestampFromValue(self: anytype, fallback_timestamp_ns: u64, value: std.json.Value) !u64 {
            const schema = self.core.schema orelse return fallback_timestamp_ns;
            return try resolveWriteTimestampForSchemaValue(schema, fallback_timestamp_ns, value);
        }

        pub fn resolveWriteTimestampNs(self: anytype, fallback_timestamp_ns: u64, value_json: []const u8) !u64 {
            const schema = self.core.schema orelse return fallback_timestamp_ns;
            if (schema.ttl_duration_ns == 0) return fallback_timestamp_ns;
            return (try ttlTimestampNsFromDocumentValue(self.alloc, schema, value_json)) orelse fallback_timestamp_ns;
        }

        pub fn retainPreparedTextRoots(sync_level: types.SyncLevel, has_text_consumers: bool, split_shadow: bool) bool {
            return split_shadow or (has_text_consumers and (sync_level == .full_text or sync_level == .full_index));
        }

        pub fn shouldPrecomputeGeneratedRequest(
            self: anytype,
            mode: GeneratedPrecomputeMode,
            request: enrichment_types.GeneratedEnrichmentRequest,
        ) !bool {
            _ = self;
            // Neighbor context depends on committed adjacency, including graph effects
            // in this batch. The precommit producer path has neither that snapshot nor
            // the composed neighbor input/skip hash. Retain this request for postcommit
            // replay; synchronous levels still wait for its generated coverage there.
            if (request.requires_committed_graph or request.neighbor_context_json.len != 0) return false;
            return switch (mode) {
                .none => false,
                .all => true,
            };
        }

        pub fn shouldWriteTimestamp(key: []const u8) bool {
            return !isMetadataKey(key) and !internal_keys.isInternalUserKey(key);
        }

        pub fn strippedStoredDocumentValueAlloc(
            alloc: Allocator,
            cleaned: []const u8,
            vector_store_field_names: []const []const u8,
            owned_values: *std.ArrayListUnmanaged([]u8),
        ) ![]const u8 {
            if (vector_store_field_names.len == 0) return cleaned;
            const stripped = (try mapper.stripTopLevelFieldsAlloc(alloc, cleaned, vector_store_field_names)) orelse try alloc.dupe(u8, "{}");
            errdefer alloc.free(stripped);
            try owned_values.append(alloc, stripped);
            return stripped;
        }

        pub fn ttlTimestampNsFromDocumentValue(
            alloc: Allocator,
            schema: schema_mod.TableSchema,
            value_json: []const u8,
        ) !?u64 {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
            defer parsed.deinit();
            const root = switch (parsed.value) {
                .object => |object| object,
                else => return null,
            };
            const ttl_value = root.get(schema.ttl_field) orelse return null;
            if (ttl_value == .null) return null;
            return try ttlTimestampNsFromJsonValue(ttl_value);
        }

        pub fn ttlTimestampNsFromJsonValue(value: std.json.Value) !u64 {
            return switch (value) {
                .integer => |integer| blk: {
                    if (integer < 0) return error.InvalidArgument;
                    break :blk std.math.cast(u64, integer) orelse return error.InvalidArgument;
                },
                .number_string => |text| std.fmt.parseInt(u64, std.mem.trim(u8, text, " \t\r\n"), 10) catch return error.InvalidArgument,
                .string => |text| ttlTimestampNsFromString(text),
                else => error.InvalidArgument,
            };
        }

        pub fn ttlTimestampNsFromString(text: []const u8) !u64 {
            const trimmed = std.mem.trim(u8, text, " \t\r\n");
            if (trimmed.len == 0) return error.InvalidArgument;
            if (std.fmt.parseInt(u64, trimmed, 10)) |ts| return ts else |_| {}
            if (try parsePatternRfc3339ToNs(trimmed)) |ts| return ts;
            return error.InvalidArgument;
        }

        pub fn prepareRelationalRows(
            allocator_guard: *PreparedRowAllocator,
            io: ?std.Io,
            db: anytype,
            writes: []const types.BatchWrite,
            validator: ?public_table_schema.CompiledTableValidator,
            table_schema: schema_mod.TableSchema,
            physical_layout: *const relational_row_codec.PhysicalLayout,
            write_plan: ?index_manager_mod.IndexManager.WritePlanSnapshot,
            retain_text_roots: bool,
            preparation_timestamp_ns: u64,
            rows: []?mapper.PreparedRelationalWrite,
            durable_rows: ?*const std.StringHashMapUnmanaged([]const u8),
            restore_timestamps: ?*const std.StringHashMapUnmanaged(u64),
            preserve_logical_values: bool,
        ) !void {
            if (writes.len != rows.len) return error.InvalidArgument;
            const Database = switch (@typeInfo(@TypeOf(db))) {
                .null => Execution,
                .pointer => |pointer| pointer.child,
                .optional => |optional| @typeInfo(optional.child).pointer.child,
                else => @compileError("expected an optional local mutation receiver"),
            };
            const RowPreparationContext = struct {
                alloc: Allocator,
                scratch_child: Allocator,
                writes: []const types.BatchWrite,
                validator: ?public_table_schema.CompiledTableValidator,
                table_schema: schema_mod.TableSchema,
                physical_layout: *const relational_row_codec.PhysicalLayout,
                db: ?*Database,
                write_plan: ?index_manager_mod.IndexManager.WritePlanSnapshot,
                retain_text_roots: bool,
                preparation_timestamp_ns: u64,
                rows: []?mapper.PreparedRelationalWrite,
                durable_rows: ?*const std.StringHashMapUnmanaged([]const u8),
                restore_timestamps: ?*const std.StringHashMapUnmanaged(u64),
                preserve_logical_values: bool,
                next: std.atomic.Value(usize) = .init(0),
                failed: std.atomic.Value(bool) = .init(false),
                io: std.Io,
                error_mutex: std.Io.Mutex = .init,
                first_error: ?anyerror = null,

                fn fail(ctx: *@This(), err: anyerror) void {
                    ctx.error_mutex.lockUncancelable(ctx.io);
                    if (ctx.first_error == null) ctx.first_error = err;
                    ctx.error_mutex.unlock(ctx.io);
                    ctx.failed.store(true, .release);
                }

                fn run(ctx: *@This()) void {
                    var scratch_arena = std.heap.ArenaAllocator.init(ctx.scratch_child);
                    defer scratch_arena.deinit();
                    // One arena per worker, not per row. Rows retain the region through
                    // commit and release it independently; only page acquisition from
                    // the shared DB allocator crosses the allocator mutex.
                    const region = mapper.PreparedRelationalWrite.createSharedRegion(ctx.alloc) catch |err| {
                        ctx.fail(err);
                        return;
                    };
                    defer region.release();
                    while (!ctx.failed.load(.acquire)) {
                        const index = ctx.next.fetchAdd(1, .monotonic);
                        if (index >= ctx.writes.len) return;
                        var prepared = (if (ctx.writes[index].json_null_fields.len != 0)
                            mapper.PreparedRelationalWrite.initTypedInSharedRegion(region, scratch_arena.allocator(), ctx.retain_text_roots, ctx.writes[index].key, ctx.writes[index].value, ctx.validator, ctx.table_schema, ctx.physical_layout, ctx.writes[index].json_null_fields, ctx.preserve_logical_values)
                        else if (ctx.preserve_logical_values)
                            mapper.PreparedRelationalWrite.initInSharedRegionPreserved(region, scratch_arena.allocator(), ctx.retain_text_roots, ctx.writes[index].key, ctx.writes[index].value, ctx.validator, ctx.table_schema, ctx.physical_layout)
                        else
                            mapper.PreparedRelationalWrite.initInSharedRegionFromIntent(
                                region,
                                scratch_arena.allocator(),
                                ctx.retain_text_roots,
                                ctx.writes[index].key,
                                ctx.writes[index].value,
                                ctx.validator,
                                ctx.table_schema,
                                ctx.physical_layout,
                                if (ctx.durable_rows) |durable| durable.get(ctx.writes[index].key) else null,
                            )) catch |err| {
                            ctx.fail(err);
                            return;
                        };
                        const owner_alloc = if (ctx.db) |owner_db| owner_db.alloc else ctx.scratch_child;
                        const row_alloc = prepared.preparationAllocator(owner_alloc);
                        requireRelationalConsumerFields(&prepared, ctx.table_schema, ctx.write_plan, ctx.retain_text_roots) catch |err| {
                            prepared.deinit(owner_alloc);
                            ctx.fail(err);
                            return;
                        };
                        if (ctx.db) |owner_db| if (splitShadowRequiresMaterializedDerivedBatch(owner_db)) {
                            prepared.requireLogicalRoot() catch |err| {
                                prepared.deinit(owner_alloc);
                                ctx.fail(err);
                                return;
                            };
                            prepared.extracted.prepared_text_root = prepared.parsedValue();
                            prepared.extracted.prepared_text_source_bytes = mapper.estimateJsonValueRetainedBytes(prepared.parsedValue());
                        };
                        if (ctx.write_plan) |pinned_write_plan| {
                            if (ctx.retain_text_roots) {
                                prepared.extracted.prepared_schema_version = ctx.table_schema.version;
                                prepared.extracted.prepared_write_plan_generation = pinned_write_plan.generation;
                            }
                            augmentExtractedWriteWithGraphFieldEdgesFromSnapshotParsed(
                                pinned_write_plan,
                                row_alloc,
                                ctx.writes[index].key,
                                prepared.parsedValue(),
                                &prepared.extracted,
                            ) catch |err| {
                                prepared.deinit(owner_alloc);
                                ctx.fail(err);
                                return;
                            };
                            pinned_write_plan.appendIndexFieldEmbeddingsFromPreparedToExtractedWrite(
                                row_alloc,
                                ctx.writes[index].key,
                                prepared.parsedValue(),
                                prepared.typedView(ctx.table_schema, ctx.physical_layout) catch |err| {
                                    prepared.deinit(owner_alloc);
                                    ctx.fail(err);
                                    return;
                                },
                                &prepared.extracted,
                            ) catch |err| {
                                prepared.deinit(owner_alloc);
                                ctx.fail(err);
                                return;
                            };
                        }
                        if (prepared.extracted.hasDocument()) {
                            if (ctx.db) |owner_db| if (ctx.write_plan) |pinned_write_plan|
                                validateDocumentExtractionInlineSourcesSnapshotParsed(
                                    scratch_arena.allocator(),
                                    owner_db,
                                    pinned_write_plan,
                                    prepared.parsedValue(),
                                    prepared.extracted,
                                ) catch |err| {
                                    prepared.deinit(owner_alloc);
                                    ctx.fail(err);
                                    return;
                                };
                            if (shouldWriteTimestamp(ctx.writes[index].key)) {
                                const retained_timestamp = if (ctx.restore_timestamps) |timestamps| timestamps.get(ctx.writes[index].key) else null;
                                const write_timestamp_ns = retained_timestamp orelse (resolveWriteTimestampForSchemaValue(
                                    ctx.table_schema,
                                    ctx.preparation_timestamp_ns,
                                    prepared.parsedValue(),
                                ) catch |err| {
                                    prepared.deinit(owner_alloc);
                                    ctx.fail(err);
                                    return;
                                });
                                prepared.finalizeMetadata(write_timestamp_ns) catch |err| {
                                    prepared.deinit(owner_alloc);
                                    ctx.fail(err);
                                    return;
                                };
                            }
                        }
                        prepared.finalizeMetadata(0) catch |err| {
                            prepared.deinit(owner_alloc);
                            ctx.fail(err);
                            return;
                        };
                        // Release the Parsed wrapper before publishing the completed
                        // row. When retain_text_roots is enabled, its backing pages
                        // belong to the row region (whose free operation is a no-op),
                        // so prepared_text_root remains valid until that region is
                        // released; otherwise the worker scratch arena is reset here.
                        prepared.releaseParsed();
                        ctx.rows[index] = prepared;
                        _ = scratch_arena.reset(.retain_capacity);
                    }
                }
            };

            // A handful of very large rows is just as CPU-heavy as a large row count.
            // Size the task group from both independent rows and input bytes; the
            // backend runtime already bounds the shared worker pool, while this local
            // ceiling prevents one request from monopolizing it.
            const desired_workers = relationalPreparationWorkers(writes, durable_rows);
            const parallel = io != null and desired_workers > 1;
            var ctx = RowPreparationContext{
                .alloc = allocator_guard.allocator(),
                .scratch_child = allocator_guard.allocator(),
                .writes = writes,
                .validator = validator,
                .table_schema = table_schema,
                .physical_layout = physical_layout,
                .db = db,
                .write_plan = write_plan,
                .retain_text_roots = retain_text_roots,
                .preparation_timestamp_ns = preparation_timestamp_ns,
                .rows = rows,
                .durable_rows = durable_rows,
                .restore_timestamps = restore_timestamps,
                .preserve_logical_values = preserve_logical_values,
                .io = io orelse std.Options.debug_io,
            };
            if (!parallel) {
                ctx.run();
                if (ctx.first_error) |err| return err;
                return;
            }

            // Reuse the backend runtime's bounded worker pool. This avoids reserving
            // fresh kernel stacks for every large request while retaining parallel
            // parsing for batches large enough to amortize coordination.
            var group: std.Io.Group = .init;
            for (1..desired_workers) |_| group.async(io.?, RowPreparationContext.run, .{&ctx});
            ctx.run();
            try group.await(io.?);
            if (ctx.first_error) |err| return err;
        }

        pub fn relationalPreparationWorkers(writes: []const types.BatchWrite, durable_rows: ?*const std.StringHashMapUnmanaged([]const u8)) usize {
            var input_bytes: usize = 0;
            for (writes) |write| {
                input_bytes +|= write.key.len;
                input_bytes +|= write.value.len;
                if (durable_rows) |rows| if (rows.get(write.key)) |row| {
                    // Commit/recovery carries only reserved fields in write.value.
                    // Packed bytes still require authentication, copying and extraction.
                    input_bytes +|= row.len;
                };
            }
            if (input_bytes < 512 * 1024) return 1;
            const size_workers = input_bytes / (256 * 1024) + @intFromBool(input_bytes % (256 * 1024) != 0);
            return @max(1, @min(8, @min(writes.len, size_workers)));
        }
    };
}
