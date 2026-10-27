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

//! Local document materialization and owned result collection. Callers retain
//! storage/catalog fences; these functions borrow stores and managers only.
const std = @import("std");
const source_claim = @import("../index_source_claim.zig");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const mapper = @import("document_mapper.zig");
const docstore_mod = @import("../docstore.zig");
const derived_types = @import("derived/derived_types.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");
const relational_store = @import("relational_store.zig");
const doc_identity = @import("doc_identity.zig");
const lookup_key_scratch = @import("lookup_key_scratch.zig");
const document_read_scratch = @import("document_read_scratch.zig");
const resource_manager_mod = @import("../resource_manager.zig");
fn monotonicTimeNs() u64 {
    return @import("antfly_platform").time.monotonicNs();
}
pub const OwnedBatchWrites = struct {
    alloc: Allocator,
    items: []types.BatchWrite = &.{},
    missing_required: usize = 0,

    pub fn deinit(self: *@This()) void {
        for (self.items) |item| self.alloc.free(@constCast(item.value));
        if (self.items.len > 0) self.alloc.free(self.items);
        self.* = undefined;
    }
};

pub const CollectDocumentWritesProfile = struct {
    scan_ns: u64 = 0,
    sort_ns: u64 = 0,
    read_ns: u64 = 0,
    materialize_ns: u64 = 0,
    input_documents: usize = 0,
    pending_documents: usize = 0,
    output_writes: usize = 0,
    missing_required: usize = 0,
    inline_hits: usize = 0,
    store_hits: usize = 0,
};

pub const CollectTextDocumentWritesOptions = struct {
    prefer_inline_when_store_tip_matches_sequence: ?u64 = null,
    relational_base_rows: bool = false,
    /// Set once replay of this window has retried error.ReplayDocumentNotVisible
    /// past the bounded limit (see ResourceManager.shouldEscalateReplayDocumentNotVisible).
    /// A document that is still missing is then skipped and logged once
    /// instead of counted toward missing_required, so the window can advance
    /// instead of retrying forever.
    tolerate_missing_replay_documents: bool = false,
};

pub const CollectDocumentWritesOptions = struct {
    prefer_inline_when_store_tip_matches_sequence: ?u64 = null,
    prefer_available_inline_values: bool = false,
    skip_doc_keys: ?*const std.StringHashMapUnmanaged(void) = null,
    relational_base_rows: bool = false,
};

pub const CollectedTextDocumentWrites = struct {
    alloc: Allocator,
    docs: std.ArrayListUnmanaged(mapper.MapperDoc) = .empty,
    owned_values: std.ArrayListUnmanaged([]u8) = .empty,
    // Exact-size batch ownership for ordinary store-backed JSON. Slices in
    // docs remain stable after the read transaction and this result move.
    document_bytes: []u8 = &.{},
    materialized_documents: std.ArrayListUnmanaged(index_manager_mod.IndexManager.MaterializedStoredDocument) = .empty,
    source_read: ?*docstore_mod.DocStore.Txn = null,
    source_observations: []source_claim.ObservedSource = &.{},
    missing_required: usize = 0,

    pub fn deinit(self: *@This()) void {
        for (self.source_observations) |*source| source.deinit(self.alloc);
        self.alloc.free(self.source_observations);
        if (self.source_read) |read| {
            read.abort();
            self.alloc.destroy(read);
        }
        for (self.materialized_documents.items) |*doc| doc.deinit(self.alloc);
        self.materialized_documents.deinit(self.alloc);
        for (self.owned_values.items) |value| self.alloc.free(value);
        self.owned_values.deinit(self.alloc);
        self.alloc.free(self.document_bytes);
        self.docs.deinit(self.alloc);
        self.* = undefined;
    }
};

pub fn collectDocumentWrites(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    documents: []const derived_types.DerivedDocument,
    byte_range: types.ByteRange,
) !OwnedBatchWrites {
    return try collectDocumentWritesProfiled(alloc, store, null, documents, byte_range, .{}, null);
}

pub fn availableDocumentValueCount(pending: anytype, values: []const ?[]const u8, has_inline: bool) usize {
    var count: usize = 0;
    for (values) |value| count += @intFromBool(value != null);
    if (has_inline) {
        for (pending, values) |item, value| {
            count += @intFromBool(value == null and item.inline_value != null);
        }
    }
    return count;
}

pub fn collectDocumentWritesProfiled(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: ?*index_manager_mod.IndexManager,
    documents: []const derived_types.DerivedDocument,
    byte_range: types.ByteRange,
    opts: CollectDocumentWritesOptions,
    profile: ?*CollectDocumentWritesProfile,
) !OwnedBatchWrites {
    const PendingDocumentWrite = struct {
        doc_key: []const u8,
        store_key: []const u8,
        inline_value: ?[]const u8,
    };

    var lookup_keys = lookup_key_scratch.Scratch.init(alloc, documents.len);
    defer lookup_keys.deinit();
    // Temporary descriptors cannot escape the synchronous read/apply below.
    var descriptor_buffer_storage: [4096]u8 align(@alignOf(std.c.max_align_t)) = undefined;
    var descriptor_buffer: std.heap.BufferFirstAllocator = .init(&descriptor_buffer_storage, alloc);
    const descriptor_alloc = descriptor_buffer.allocator();
    var pending = std.ArrayListUnmanaged(PendingDocumentWrite).empty;
    defer {
        pending.deinit(descriptor_alloc);
    }
    // A bounded small batch fits entirely on the stack. Reserve once so
    // append does not repeatedly enter the capacity-growth path. Larger
    // batches keep lazy growth proportional to the selected documents.
    if (documents.len <= 64) {
        comptime std.debug.assert(64 * @sizeOf(PendingDocumentWrite) <= 4096);
        try pending.ensureTotalCapacityPrecise(descriptor_alloc, documents.len);
    }

    var has_pending_inline = false;
    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    errdefer {
        for (writes.items) |item| alloc.free(@constCast(item.value));
        writes.deinit(alloc);
    }

    var txn = try store.beginProbeTxn();
    defer txn.abort();
    var missing_required: usize = 0;
    const trust_inline = opts.prefer_available_inline_values or
        if (opts.prefer_inline_when_store_tip_matches_sequence) |sequence|
            store.nextReplaySequence(sequence + 1) == sequence + 1
        else
            false;

    if (profile) |p| p.input_documents = documents.len;
    const scan_start_ns = if (profile != null) monotonicTimeNs() else 0;
    for (documents) |doc| {
        if (doc.action != .upsert) continue;
        if (!replayDocumentKeyInRange(byte_range, doc.key)) continue;
        if (opts.skip_doc_keys) |skip_doc_keys| {
            if (skip_doc_keys.contains(doc.key)) continue;
        }
        if (trust_inline and doc.cleaned_value != null) {
            const owned_value = try alloc.dupe(u8, doc.cleaned_value.?);
            writes.append(alloc, .{
                .key = doc.key,
                .value = owned_value,
            }) catch |err| {
                alloc.free(owned_value);
                return err;
            };
            if (profile) |p| p.inline_hits += 1;
            continue;
        }
        try pending.append(descriptor_alloc, .{
            .doc_key = doc.key,
            .store_key = try lookup_keys.key(doc.key, opts.relational_base_rows),
            .inline_value = doc.cleaned_value,
        });
        has_pending_inline = has_pending_inline or doc.cleaned_value != null;
    }
    if (profile) |p| {
        p.scan_ns = monotonicTimeNs() - scan_start_ns;
        p.pending_documents = pending.items.len;
        p.output_writes = writes.items.len;
    }

    if (pending.items.len == 0) {
        return .{
            .alloc = alloc,
            .items = try writes.toOwnedSlice(alloc),
        };
    }

    const SortContext = struct {};
    const sort_start_ns = if (profile != null) monotonicTimeNs() else 0;
    std.mem.sort(PendingDocumentWrite, pending.items, SortContext{}, struct {
        fn lessThan(_: SortContext, lhs: PendingDocumentWrite, rhs: PendingDocumentWrite) bool {
            return std.mem.order(u8, lhs.store_key, rhs.store_key) == .lt;
        }
    }.lessThan);
    if (profile) |p| p.sort_ns = monotonicTimeNs() - sort_start_ns;

    var read_scratch = try document_read_scratch.Scratch.init(descriptor_alloc, pending.items.len);
    defer read_scratch.deinit();
    const read_keys = read_scratch.keys;
    const read_values = read_scratch.values;

    for (pending.items, 0..) |item, i| {
        read_keys[i] = item.store_key;
    }
    const read_start_ns = if (profile != null) monotonicTimeNs() else 0;
    try txn.getManySorted(read_keys, read_values);
    if (profile) |p| p.read_ns = monotonicTimeNs() - read_start_ns;

    const available_values = availableDocumentValueCount(pending.items, read_values, has_pending_inline);
    try writes.ensureTotalCapacityPrecise(alloc, try std.math.add(usize, writes.items.len, available_values));

    const materialize_start_ns = if (profile != null) monotonicTimeNs() else 0;
    for (pending.items, 0..) |item, i| {
        const value = if (read_values[i]) |store_value| blk: {
            if (profile) |p| p.store_hits += 1;
            break :blk store_value;
        } else if (item.inline_value) |inline_value| blk: {
            if (profile) |p| p.inline_hits += 1;
            break :blk inline_value;
        } else {
            if (try replayDocumentIsDurablyDeleted(alloc, &txn, item.doc_key)) continue;
            missing_required += 1;
            continue;
        };
        const owned_value = if (read_values[i] != null)
            if (index_manager) |manager|
                try manager.materializeStoredValueAlloc(alloc, item.store_key, value)
            else
                try relational_store.materializeStoredValueAlloc(alloc, item.store_key, value)
        else
            try alloc.dupe(u8, value);
        writes.append(alloc, .{
            .key = item.doc_key,
            .value = owned_value,
        }) catch |err| {
            alloc.free(owned_value);
            return err;
        };
    }
    if (profile) |p| {
        p.materialize_ns = monotonicTimeNs() - materialize_start_ns;
        p.output_writes = writes.items.len;
        p.missing_required = missing_required;
    }

    return .{
        .alloc = alloc,
        .items = try writes.toOwnedSlice(alloc),
        .missing_required = missing_required,
    };
}

pub fn ordinaryTextDocument(doc_key: []const u8, store_key: []const u8) bool {
    return !internal_keys.isInternalUserKey(doc_key) and !internal_keys.isRelationalRowKey(store_key);
}

pub fn collectTextDocumentWritesForIndex(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    documents: []const derived_types.DerivedDocument,
    index_name: []const u8,
    chunk_backed: bool,
    byte_range: types.ByteRange,
    opts: CollectTextDocumentWritesOptions,
) !CollectedTextDocumentWrites {
    const PendingTextWrite = struct {
        doc_key: []const u8,
        store_key: []const u8,
        inline_value: ?[]const u8,
    };

    var lookup_keys = lookup_key_scratch.Scratch.init(alloc, documents.len);
    defer lookup_keys.deinit();
    // Temporary descriptors cannot escape the synchronous read/apply below.
    var descriptor_buffer_storage: [4096]u8 align(@alignOf(std.c.max_align_t)) = undefined;
    var descriptor_buffer: std.heap.BufferFirstAllocator = .init(&descriptor_buffer_storage, alloc);
    const descriptor_alloc = descriptor_buffer.allocator();
    var pending = std.ArrayListUnmanaged(PendingTextWrite).empty;
    defer {
        pending.deinit(descriptor_alloc);
    }
    // A bounded small batch fits entirely on the stack. Reserve once so
    // append does not repeatedly enter the capacity-growth path. Larger
    // batches keep lazy growth proportional to the selected documents.
    if (documents.len <= 64) {
        comptime std.debug.assert(64 * @sizeOf(PendingTextWrite) <= 4096);
        try pending.ensureTotalCapacityPrecise(descriptor_alloc, documents.len);
    }

    var has_pending_inline = false;
    var result = CollectedTextDocumentWrites{ .alloc = alloc };
    errdefer result.deinit();
    var schema_views = index_manager_mod.IndexManager.SchemaViewSet.init(index_manager);
    defer schema_views.deinit();

    // Only an admitted JSON-source table retains observations. Legacy tables
    // keep the current-probe and trusted inline fast paths unchanged.
    if (store.documentLayoutEnabled() and !opts.relational_base_rows and !chunk_backed and
        @import("../schema.zig").maximum_reader_format_version >= 28)
    {
        const read = try alloc.create(docstore_mod.DocStore.Txn);
        errdefer alloc.destroy(read);
        read.* = try store.beginReadTxn();
        errdefer read.abort();
        const raw_schema: ?[]const u8 = read.getPhysical(@import("../schema.zig").schema_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (if (raw_schema) |bytes| try @import("../schema.zig").serializedReaderFormat(bytes) >= 28 else false) {
            result.source_read = read;
        } else {
            read.abort();
            alloc.destroy(read);
        }
    }
    const trust_inline = result.source_read == null and if (opts.prefer_inline_when_store_tip_matches_sequence) |sequence|
        store.nextReplaySequence(sequence + 1) == sequence + 1
    else
        false;
    const active_schema_version = index_manager.activeSchemaVersion();
    const active_write_plan_generation = index_manager.writePlanGeneration();

    for (documents) |doc| {
        if (doc.action != .upsert) continue;
        if (!replayDocumentKeyInRange(byte_range, doc.key)) continue;
        if (!documentTargetsTextIndex(doc, index_name, chunk_backed)) continue;
        if (trust_inline and doc.cleaned_value != null) {
            const projected = try textualAssetFullTextProjectionAlloc(
                alloc,
                index_manager,
                doc.key,
                doc.cleaned_value.?,
            );
            if (projected) |value| result.owned_values.append(alloc, value) catch |err| {
                alloc.free(value);
                return err;
            };
            try result.docs.append(alloc, .{
                .key = doc.key,
                .value = projected orelse doc.cleaned_value.?,
            });
            continue;
        }
        if (trust_inline and doc.prepared_text_root != null and
            active_schema_version != null and
            active_schema_version.? == doc.prepared_schema_version and
            active_write_plan_generation == doc.prepared_write_plan_generation)
        {
            try result.docs.append(alloc, .{
                .key = doc.key,
                .value = "",
                .source_bytes = doc.prepared_text_source_bytes,
                .root = doc.prepared_text_root,
            });
            continue;
        }
        try pending.append(descriptor_alloc, .{
            .doc_key = doc.key,
            .store_key = try lookup_keys.key(doc.key, opts.relational_base_rows),
            .inline_value = doc.cleaned_value,
        });
        has_pending_inline = has_pending_inline or doc.cleaned_value != null;
    }

    if (pending.items.len == 0) return result;

    var probe = if (result.source_read == null) try store.beginProbeTxn() else null;
    defer if (probe) |*read| read.abort();
    const txn = result.source_read orelse &probe.?;
    const SortContext = struct {};
    std.mem.sort(PendingTextWrite, pending.items, SortContext{}, struct {
        fn lessThan(_: SortContext, lhs: PendingTextWrite, rhs: PendingTextWrite) bool {
            return std.mem.order(u8, lhs.store_key, rhs.store_key) == .lt;
        }
    }.lessThan);

    var read_scratch = try document_read_scratch.Scratch.init(descriptor_alloc, pending.items.len);
    defer read_scratch.deinit();
    const read_keys = read_scratch.keys;
    const read_values = read_scratch.values;

    for (pending.items, 0..) |item, i| {
        read_keys[i] = item.store_key;
    }
    if (result.source_read) |read| {
        const ordinary = for (pending.items) |item| {
            if (!ordinaryTextDocument(item.doc_key, item.store_key)) break false;
        } else true;
        if (ordinary) if (try source_claim.ObservedSource.readSortedOwned(alloc, read, read_keys)) |observations| {
            result.source_observations = observations;
            try result.docs.ensureUnusedCapacity(alloc, observations.len);
            for (pending.items, observations) |item, *source| {
                result.docs.appendAssumeCapacity(.{ .key = item.doc_key, .value = source.produced, .source_bytes = source.produced.len, .source_observation = source });
            }
            return result;
        };
    }
    try txn.getManySorted(read_keys, read_values);

    const available_values = availableDocumentValueCount(pending.items, read_values, has_pending_inline);
    try result.docs.ensureTotalCapacityPrecise(alloc, try std.math.add(usize, result.docs.items.len, available_values));

    // Read-transaction bytes cannot escape. Homogeneous ordinary batches
    // share one exact-size slab. Mixed/internal batches keep lazy per-value
    // ownership: the slab saved allocations there but regressed smp timings.
    const document_bytes_len: ?usize = sizing: {
        var bytes: usize = 0;
        for (pending.items, read_values) |item, visible| {
            if (!ordinaryTextDocument(item.doc_key, item.store_key)) break :sizing null;
            if (visible) |value| bytes = try std.math.add(usize, bytes, value.len);
        }
        break :sizing bytes;
    };
    if (document_bytes_len) |bytes| {
        if (bytes != 0) result.document_bytes = try alloc.alloc(u8, bytes);
    } else if (pending.items.len >= 16 and pending.items.len <= 256) {
        // Preserve the existing bounded reservation for public relational
        // rows. Sorted mixed/internal batches normally exit on the first key.
        const ordinary_rows = count: {
            var rows: usize = 0;
            for (pending.items, read_values) |candidate, visible| {
                if (internal_keys.isInternalUserKey(candidate.doc_key)) break :count 0;
                if (visible != null) rows += 1;
            }
            break :count rows;
        };
        if (ordinary_rows >= 16)
            try result.materialized_documents.ensureTotalCapacityPrecise(alloc, ordinary_rows);
    }
    var document_bytes_offset: usize = 0;

    for (pending.items, 0..) |item, i| {
        const value = read_values[i] orelse item.inline_value orelse {
            if (try replayDocumentIsDurablyDeleted(alloc, txn, item.doc_key)) continue;
            if (opts.tolerate_missing_replay_documents) {
                std.log.warn(
                    "full-text replay giving up on a document with no visible content after bounded retries index={s} key={s}",
                    .{ index_name, item.doc_key },
                );
                if (index_manager.resource_manager) |manager| manager.recordReplayDocumentNotVisibleSkipped(resource_manager_mod.replayOwnerIdFromPtr(index_manager), index_name);
                continue;
            }
            result.missing_required += 1;
            continue;
        };
        const projected = try textualAssetFullTextProjectionAlloc(
            alloc,
            index_manager,
            item.doc_key,
            value,
        );
        if (projected) |owned| {
            result.owned_values.append(alloc, owned) catch |err| {
                alloc.free(owned);
                return err;
            };
            try result.docs.append(alloc, .{ .key = item.doc_key, .value = owned });
            continue;
        }

        // An inline replay fallback is already a JSON API representation. Only
        // store-backed relational rows carry AROW bytes and require ordinal-
        // native materialization through the pinned schema-view set.
        if (read_values[i] == null) {
            try result.docs.append(alloc, .{ .key = item.doc_key, .value = value });
            continue;
        }

        if (document_bytes_len != null) {
            const owned = result.document_bytes[document_bytes_offset..][0..value.len];
            @memcpy(owned, value);
            document_bytes_offset += value.len;
            try result.docs.append(alloc, .{ .key = item.doc_key, .value = owned, .source_bytes = owned.len });
            continue;
        }

        var materialized = try index_manager.materializeStoredDocumentWithSchemaViewsAlloc(
            alloc,
            item.store_key,
            value,
            &schema_views,
        );
        result.materialized_documents.append(alloc, materialized) catch |err| {
            materialized.deinit(alloc);
            return err;
        };
        const stable = &result.materialized_documents.items[result.materialized_documents.items.len - 1];
        result.docs.append(alloc, .{
            .key = item.doc_key,
            .value = stable.value,
            .source_bytes = stable.retainedBytes(),
            .root = stable.root,
            .root_arena = stable.root_arena,
        }) catch |err| {
            var removed = result.materialized_documents.pop().?;
            removed.deinit(alloc);
            return err;
        };
    }

    if (result.source_read) |read| {
        const observations = try alloc.alloc(source_claim.ObservedSource, result.docs.items.len);
        var initialized: usize = 0;
        errdefer {
            for (observations[0..initialized]) |*source| source.deinit(alloc);
            alloc.free(observations);
        }
        for (result.docs.items) |*doc| {
            // Artifact and typed rows require their own producer observations.
            // Never label a transformed source as an exact primary afterimage.
            if (internal_keys.isInternalUserKey(doc.key) or doc.root != null) continue;
            const physical = try internal_keys.documentKeyAlloc(alloc, doc.key);
            defer alloc.free(physical);
            observations[initialized] = try source_claim.ObservedSource.capture(alloc, read, physical, doc.value);
            doc.source_observation = &observations[initialized];
            initialized += 1;
        }
        // Keep the allocation's original capacity for exact allocator ownership.
        // Skipped rows use a separate initialized count represented by shrinking.
        result.source_observations = try alloc.realloc(observations, initialized);
        for (result.docs.items) |*doc| doc.source_observation = null;
        var ordinal: usize = 0;
        for (result.docs.items) |*doc| {
            if (internal_keys.isInternalUserKey(doc.key) or doc.root != null) continue;
            doc.source_observation = &result.source_observations[ordinal];
            ordinal += 1;
        }
    }

    return result;
}

pub fn replayDocumentKeyInRange(byte_range: types.ByteRange, key: []const u8) bool {
    return internal_keys.isInternalUserKey(key) or byte_range.contains(key);
}

pub fn replayDocumentIsDurablyDeleted(alloc: Allocator, txn: anytype, doc_key: []const u8) !bool {
    const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, txn, doc_key)) orelse return false;
    const state = (try doc_identity.lookupStateTxn(txn, ordinal)) orelse return false;
    return !state.isLive();
}

pub fn textualAssetFullTextProjectionAlloc(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    key: []const u8,
    raw: []const u8,
) !?[]u8 {
    const name_body = internal_keys.assetArtifactNameBody(key) orelse return null;
    var name_buffer_storage: [256]u8 align(@alignOf(std.c.max_align_t)) = undefined;
    var name_buffer: std.heap.BufferFirstAllocator = .init(&name_buffer_storage, alloc);
    const name_alloc = name_buffer.allocator();
    var name_scratch = std.ArrayListUnmanaged(u8).empty;
    defer name_scratch.deinit(name_alloc);
    const name = (try internal_keys.decodeBodyView(name_body)) orelse blk: {
        // Reserve once so short escaped names fit the bounded stack buffer.
        // Longer names fall back to the caller's accounted allocator.
        try name_scratch.ensureTotalCapacityPrecise(name_alloc, name_body.len);
        break :blk try internal_keys.decodeBodyIntoList(&name_scratch, name_alloc, name_body);
    };
    const enrichment = index_manager.getEnrichment(.asset, name) orelse return null;
    // The lookup borrows its name only for the call; release spill storage
    // before allocating the projected value.
    name_scratch.clearAndFree(name_alloc);
    if (assetContentTypeIsJson(enrichment.content_type)) return null;

    const field = if (enrichment.source_field.len > 0) enrichment.source_field else "text";
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    try appendJsonString(alloc, &out, field);
    try out.append(alloc, ':');
    try appendJsonString(alloc, &out, raw);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn documentTargetsTextIndex(doc: derived_types.DerivedDocument, index_name: []const u8, is_chunk_index: bool) bool {
    for (doc.targets) |target| {
        if (target.kind != .full_text) continue;
        if (std.mem.eql(u8, target.index_name, index_name)) return true;
        if (!is_chunk_index and std.mem.eql(u8, target.index_name, "*")) return true;
    }
    return false;
}

pub fn assetContentTypeIsJson(content_type: []const u8) bool {
    const media_type = std.mem.trim(u8, if (std.mem.indexOfScalar(u8, content_type, ';')) |semi| content_type[0..semi] else content_type, &std.ascii.whitespace);
    return std.mem.eql(u8, media_type, "application/json") or std.mem.endsWith(u8, media_type, "+json");
}

pub fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    // Transfer the existing output buffer to the writer and restore ownership
    // on every exit, including a partially written string after allocation failure.
    var writer = std.Io.Writer.Allocating.fromArrayList(alloc, out);
    defer out.* = writer.toArrayList();
    writer.writer.print("{f}", .{std.json.fmt(value, .{})}) catch return error.OutOfMemory;
}

fn documentCollectorFailureSweep(alloc: Allocator, store: *docstore_mod.DocStore, documents: []const derived_types.DerivedDocument, inline_values: bool) !void {
    var writes = try collectDocumentWritesProfiled(alloc, store, null, documents, .{ .start = "", .end = "" }, .{ .prefer_available_inline_values = inline_values }, null);
    defer writes.deinit();
    try std.testing.expectEqual(documents.len, writes.items.len);
    for (writes.items) |write| try std.testing.expectEqualStrings("{\"title\":\"value\"}", write.value);
}

test "document collectors release owned values and pooled read scratch on allocation failures" {
    const alloc = std.testing.allocator;
    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore_mod.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var names: [80][96]u8 = undefined;
    var docs: [80]derived_types.DerivedDocument = undefined;
    for (&names, &docs, 0..) |*name, *doc, i| {
        @memset(name, 'x');
        _ = try std.fmt.bufPrint(name[0..8], "{d:0>8}", .{i});
        doc.* = .{ .key = name, .action = .upsert, .cleaned_value = "{\"title\":\"value\"}" };
    }
    const stored_key = try replayDocumentStoreKeyAlloc(alloc, docs[0].key, false);
    defer alloc.free(stored_key);
    try store.putBatch(&.{.{ .key = stored_key, .value = docs[0].cleaned_value.? }}, &.{});
    for ([_]bool{ false, true }) |inline_values|
        try std.testing.checkAllAllocationFailures(alloc, documentCollectorFailureSweep, .{ &store, @as([]const derived_types.DerivedDocument, &docs), inline_values });
}

pub fn replayDocumentStoreKeyAlloc(alloc: Allocator, key: []const u8, relational_base_rows: bool) ![]u8 {
    return if (internal_keys.isInternalUserKey(key))
        try alloc.dupe(u8, key)
    else if (relational_base_rows)
        try relational_store.keyAlloc(alloc, key)
    else
        try internal_keys.documentKeyAlloc(alloc, key);
}

const mem_backend_mod = @import("../mem_backend.zig");
